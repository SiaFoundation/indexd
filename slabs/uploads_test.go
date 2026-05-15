package slabs_test

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestUploadShards(t *testing.T) {
	log := zaptest.NewLogger(t)
	// prepare dependencies
	store := newMockStore(t)
	chain := newMockChainManager()
	am := newMockAccountManager()
	hm := newMockHostManager()
	account := types.GeneratePrivateKey()
	client := newMockHostClient()

	// prepare hosts
	hostKeys := make([]types.PrivateKey, 4)
	hosts := make([]hosts.Host, 0, len(hostKeys))
	availableHosts := make([]types.PublicKey, 0, len(hostKeys))
	for i := range hostKeys {
		sk := types.GeneratePrivateKey()
		h := client.addTestHost(sk)
		hostKeys[i] = sk
		if h.Settings.Prices.StoragePrice.IsZero() {
			t.Fatal("host has zero storage price")
		}
		hosts = append(hosts, h)
		availableHosts = append(availableHosts, sk.PublicKey())
	}

	// prepare shards
	root1, sector1 := newTestSector()
	root2, sector2 := newTestSector()
	root3, sector3 := newTestSector()
	shards := [][]byte{sector1[:], sector2[:], sector3[:]}

	slab := slabs.Slab{
		Sectors: []slabs.Sector{
			{Root: root1},
			{Root: root2},
			{Root: root3},
		},
	}

	// create manager
	alerter := alerts.NewManager()
	sm := slabs.NewSlabManager(chain, am, nil, hm, store, client, alerter, account, types.GeneratePrivateKey())
	sm.SetShardTimeout(50 * time.Millisecond)

	// set balance to 1SC
	for _, hostKey := range availableHosts {
		err := am.UpdateServiceAccountBalance(hostKey, sm.MigrationAccount(), types.Siacoins(1))
		if err != nil {
			t.Fatal(err)
		}
	}

	assertSectors := func(t *testing.T, potentialSectors []types.Hash256, n int, unexpected []types.Hash256) {
		t.Helper()

		var uploaded int
		potentialMap := make(map[types.Hash256]struct{}, len(potentialSectors))
		for _, root := range potentialSectors {
			potentialMap[root] = struct{}{}
		}
		unexpectedMap := make(map[types.Hash256]struct{}, len(unexpected))
		for _, root := range unexpected {
			unexpectedMap[root] = struct{}{}
		}

		// check that enough candidate sectors were uploaded to at most one host
		// then reset the mock host storage for the next test
		client.mu.Lock()
		for _, stored := range client.hostSectors {
			for root := range stored {
				if _, ok := unexpectedMap[root]; ok {
					client.mu.Unlock()
					t.Fatalf("unexpected sector found: %v", root)
				} else if _, ok := potentialMap[root]; ok {
					uploaded++
					delete(potentialMap, root)
				}
			}
		}
		client.mu.Unlock()
		if uploaded != n {
			t.Fatalf("expected %d uploaded sectors, got %d", n, uploaded)
		}
		client.resetStorage()
	}

	// assert passing in no hosts returns an error and no uploads
	_, err := sm.UploadShards(context.Background(), slab, shards, nil, zap.NewNop())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	assertSectors(t, nil, 0, nil)

	// assert passing in enough hosts uploads all shards
	uploaded, err := sm.UploadShards(context.Background(), slab, shards, availableHosts[:3], log)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if uploaded != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", uploaded)
	}
	assertSectors(t, []types.Hash256{root1, root2, root3}, 3, nil)

	// asserts hosts are debited for the upload
	for _, h := range hosts[:3] {
		balance, err := am.ServiceAccountBalance(h.PublicKey, sm.MigrationAccount())
		if err != nil {
			t.Fatal(err)
		} else if !balance.Equals(types.Siacoins(1).Sub(h.Settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost())) {
			t.Fatalf("unexpected balance %v", balance)
		}
	}

	// assert passing in too few hosts returns the uploaded shards and no error
	uploaded, err = sm.UploadShards(context.Background(), slab, shards, availableHosts[:2], log)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if uploaded != 2 {
		t.Fatalf("expected 2 uploaded shards, got %d", uploaded)
	}
	assertSectors(t, []types.Hash256{root1, root2, root3}, 2, nil) // all are possible, but only 2 should be succeed

	// assert hosts are tried until one succeeds
	client.slowHosts[hosts[0].PublicKey] = time.Second
	uploaded, err = sm.UploadShards(context.Background(), slab, shards, availableHosts, log)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if uploaded != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", uploaded)
	}
	assertSectors(t, []types.Hash256{root1, root2, root3}, 3, nil)

	// assert migrations are not successful if sector roots
	// do not match
	corrupted := slabs.Slab{Sectors: slices.Clone(slab.Sectors)}
	corrupted.Sectors[1].Root = frand.Entropy256()
	uploaded, err = sm.UploadShards(context.Background(), corrupted, shards, availableHosts, log)
	if err != nil {
		t.Fatal(err)
	} else if uploaded >= 3 {
		t.Fatalf("expected fewer uploaded shards, got %d", uploaded)
	}
	for _, stored := range client.hostSectors {
		for root := range stored {
			if root == corrupted.Sectors[1].Root {
				t.Fatalf("corrupted sector was uploaded: %v", root)
			}
		}
	}
}

// TestUploadShardsDemotion verifies that uploadShards demotes a host when
// the per-shard timeout fires while the overall migration is still in
// progress, and does NOT demote on quick errors (e.g. host became unusable)
// or on successful uploads.
func TestUploadShardsDemotion(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newMockStore(t)
	chain := newMockChainManager()
	am := newMockAccountManager()
	hm := newMockHostManager()
	account := types.GeneratePrivateKey()
	client := newMockHostClient()

	// 3 hosts; the queue iterates through them in order.
	hs := make([]hosts.Host, 3)
	available := make([]types.PublicKey, 0, len(hs))
	for i := range hs {
		sk := types.GeneratePrivateKey()
		hs[i] = client.addTestHost(sk)
		available = append(available, sk.PublicKey())
	}

	// 1 shard - one goroutine will iterate the queue trying each host.
	root, sector := newTestSector()
	shards := [][]byte{sector[:]}
	slab := slabs.Slab{Sectors: []slabs.Sector{{Root: root}}}

	sm := slabs.NewSlabManager(chain, am, nil, hm, store, client, alerts.NewManager(), account, types.GeneratePrivateKey())
	sm.SetShardTimeout(2 * time.Second)

	for _, hk := range available {
		if err := am.UpdateServiceAccountBalance(hk, sm.MigrationAccount(), types.Siacoins(1)); err != nil {
			t.Fatal(err)
		}
	}

	// hs[0] will hit the per-shard timeout (slow WriteSector) -> demote.
	client.slowHosts[hs[0].PublicKey] = 30 * time.Minute
	// hs[1] fails the Usable check fast -> not a timeout, not demoted.
	hm.unusable[hs[1].PublicKey] = struct{}{}
	// hs[2] is healthy -> succeeds, not demoted.

	synctest.Test(t, func(t *testing.T) {
		uploaded, err := sm.UploadShards(context.Background(), slab, shards, available, log)
		if err != nil {
			t.Fatal(err)
		} else if uploaded != 1 {
			t.Fatalf("expected 1 uploaded shard, got %d", uploaded)
		}
	})

	wasDemoted := func(hk types.PublicKey) bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.failedRPCs[hk] > 0
	}

	if !wasDemoted(hs[0].PublicKey) {
		t.Fatalf("expected timed-out host hs[0] to be demoted")
	}
	if wasDemoted(hs[1].PublicKey) {
		t.Fatalf("expected unusable host hs[1] not to be demoted")
	}
	if wasDemoted(hs[2].PublicKey) {
		t.Fatalf("expected successful host hs[2] not to be demoted")
	}
}

func newTestHost(hk types.PublicKey) hosts.Host {
	countries := []string{"US", "DE", "FR", "CN", "JP", "IN", "BR", "RU", "GB", "IT", "ES", "CA", "AU"}
	return hosts.Host{
		PublicKey: hk,
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,

		CountryCode: countries[frand.Intn(len(countries))],
		Latitude:    frand.Float64()*180 - 90,
		Longitude:   frand.Float64()*360 - 180,
		Addresses: []chain.NetAddress{
			{Protocol: siamux.Protocol, Address: "test.siamux:1234"},
			{Protocol: quic.Protocol, Address: "test.quic:2468"},
		},
	}
}

func newTestSector() (types.Hash256, [proto.SectorSize]byte) {
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	root := proto.SectorRoot(&sector)
	return root, sector
}
