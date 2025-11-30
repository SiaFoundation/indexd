package slabs

import (
	"context"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestUploadShards(t *testing.T) {
	log := zaptest.NewLogger(t)
	// prepare dependencies
	store := newMockStore()
	chain := newMockChainManager()
	am := newMockAccountManager(store)
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

	slab := Slab{
		Sectors: []Sector{
			{Root: root1},
			{Root: root2},
			{Root: root3},
		},
	}

	// create manager
	alerter := alerts.NewManager()
	sm, err := newSlabManager(chain, am, nil, hm, store, client, alerter, account, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}
	sm.shardTimeout = 50 * time.Millisecond

	// set balance to 1SC
	for _, hostKey := range availableHosts {
		err = am.UpdateServiceAccountBalance(context.Background(), hostKey, sm.migrationAccount, types.Siacoins(1))
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
	_, err = sm.uploadShards(context.Background(), slab, shards, nil, zap.NewNop())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	assertSectors(t, nil, 0, nil)

	// assert passing in enough hosts uploads all shards
	uploaded, err := sm.uploadShards(context.Background(), slab, shards, availableHosts[:3], log)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if uploaded != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", uploaded)
	}
	assertSectors(t, []types.Hash256{root1, root2, root3}, 3, nil)

	// asserts hosts are debited for the upload
	for _, h := range hosts[:3] {
		balance, err := sm.am.ServiceAccountBalance(context.Background(), h.PublicKey, sm.migrationAccount)
		if err != nil {
			t.Fatal(err)
		} else if !balance.Equals(types.Siacoins(1).Sub(h.Settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost())) {
			t.Fatalf("unexpected balance %v", balance)
		}
	}

	// assert passing in too few hosts returns the uploaded shards and no error
	uploaded, err = sm.uploadShards(context.Background(), slab, shards, availableHosts[:2], log)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if uploaded != 2 {
		t.Fatalf("expected 2 uploaded shards, got %d", uploaded)
	}
	assertSectors(t, []types.Hash256{root1, root2, root3}, 2, nil) // all are possible, but only 2 should be succeed

	// assert hosts are tried until one succeeds
	client.slowHosts[hosts[0].PublicKey] = time.Second
	uploaded, err = sm.uploadShards(context.Background(), slab, shards, availableHosts, log)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if uploaded != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", uploaded)
	}
	assertSectors(t, []types.Hash256{root1, root2, root3}, 3, nil)

	// assert migrations are not successful if sector roots
	// do not match
	corrupted := Slab{Sectors: slices.Clone(slab.Sectors)}
	corrupted.Sectors[1].Root = frand.Entropy256()
	uploaded, err = sm.uploadShards(context.Background(), corrupted, shards, availableHosts, log)
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

func newTestHost(hk types.PublicKey) hosts.Host {
	countries := []string{"US", "DE", "FR", "CN", "JP", "IN", "BR", "RU", "GB", "IT", "ES", "CA", "AU"}
	return hosts.Host{
		PublicKey: hk,
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,

		CountryCode: countries[frand.Intn(len(countries))],
		Latitude:    frand.Float64()*180 - 90,
		Longitude:   frand.Float64()*360 - 180,
	}
}

func newTestSector() (types.Hash256, [proto.SectorSize]byte) {
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	root := proto.SectorRoot(&sector)
	return root, sector
}
