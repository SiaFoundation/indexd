package slabs

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestUploadShards(t *testing.T) {
	// prepare dependencies
	store := newMockStore()
	am := newMockAccountManager(store)
	hm := newMockHostManager()
	account := types.GeneratePrivateKey()

	// prepare dialer
	h1 := newTestHost(types.PublicKey{1})
	h2 := newTestHost(types.PublicKey{2})
	h3 := newTestHost(types.PublicKey{3})
	h4 := newTestHost(types.PublicKey{4})
	dialer := newMockDialer([]hosts.Host{h1, h2, h3, h4})

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
	sm, err := newSlabManager(am, nil, hm, store, dialer, alerter, account, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}
	sm.shardTimeout = 50 * time.Millisecond

	pool := newConnPool(sm.dialer, zap.NewNop())
	defer pool.Close()

	// assert passing in no hosts returns an error
	_, err = sm.uploadShards(context.Background(), slab, shards, nil, pool, zap.NewNop())
	if !errors.Is(err, errNotEnoughHosts) {
		t.Fatalf("expected [errNotEnoughHosts] got %v", err)
	}

	// assert passing in too few hosts returns the uploaded shards alongside an error
	uploaded, err := sm.uploadShards(context.Background(), slab, shards, []hosts.Host{h1, h2}, pool, zap.NewNop())
	if !errors.Is(err, errNotEnoughHosts) {
		t.Fatalf("expected [errNotEnoughHosts] got %v", err)
	} else if len(uploaded) != 2 {
		t.Fatalf("expected 2 uploaded shards, got %d", len(uploaded))
	}

	// assert passing in enough hosts uploads all shards
	uploaded, err = sm.uploadShards(context.Background(), slab, shards, []hosts.Host{h1, h2, h3}, pool, zap.NewNop())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if len(uploaded) != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", len(uploaded))
	}

	// assert hosts are tried until one succeeds
	dialer.clients[h1.PublicKey].delay = time.Second
	uploaded, err = sm.uploadShards(context.Background(), slab, shards, []hosts.Host{h1, h2, h3, h4}, pool, zap.NewNop())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if len(uploaded) != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", len(uploaded))
	}

	// assert the upload fails upon a root mismatch
	corrupted := Slab{Sectors: slices.Clone(slab.Sectors)}
	corrupted.Sectors[1].Root = types.Hash256{}
	uploaded, err = sm.uploadShards(context.Background(), corrupted, shards, []hosts.Host{h1, h2, h3, h4}, pool, zap.NewNop())
	if !errors.Is(err, errRootMismatch) {
		t.Fatalf("expected [errRootMismatch] got %v", err)
	} else if len(uploaded) != 1 {
		t.Fatalf("expected 1 uploaded shard, got %d", len(uploaded))
	}

	// asserts hosts are debited for the upload
	err = am.UpdateServiceAccountBalance(context.Background(), h2.PublicKey, sm.migrationAccount, types.Siacoins(1))
	if err != nil {
		t.Fatal(err)
	}
	uploaded, err = sm.uploadShards(context.Background(), slab, shards, []hosts.Host{h2}, pool, zap.NewNop())
	if !errors.Is(err, errNotEnoughHosts) {
		t.Fatalf("expected [errNotEnoughHosts] got %v", err)
	} else if len(uploaded) != 1 {
		t.Fatalf("expected 1 uploaded shard, got %d", len(uploaded))
	}

	balance, err := sm.am.ServiceAccountBalance(context.Background(), h2.PublicKey, sm.migrationAccount)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.Siacoins(1).Sub(h2.Settings.Prices.RPCWriteSectorCost(proto.SectorSize).RenterCost())) {
		t.Fatalf("unexpected balance %v", balance)
	}

	// reset clients
	for _, client := range dialer.clients {
		client.sectors = make(map[types.Hash256][proto.SectorSize]byte)
		client.delay = 0
	}

	// assert uploaded shards are stored on the hosts
	uploaded, err = sm.uploadShards(context.Background(), slab, shards, []hosts.Host{h1, h2, h3}, pool, zap.NewNop())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if len(uploaded) != 3 {
		t.Fatalf("expected 3 uploaded shards, got %d", len(uploaded))
	}
	seen := map[types.Hash256]struct{}{
		root1: {},
		root2: {},
		root3: {},
	}
	for _, upload := range uploaded {
		if len(dialer.clients[upload.HostKey].sectors) != 1 {
			t.Fatal("unexpected number of uploaded sectors", len(dialer.clients[upload.HostKey].sectors))
		} else if _, ok := dialer.clients[upload.HostKey].sectors[upload.Root]; !ok {
			t.Fatal("expected sector to be uploaded", upload.Root)
		}
		delete(seen, upload.Root)
	}
	if len(seen) != 0 {
		t.Fatalf("expected all sectors to be uploaded, but %v were not", seen)
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
