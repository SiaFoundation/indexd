package slabs

import (
	"context"
	"errors"
	"reflect"
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

func TestDownloadCandidates(t *testing.T) {
	// prepare hosts
	h1 := newTestHost(types.PublicKey{1})
	h2 := newTestHost(types.PublicKey{2})
	h3 := newTestHost(types.PublicKey{3})
	h4 := newTestHost(types.PublicKey{4})

	// prepare prices
	h1.Settings.Prices.EgressPrice = types.Siacoins(4)
	h2.Settings.Prices.EgressPrice = types.Siacoins(1)
	h3.Settings.Prices.EgressPrice = types.Siacoins(2)
	h4.Settings.Prices.EgressPrice = types.Siacoins(3)

	// prepare slab with sectors on h1 and h2
	slab := Slab{
		Sectors: []Sector{
			{Root: types.Hash256{1}, HostKey: &h1.PublicKey},
			{Root: types.Hash256{2}, HostKey: nil},
			{Root: types.Hash256{3}, HostKey: &h3.PublicKey},
			{Root: types.Hash256{4}, HostKey: &h4.PublicKey},
		},
	}

	// prepare candidates
	candidates := newDownloadCandidates([]hosts.Host{h1, h2, h3, h4}, slab)

	// test next() method
	if host, ok := candidates.next(); !ok || host.PublicKey != h3.PublicKey {
		t.Fatalf("expected host %v, got %v", h3.PublicKey, host.PublicKey)
	} else if host, ok := candidates.next(); !ok || host.PublicKey != h4.PublicKey {
		t.Fatalf("expected host %v, got %v", h4.PublicKey, host.PublicKey)
	} else if host, ok := candidates.next(); !ok || host.PublicKey != h1.PublicKey {
		t.Fatalf("expected host %v, got %v", h1.PublicKey, host.PublicKey)
	} else if _, ok := candidates.next(); ok {
		t.Fatal("expected no more hosts")
	}

	// assert indices
	if idx, ok := candidates.indices[h1.PublicKey]; !ok || idx != 0 {
		t.Fatalf("expected index for host %v to be 0, got %d", h1.PublicKey, idx)
	} else if _, ok := candidates.indices[h2.PublicKey]; ok {
		t.Fatal("expected no index for host without sector")
	} else if idx, ok := candidates.indices[h3.PublicKey]; !ok || idx != 2 {
		t.Fatalf("expected index for host %v to be 2, got %d", h3.PublicKey, idx)
	} else if idx, ok := candidates.indices[h4.PublicKey]; !ok || idx != 3 {
		t.Fatalf("expected index for host %v to be 3, got %d", h4.PublicKey, idx)
	}
}

func TestDownloadShards(t *testing.T) {
	store := newMockStore()
	am := newMockAccountManager(store)
	hm := newMockHostManager()
	account := types.GeneratePrivateKey()

	settings := func(egress uint64) proto.HostSettings {
		settings := goodSettings
		settings.Prices.EgressPrice = types.NewCurrency64(egress)
		return settings
	}

	// setup includes 3 hosts storing 1 sector each
	hk1, hk2, hk3 := types.PublicKey{1}, types.PublicKey{2}, types.PublicKey{3}
	host1 := hosts.Host{PublicKey: hk1, Settings: settings(2)}
	host2 := hosts.Host{PublicKey: hk2, Settings: settings(3)}
	host3 := hosts.Host{PublicKey: hk3, Settings: settings(1)}
	allHosts := []hosts.Host{host1, host2, host3}

	hm.hosts = map[types.PublicKey]hosts.Host{
		hk1: host1,
		hk2: host2,
		hk3: host3,
	}

	var sector1, sector2, sector3 [proto.SectorSize]byte
	frand.Read(sector1[:])
	frand.Read(sector2[:])
	frand.Read(sector3[:])

	slab := Slab{
		MinShards: 2,
		Sectors: []Sector{
			{Root: proto.SectorRoot(&sector1), ContractID: nil, HostKey: &hk1},
			{Root: proto.SectorRoot(&sector2), ContractID: nil, HostKey: &hk2},
			{Root: proto.SectorRoot(&sector3), ContractID: nil, HostKey: &hk3},
		},
	}

	newClient := func(sector [proto.SectorSize]byte, settings proto.HostSettings) *mockHostClient {
		return &mockHostClient{
			sectors: map[types.Hash256][proto.SectorSize]byte{
				proto.SectorRoot(&sector): sector,
			},
			settings: settings,
		}
	}
	dialer := &mockDialer{clients: map[types.PublicKey]*mockHostClient{
		hk1: newClient(sector1, host1.Settings),
		hk2: newClient(sector2, host2.Settings),
		hk3: newClient(sector3, host3.Settings),
	}}

	sm, err := newSlabManager(am, nil, hm, store, dialer, alerts.NewManager(), account, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	// replenish service account
	initialFunds := types.Siacoins(1)
	fundAccount := func(hostKey types.PublicKey) {
		t.Helper()
		err := am.UpdateServiceAccountBalance(context.Background(), hostKey, sm.migrationAccount, initialFunds)
		if err != nil {
			t.Fatal(err)
		}
	}
	fundAccount(hk1)
	fundAccount(hk2)
	fundAccount(hk3)

	resetTimeouts := func() {
		sm.shardTimeout = 30 * time.Second
		for _, client := range dialer.clients {
			client.delay = 0
		}
	}

	// assert that passing no hosts results in not enough shards being downloaded
	t.Run("no enough hosts", func(t *testing.T) {
		_, err := sm.downloadShards(context.Background(), slab, nil, zap.NewNop())
		if !errors.Is(err, errNotEnoughShards) {
			t.Fatal(err)
		}
	})

	t.Run("no enough hosts", func(t *testing.T) {
		unavailableSlab := slab
		unavailableSlab.Sectors = slices.Clone(unavailableSlab.Sectors)
		unavailableSlab.Sectors[0].HostKey = nil
		unavailableSlab.Sectors[1].HostKey = nil
		_, err := sm.downloadShards(context.Background(), unavailableSlab, nil, zap.NewNop())
		if !errors.Is(err, errNotEnoughShards) {
			t.Fatal(err)
		}
	})

	// assert that if all hosts are passed, we fetch exactly minShards sectors
	// and that we fetch the cheapest ones first
	t.Run("success", func(t *testing.T) {
		sectors, err := sm.downloadShards(context.Background(), slab, allHosts, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(sectors, [][]byte{sector1[:], nil, sector3[:]}) {
			t.Fatal("downloaded sectors do not match expected sectors")
		}
	})

	// assert that if the cheapest host times out, we still succeed.
	t.Run("success with delay", func(t *testing.T) {
		defer resetTimeouts()
		sm.shardTimeout = 100 * time.Millisecond
		dialer.clients[hk3].delay = time.Second
		sectors, err := sm.downloadShards(context.Background(), slab, allHosts, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(sectors, [][]byte{sector1[:], sector2[:], nil}) {
			t.Fatal("downloaded sectors do not match expected sectors")
		}
	})

	// assert that a host losing a sector will mark the sector as lost
	t.Run("success with lost sector", func(t *testing.T) {
		dialer.clients[hk1].sectors = make(map[types.Hash256][proto.SectorSize]byte)
		sectors, err := sm.downloadShards(context.Background(), slab, allHosts, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(sectors, [][]byte{nil, sector2[:], sector3[:]}) {
			t.Fatal("downloaded sectors do not match expected sectors")
		} else if sectors := store.lostSectors[hk1]; len(sectors) != 1 {
			t.Fatalf("expected 1 lost sector for host %v, got %d", hk1, len(store.lostSectors[hk1]))
		} else if _, ok := sectors[proto.SectorRoot(&sector1)]; !ok {
			t.Fatalf("expected sector %v to be marked as lost, but it wasn't", proto.SectorRoot(&sector1))
		}
	})

	// assert that after the downloads, each host has the right remaining
	// balance
	assertBalance := func(host hosts.Host, nSectors uint64) {
		t.Helper()
		cost := host.Settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost().Mul64(nSectors)
		balance, err := sm.am.ServiceAccountBalance(context.Background(), host.PublicKey, sm.migrationAccount)
		if err != nil {
			t.Fatal(err)
		} else if !balance.Equals(initialFunds.Sub(cost)) {
			t.Fatalf("expected balance for host %v is %v, got %v", host.PublicKey, initialFunds.Sub(cost), balance)
		}
	}
	assertBalance(host1, 2)
	assertBalance(host2, 2)
	assertBalance(host3, 2)
}
