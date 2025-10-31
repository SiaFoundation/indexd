package slabs

import (
	"bytes"
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

	// try to add duplicate hosts
	candidates := newDownloadCandidates([]hosts.Host{h1, h1, h1, h1, h2, h3, h4}, slab)
	if len(candidates.hosts) != 3 {
		t.Fatalf("expected 3 unique candidates, got %d", len(candidates.hosts))
	}

	// prepare candidates
	candidates = newDownloadCandidates([]hosts.Host{h1, h2, h3, h4}, slab)

	// test next() method
	selected := make(map[types.PublicKey]struct{})
	for range 3 {
		if host, ok := candidates.next(); ok {
			selected[host.PublicKey] = struct{}{}
		}
	}
	if _, ok := candidates.next(); ok {
		t.Fatal("expected no more hosts")
	} else if len(selected) != 3 {
		t.Fatalf("expected 3 selected hosts, got %d", len(selected))
	} else if _, ok := selected[h1.PublicKey]; !ok {
		t.Fatal("expected host 1 to be selected")
	} else if _, ok := selected[h3.PublicKey]; !ok {
		t.Fatal("expected host 3 to be selected")
	} else if _, ok := selected[h4.PublicKey]; !ok {
		t.Fatal("expected host 4 to be selected")
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

	// setup includes 3 hosts storing 1 sector each
	hk1, hk2, hk3 := types.PublicKey{1}, types.PublicKey{2}, types.PublicKey{3}
	host1 := hosts.Host{PublicKey: hk1, Settings: goodSettings}
	host2 := hosts.Host{PublicKey: hk2, Settings: goodSettings}
	host3 := hosts.Host{PublicKey: hk3, Settings: goodSettings}

	// assert costs are non-zero
	allHosts := []hosts.Host{host1, host2, host3}
	for _, host := range allHosts {
		cost := host.Settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost()
		if cost.IsZero() {
			t.Fatal("expected non-zero cost for reading sector")
		}
	}

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

	account := types.GeneratePrivateKey()
	sm, err := newSlabManager(am, nil, hm, store, dialer, alerts.NewManager(), account, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	resetTimeouts := func() {
		sm.shardTimeout = 30 * time.Second
		for _, client := range dialer.clients {
			client.delay = 0
		}
	}

	pool := newConnPool(sm.dialer, zap.NewNop())
	defer pool.Close()

	// assert that passing no hosts results in errNotEnoughShards
	t.Run("not enough hosts", func(t *testing.T) {
		_, err := sm.downloadShards(context.Background(), slab, nil, pool, zap.NewNop())
		if !errors.Is(err, errNotEnoughShards) {
			t.Fatal(err)
		}
	})

	// assert that passing in a slab with not enough available sectors results
	// in errNotEnoughShards
	t.Run("not enough hosts", func(t *testing.T) {
		unavailableSlab := slab
		unavailableSlab.Sectors = slices.Clone(unavailableSlab.Sectors)
		unavailableSlab.Sectors[0].HostKey = nil
		unavailableSlab.Sectors[1].HostKey = nil
		_, err := sm.downloadShards(context.Background(), unavailableSlab, nil, pool, zap.NewNop())
		if !errors.Is(err, errNotEnoughShards) {
			t.Fatal(err)
		}
	})

	// assert that if all hosts are passed, we succeed and fetch exactly 'minShards' sectors
	t.Run("success", func(t *testing.T) {
		sectors, err := sm.downloadShards(context.Background(), slab, allHosts, pool, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}

		fetchedS1 := sectors[0] != nil
		fetchedS2 := sectors[1] != nil
		fetchedS3 := sectors[2] != nil

		if fetchedS1 && !bytes.Equal(sectors[0], sector1[:]) {
			t.Fatal("downloaded sector 1 does not match expected data")
		} else if fetchedS2 && !bytes.Equal(sectors[1], sector2[:]) {
			t.Fatal("downloaded sector 2 does not match expected data")
		} else if fetchedS3 && !bytes.Equal(sectors[2], sector3[:]) {
			t.Fatal("downloaded sector 3 does not match expected data")
		}

		if fetchedS1 && !fetchedS2 && !fetchedS3 {
			t.Fatal("expected at least 2 sectors to be fetched")
		} else if !fetchedS1 && fetchedS2 && !fetchedS3 {
			t.Fatal("expected at least 2 sectors to be fetched")
		} else if !fetchedS1 && !fetchedS2 && fetchedS3 {
			t.Fatal("expected at least 2 sectors to be fetched")
		}
	})

	// update service account balances to assert debiting
	initialFunds := types.Siacoins(1)
	for _, hk := range []types.PublicKey{hk1, hk2, hk3} {
		if err := am.UpdateServiceAccountBalance(context.Background(), hk, sm.migrationAccount, initialFunds); err != nil {
			t.Fatal(err)
		}
	}

	// assert that if h3 times out, we still succeed
	t.Run("success with delay", func(t *testing.T) {
		defer resetTimeouts()
		sm.shardTimeout = 100 * time.Millisecond
		dialer.clients[hk3].delay = time.Second
		sectors, err := sm.downloadShards(context.Background(), slab, allHosts, pool, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(sectors, [][]byte{sector1[:], sector2[:], nil}) {
			t.Fatal("downloaded sectors do not match expected sectors")
		}
	})

	// assert that a host losing a sector will mark the sector as lost
	t.Run("mark sector lost", func(t *testing.T) {
		dialer.clients[hk1].sectors = make(map[types.Hash256][proto.SectorSize]byte)
		_, err := sm.downloadShards(context.Background(), slab, []hosts.Host{host1, host2}, pool, zap.NewNop())
		if !errors.Is(err, errNotEnoughShards) {
			t.Fatal("expected not enough hosts error due to lost sector")
		} else if sectors, ok := store.lostSectors[hk1]; !ok {
			t.Fatalf("expected lost sector for host %v, got none", hk1)
		} else if len(sectors) != 1 {
			t.Fatalf("expected 1 lost sector for host %v, got %d %+v", hk1, len(store.lostSectors[hk1]), store.lostSectors)
		} else if _, ok := sectors[proto.SectorRoot(&sector1)]; !ok {
			t.Fatalf("expected sector %v to be marked as lost, but it wasn't", proto.SectorRoot(&sector1))
		}
	})

	// assert service account balance after downloads
	assertBalance := func(host hosts.Host, nSectors uint64) {
		t.Helper()
		cost := host.Settings.Prices.RPCReadSectorCost(proto.SectorSize).RenterCost()
		expected := initialFunds.Sub(cost.Mul64(nSectors))
		got, err := sm.am.ServiceAccountBalance(context.Background(), host.PublicKey, sm.migrationAccount)
		if err != nil {
			t.Fatal(err)
		} else if !got.Equals(expected) {
			t.Fatalf("expected balance for host %v is %v, got %v", host.PublicKey, expected, got)
		}
	}
	assertBalance(host1, 1)
	assertBalance(host2, 2)
	assertBalance(host3, 0)
}
