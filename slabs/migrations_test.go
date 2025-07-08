package slabs

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"reflect"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var goodSettings = proto.HostSettings{
	AcceptingContracts: true,
	RemainingStorage:   math.MaxUint32,
	Prices: proto.HostPrices{
		ContractPrice: types.Siacoins(1),
		Collateral:    types.NewCurrency64(1),
		StoragePrice:  types.NewCurrency64(1),
	},
	MaxContractDuration: 90 * 144,
	MaxCollateral:       types.Siacoins(1000),
}

func TestContractsForRepair(t *testing.T) {
	newHost := func(i byte, usable, blocked, networks bool) hosts.Host {
		h := hosts.Host{
			Blocked:   blocked,
			PublicKey: types.PublicKey{i},
			Settings:  goodSettings,
		}
		if usable {
			h.Usability = hosts.GoodUsability
		}
		if networks {
			h.Networks = []net.IPNet{{IP: net.IP{1, 1, 1, i}, Mask: net.CIDRMask(24, 32)}}
		}
		return h
	}

	newContract := func(i byte, hk types.PublicKey, goodForUpload bool) contracts.Contract {
		c := contracts.Contract{
			ID:                 types.FileContractID{i},
			HostKey:            hk,
			Good:               goodForUpload,
			RemainingAllowance: types.MaxCurrency,
			TotalCollateral:    types.MaxCurrency,
		}
		if isGood := c.GoodForUpload(goodSettings.Prices, goodSettings.MaxCollateral, 100); isGood != goodForUpload {
			// sanity check
			t.Fatalf("contract %d: expected goodForUpload %v, got %v", i, goodForUpload, isGood)
		}
		return c
	}

	// good host with good contract
	goodHost := newHost(1, true, false, true)
	goodContract := newContract(1, goodHost.PublicKey, true)

	// good host with bad contract
	badContract := newContract(2, goodHost.PublicKey, false)

	// good host with redundant CIDR
	redundantCIDRHost := newHost(2, true, false, true)
	redundantCIDRHost.Networks = goodHost.Networks
	redundantCIDRContract := newContract(3, redundantCIDRHost.PublicKey, true)

	// prepare a slab that has one good sector and multiple bad sectors for
	// various reasons
	slab := Slab{
		Sectors: []Sector{
			// good sector -> don't migrate
			{
				Root:       types.Hash256{1},
				ContractID: &goodContract.ID,
				HostKey:    &goodContract.HostKey,
			},
			// lost sector -> migrate
			{
				Root:       types.Hash256{2},
				ContractID: nil,
				HostKey:    nil,
			},
			// bad contract -> migrate
			{
				Root:       types.Hash256{3},
				ContractID: &badContract.ID,
				HostKey:    &badContract.HostKey,
			},
			// good contract with host on redundant CIDR -> don't migrate
			{
				Root:       types.Hash256{4},
				ContractID: &redundantCIDRContract.ID,
				HostKey:    &redundantCIDRContract.HostKey,
			},
		},
	}
	_ = slab

	// helper to assert result of contractsForRepair
	assertResult := func(availableHosts []hosts.Host, availableContracts []contracts.Contract, expectedRoots, expectedContracts []int) {
		t.Helper()
		toRepair, toUse := contractsForRepair(slab, availableHosts, availableContracts, 100)
		if len(toRepair) != len(expectedRoots) {
			t.Fatalf("expected %d roots to repair, got %d: %v", len(expectedRoots), len(toRepair), toRepair)
		} else if len(toUse) != len(expectedContracts) {
			t.Fatalf("expected %d contracts to use, got %d: %v", len(expectedContracts), len(toUse), toUse)
		}
		for i := range toRepair {
			if toRepair[i] != expectedRoots[i] {
				t.Fatalf("expected root %d to repair, got %d", expectedRoots[i], toRepair[i])
			}
		}
		expectedContractsMap := make(map[types.FileContractID]struct{})
		for i := range expectedContracts {
			expectedContractsMap[types.FileContractID{byte(expectedContracts[i])}] = struct{}{}
		}
		for i := range toUse {
			if _, ok := expectedContractsMap[toUse[i].ID]; !ok {
				t.Fatalf("contract %v is unexpected", toUse[i].ID)
			}
		}
	}

	// with no contracts or hosts, all sectors require migration but no
	// contracts are available
	assertResult(nil, nil, []int{0, 1, 2, 3}, []int{})

	// calling contractsForRepair with just the hosts and contracts the slab is stored on should
	// return the missing sectors and no contracts
	allHosts := []hosts.Host{goodHost, redundantCIDRHost}
	allContracts := []contracts.Contract{goodContract, badContract, redundantCIDRContract}
	assertResult(allHosts, allContracts, []int{1, 2}, []int{})

	// prepare a bunch of hosts and contracts which can't be used for repairs
	badHost2 := newHost(3, false, false, true)
	cBadHost2 := newContract(4, badHost2.PublicKey, true)

	hostWithoutNetworks := newHost(4, true, false, false)
	cHostWithoutNetworks := newContract(5, hostWithoutNetworks.PublicKey, true)

	blockedHost := newHost(5, true, true, false)
	cBlockedHost := newContract(6, blockedHost.PublicKey, true)

	redundantCIDRHost2 := newHost(6, true, false, true)
	redundantCIDRHost2.Networks = goodHost.Networks
	cRedundantCIDRHost2 := newContract(7, redundantCIDRHost2.PublicKey, true)

	// add the bad hosts+contracts and try again - expect same result
	allHosts = append(allHosts, badHost2, hostWithoutNetworks, blockedHost, redundantCIDRHost2)
	allContracts = append(allContracts, cBadHost2, cHostWithoutNetworks, cBlockedHost, cRedundantCIDRHost2)
	assertResult(allHosts, allContracts, []int{1, 2}, []int{})

	// prepare 2 good hosts
	goodHost2 := newHost(7, true, false, true)
	cGoodHost2 := newContract(8, goodHost2.PublicKey, true)

	goodHost3 := newHost(8, true, false, true)
	cGoodHost3 := newContract(9, goodHost3.PublicKey, true)

	// should use them
	allHosts = append(allHosts, goodHost2, goodHost3)
	allContracts = append(allContracts, cGoodHost2, cGoodHost3)
	assertResult(allHosts, allContracts, []int{1, 2}, []int{8, 9})
}

type mockDialer struct {
	clients map[types.PublicKey]*mockHostClient
}

func newMockDialer(hosts []hosts.Host) *mockDialer {
	clients := make(map[types.PublicKey]*mockHostClient, len(hosts))
	for _, host := range hosts {
		clients[host.PublicKey] = &mockHostClient{
			sectors:  make(map[types.Hash256][proto.SectorSize]byte),
			settings: host.Settings,
		}
	}
	return &mockDialer{clients: clients}
}

func (d *mockDialer) DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error) {
	if client, ok := d.clients[hostKey]; ok {
		return client, nil
	}
	return nil, errors.New("failed to dial host")
}

type mockHostClient struct {
	delay    time.Duration
	sectors  map[types.Hash256][proto.SectorSize]byte
	settings proto.HostSettings
}

func (c *mockHostClient) ReadSector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (rhp.RPCReadSectorResult, error) {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return rhp.RPCReadSectorResult{}, ctx.Err()
	}

	sector, ok := c.sectors[root]
	if !ok {
		return rhp.RPCReadSectorResult{}, proto.ErrSectorNotFound
	}
	_, err := w.Write(sector[:])
	if err != nil {
		return rhp.RPCReadSectorResult{}, err
	}
	return rhp.RPCReadSectorResult{
		Usage: c.settings.Prices.RPCReadSectorCost(proto.SectorSize),
	}, nil
}

func (c *mockHostClient) WriteSector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, data io.Reader, length uint64) (rhp.RPCWriteSectorResult, error) {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return rhp.RPCWriteSectorResult{}, ctx.Err()
	}

	var sector [proto.SectorSize]byte
	_, err := io.ReadFull(data, sector[:])
	if err != nil {
		return rhp.RPCWriteSectorResult{}, err
	}
	root := proto.SectorRoot(&sector)
	c.sectors[root] = sector
	return rhp.RPCWriteSectorResult{
		Root:  root,
		Usage: c.settings.Prices.RPCWriteSectorCost(proto.SectorSize),
	}, nil
}

func (c *mockHostClient) Settings(context.Context, types.PublicKey) (proto.HostSettings, error) {
	return c.settings, nil
}

func TestDownloadSlab(t *testing.T) {
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

	sm, err := newSlabManager(am, hm, store, dialer, account, types.GeneratePrivateKey())
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
		_, err := sm.downloadShards(context.Background(), slab, nil, zap.NewNop())
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
