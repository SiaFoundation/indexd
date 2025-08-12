package contracts

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type appendSectorCall struct {
	hostPrices proto.HostPrices
	contractID types.FileContractID
	sectors    []types.Hash256
}

func (c *hostClientMock) AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	if c.failsRPCs {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("mocked error")
	}

	c.appendSectorCalls = append(c.appendSectorCalls, appendSectorCall{
		hostPrices: hostPrices,
		contractID: contractID,
		sectors:    sectors,
	})

	appended := make([]types.Hash256, 0, len(sectors))
	for _, sector := range sectors {
		if _, ok := c.missingSectors[sector]; !ok {
			appended = append(appended, sector)
		}
	}

	return rhp.RPCAppendSectorsResult{Sectors: appended}, nil
}

func (c *hostClientMock) Settings(ctx context.Context) (proto.HostSettings, error) {
	return proto.HostSettings{}, nil
}

func (s *storeMock) ContractsForPinning(ctx context.Context, hk types.PublicKey, maxContractSize uint64) ([]types.FileContractID, error) {
	var contracts []Contract
	for _, c := range s.contracts {
		if c.HostKey == hk && !c.RemainingAllowance.IsZero() {
			contracts = append(contracts, c)
		}
	}
	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].Capacity == contracts[j].Capacity {
			return contracts[i].Size > contracts[j].Size
		}
		return contracts[i].Capacity > contracts[j].Capacity
	})

	out := make([]types.FileContractID, len(contracts))
	for i, c := range contracts {
		out[i] = c.ID
	}
	return out, nil
}

func (s *storeMock) HostsForPinning(ctx context.Context) ([]types.PublicKey, error) {
	hasContract := make(map[types.PublicKey]struct{})
	for _, c := range s.contracts {
		hasContract[c.HostKey] = struct{}{}
	}

	var hosts []types.PublicKey
	for hk := range s.hosts {
		if _, ok := hasContract[hk]; ok {
			hosts = append(hosts, hk)
		}
	}

	return hosts, nil
}

func (s *storeMock) PinSectors(ctx context.Context, contractID types.FileContractID, roots []types.Hash256) error {
	// find host key
	var hk types.PublicKey
	for _, contract := range s.contracts {
		if contract.ID == contractID {
			hk = contract.HostKey
			break
		}
	}
	if hk == (types.PublicKey{}) {
		panic("contract not found")
	}

	lookup := make(map[types.Hash256]struct{}, len(roots))
	for _, root := range roots {
		lookup[root] = struct{}{}
	}

	// pin sectors
	if updated, ok := s.sectors[hk]; ok {
		for i, sector := range updated {
			if _, ok := lookup[sector.root]; ok {
				updated[i].contractID = &contractID
			}
		}
		s.sectors[hk] = updated
	}
	return nil
}

func (s *storeMock) MarkSectorsLost(ctx context.Context, hk types.PublicKey, roots []types.Hash256) error {
	// build map
	lookup := make(map[types.Hash256]struct{}, len(roots))
	for _, root := range roots {
		lookup[root] = struct{}{}
	}

	// mark sectors as lost
	updated, ok := s.sectors[hk]
	if !ok {
		panic("no host sectors found")
	}
	for i, sector := range updated {
		if _, ok := lookup[sector.root]; ok {
			updated[i].contractID = nil
		}
	}
	s.sectors[hk] = updated
	return nil
}

func (s *storeMock) UnpinnedSectors(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	sectors, ok := s.sectors[hostKey]
	if !ok {
		return nil, nil
	}
	var unpinned []types.Hash256
	for _, sector := range sectors {
		if sector.contractID == nil {
			unpinned = append(unpinned, sector.root)
		}
	}
	if len(unpinned) > limit {
		unpinned = unpinned[:limit]
	}
	return unpinned, nil
}

func TestPerformSectorPinningOnHost(t *testing.T) {
	store := newStoreMock()

	// prepare two hosts
	hk1 := types.PublicKey{1}
	h1 := hosts.Host{
		PublicKey: hk1,
		Networks:  []net.IPNet{{IP: net.IP{127, 0, 0, 1}, Mask: net.CIDRMask(24, 32)}},
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host1.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	h1.Settings.Prices.StoragePrice = types.NewCurrency64(123)
	h1.Settings.Prices.TipHeight = 111
	store.hosts[hk1] = h1

	hk2 := types.PublicKey{2}
	h2 := hosts.Host{
		PublicKey: hk2,
		Networks:  []net.IPNet{{IP: net.IP{127, 0, 0, 2}, Mask: net.CIDRMask(24, 32)}},
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host2.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	h2.Settings.Prices.StoragePrice = types.NewCurrency64(456)
	h2.Settings.Prices.TipHeight = 222
	store.hosts[hk2] = h2

	// add two contracts for h1
	fcid1 := types.FileContractID{1}
	if err := store.AddFormedContract(context.Background(), hk1, fcid1, newTestRevision(hk1), types.ZeroCurrency, types.NewCurrency64(1), types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}
	fcid2 := types.FileContractID{2}
	if err := store.AddFormedContract(context.Background(), hk1, fcid2, newTestRevision(hk1), types.ZeroCurrency, types.NewCurrency64(1), types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}

	// make fcid2 the better contract for pinning
	for i, c := range store.contracts {
		switch c.ID {
		case fcid1:
			store.contracts[i].Capacity = 100
		case fcid2:
			store.contracts[i].Capacity = 200
		}
	}

	// add one contract for h2
	fcid3 := types.FileContractID{3}
	if err := store.AddFormedContract(context.Background(), hk2, fcid3, newTestRevision(hk2), types.ZeroCurrency, types.NewCurrency64(1), types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}

	// prepare roots
	r1 := types.Hash256{1}
	r2 := types.Hash256{2}
	r3 := types.Hash256{3}
	r4 := types.Hash256{4}
	r5 := types.Hash256{5}
	r6 := types.Hash256{6}
	r7 := types.Hash256{7}
	r8 := types.Hash256{8}

	// prepare sectors for h1
	store.sectors[hk1] = []sector{
		{root: r1},
		{root: r2},
		{root: r3},
		{root: r4},
		{root: r5},
		{root: r6},
	}

	// prepare sectors for h2
	store.sectors[hk2] = []sector{
		{root: r7},
		{root: r8},
	}

	// prepare sectors for h3 - these will remain unpinned
	store.sectors[types.PublicKey{3}] = []sector{
		{root: frand.Entropy256()},
		{root: frand.Entropy256()},
	}

	// prepare dialer
	h1Mock := newHostClientMock()
	h2Mock := newHostClientMock()

	dialer := newDialerMock()
	dialer.clients[hk1] = h1Mock
	dialer.clients[hk2] = h2Mock

	// indicate that root 4 is missing
	dialer.clients[hk1].missingSectors[r4] = struct{}{}

	// prepare hm
	hm := newHostManagerMock(store)
	hm.settings[hk1] = h1.Settings
	hm.settings[hk2] = h2.Settings

	// prepare contract manager
	cm := newContractManager(types.PublicKey{}, nil, nil, store, dialer, hm, nil, nil)

	// pin sectors on h1
	h1Prices := h1.Settings.Prices
	err := cm.performSectorPinningOnHost(context.Background(), h1, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert sector pinning on h1
	if len(h1Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one call, got %v", len(h1Mock.appendSectorCalls))
	} else if call := h1Mock.appendSectorCalls[0]; call.hostPrices != h1Prices {
		t.Fatalf("unexpected host prices %v, expected %v", call.hostPrices, h1Prices)
	} else if call.contractID != fcid2 {
		t.Fatalf("unexpected contract ID %v, expected %v", call.contractID, fcid2)
	} else if len(call.sectors) != 6 {
		t.Fatalf("expected 6 sectors, got %v", call.sectors)
	}

	// pin sectors on h2
	h2Prices := h2.Settings.Prices
	err = cm.performSectorPinningOnHost(context.Background(), h2, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert sector pinning on h2
	if len(h2Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one calls, got %v", len(h2Mock.appendSectorCalls))
	} else if call := h2Mock.appendSectorCalls[0]; call.hostPrices != h2Prices {
		t.Fatalf("unexpected host prices %v, got %v", call.hostPrices, h2Prices)
	} else if call.contractID != fcid3 {
		t.Fatalf("expected contract ID %v, got %v", call.contractID, fcid3)
	} else if len(call.sectors) != 2 {
		t.Fatalf("expected 2 sectors, got %v", len(call.sectors))
	}

	// assert sectors are pinned in the store
	if h1Sectors, ok := store.sectors[hk1]; !ok {
		t.Fatalf("expected sectors for host %v", hk1)
	} else {
		for _, sector := range h1Sectors {
			switch sector.root {
			case r1, r2, r3, r5, r6:
				if *sector.contractID != fcid2 {
					t.Fatalf("expected contract ID %v, got %v", fcid2, *sector.contractID)
				}
			case r4:
				if sector.contractID != nil {
					t.Fatalf("expected unpinned sector, got %v", *sector.contractID)
				}
			default:
				t.Fatalf("unexpected root %v", sector.root)
			}
		}
	}

	if h2Sectors, ok := store.sectors[hk2]; !ok {
		t.Fatalf("expected sectors for host %v", hk2)
	} else {
		for _, sector := range h2Sectors {
			switch sector.root {
			case r7, r8:
				if *sector.contractID != fcid3 {
					t.Fatalf("expected contract ID %v, got %v", fcid3, *sector.contractID)
				}
			default:
				t.Fatalf("unexpected root %v", sector.root)
			}
		}
	}

	if h3Sectors, ok := store.sectors[types.PublicKey{3}]; !ok {
		t.Fatalf("expected sectors for host %v", types.PublicKey{3})
	} else {
		if len(h3Sectors) != 2 {
			t.Fatalf("expected 2 sectors, got %v", len(h3Sectors))
		}
		for _, sector := range h3Sectors {
			if sector.contractID != nil {
				t.Fatalf("expected unpinned sector, got %v", *sector.contractID)
			}
		}
	}
}
