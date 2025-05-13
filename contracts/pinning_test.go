package contracts

import (
	"context"
	"errors"
	"net"
	"sort"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type mockSectorPinner struct {
	hp proto.HostPrices
	c  Contractor
}

func newMockSectorPinner(hp proto.HostPrices, c Contractor) *mockSectorPinner {
	return &mockSectorPinner{
		hp: hp,
		c:  c,
	}
}

func (m *mockSectorPinner) PinSectors(ctx context.Context, contractIDs []types.FileContractID, sectors []types.Hash256, log *zap.Logger) (usedContractID types.FileContractID, missing []types.Hash256, _ error) {
	for _, contractID := range contractIDs {
		res, err := m.c.AppendSectors(ctx, nil, m.hp, contractID, sectors)
		if err != nil {
			continue
		} else if len(res.Sectors) == 0 {
			continue
		}

		// figure out which sectors were missing if necessary
		if len(res.Sectors) != len(sectors) {
			lookup := make(map[types.Hash256]struct{}, len(sectors))
			for _, sector := range sectors {
				lookup[sector] = struct{}{}
			}
			for _, sector := range res.Sectors {
				delete(lookup, sector)
			}
			for sector := range lookup {
				missing = append(missing, sector)
			}
		}

		usedContractID = contractID
		return
	}
	return types.FileContractID{}, nil, errors.New("no contract found")
}

type appendSectorCall struct {
	hostPrices proto.HostPrices
	contractID types.FileContractID
	sectors    []types.Hash256
}

type pinCall struct {
	contractID types.FileContractID
	roots      []types.Hash256
}

func (c *contractorMock) AppendSectors(ctx context.Context, tc rhp.TransportClient, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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

func (s *storeMock) PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab slabs.SlabPinParams) (slabs.SlabID, error) {
	// only keep track of sectors
	for _, sector := range slab.Sectors {
		s.sectors[sector.HostKey] = append(s.sectors[sector.HostKey], slabs.Sector{
			Root:       sector.Root,
			ContractID: nil,
			HostKey:    &sector.HostKey,
		})
	}
	return slabs.SlabID{}, nil
}

func (s *storeMock) PinSectors(ctx context.Context, contractID types.FileContractID, roots []types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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
			if _, ok := lookup[sector.Root]; ok {
				updated[i].ContractID = &contractID
			}
		}
		s.sectors[hk] = updated
	}
	return nil
}

func (s *storeMock) MarkSectorsLost(ctx context.Context, hk types.PublicKey, roots []types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// build map
	lookup := make(map[types.Hash256]struct{}, len(roots))
	for _, root := range roots {
		lookup[root] = struct{}{}
	}

	// remove sectors
	updated, ok := s.sectors[hk]
	if !ok {
		panic("no host sectors found")
	}
	for i, sector := range updated {
		if _, ok := lookup[sector.Root]; ok {
			if sector.HostKey == nil || *sector.HostKey != hk {
				panic("sector host key mismatch")
			}
			updated[i].HostKey = nil
			updated[i].ContractID = nil
		}
	}
	s.sectors[hk] = updated
	return nil
}

func (s *storeMock) UnpinnedSectors(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sectors, ok := s.sectors[hostKey]
	if !ok {
		return nil, nil
	}
	var unpinned []types.Hash256
	for _, sector := range sectors {
		if sector.ContractID == nil {
			unpinned = append(unpinned, sector.Root)
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
	if err := store.AddFormedContract(context.Background(), fcid1, hk1, 100, 200, types.ZeroCurrency, types.NewCurrency64(1), types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}
	fcid2 := types.FileContractID{2}
	if err := store.AddFormedContract(context.Background(), fcid2, hk1, 100, 200, types.ZeroCurrency, types.NewCurrency64(1), types.ZeroCurrency, types.ZeroCurrency); err != nil {
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
	if err := store.AddFormedContract(context.Background(), fcid3, hk2, 100, 200, types.ZeroCurrency, types.NewCurrency64(1), types.ZeroCurrency, types.ZeroCurrency); err != nil {
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

	// pin sectors for h1
	_, err := store.PinSlab(context.Background(), proto.Account{}, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{Root: r1, HostKey: hk1},
			{Root: r2, HostKey: hk1},
			{Root: r3, HostKey: hk1},
			{Root: r4, HostKey: hk1},
			{Root: r5, HostKey: hk1},
			{Root: r6, HostKey: hk1},
		},
	})
	// pin sectors for h2
	_, err = store.PinSlab(context.Background(), proto.Account{}, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{Root: r7, HostKey: hk2},
			{Root: r8, HostKey: hk2},
		},
	})

	// pin sectors for h3 - these will remain unpinned
	_, err = store.PinSlab(context.Background(), proto.Account{}, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{Root: frand.Entropy256(), HostKey: types.PublicKey{3}},
			{Root: frand.Entropy256(), HostKey: types.PublicKey{3}},
		},
	})

	// indicate that root 4 is missing
	contractor := newContractorMock()
	contractor.missingSectors[r4] = struct{}{}

	// prepare contract manager
	cm := newContractManager(types.PublicKey{}, nil, nil, contractor, nil, store, nil, nil)

	// pin sectors on h1
	h1Prices := h1.Settings.Prices
	err = cm.performSectorPinningOnHost(context.Background(), newMockSectorPinner(h1Prices, contractor), h1, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert sector pinning on h1
	if len(contractor.appendSectorCalls) != 1 {
		t.Fatalf("expected one call, got %v", len(contractor.appendSectorCalls))
	} else if call := contractor.appendSectorCalls[0]; call.hostPrices != h1Prices {
		t.Fatalf("unexpected host prices %v, expected %v", call.hostPrices, h1Prices)
	} else if call.contractID != fcid2 {
		t.Fatalf("unexpected contract ID %v, expected %v", call.contractID, fcid2)
	} else if len(call.sectors) != 6 {
		t.Fatalf("expected 6 sectors, got %v", call.sectors)
	}

	// pin sectors on h2
	h2Prices := h2.Settings.Prices
	err = cm.performSectorPinningOnHost(context.Background(), newMockSectorPinner(h2Prices, contractor), h2, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert sector pinning on h2
	if len(contractor.appendSectorCalls) != 2 {
		t.Fatalf("expected two calls, got %v", len(contractor.appendSectorCalls))
	} else if call := contractor.appendSectorCalls[1]; call.hostPrices != h2Prices {
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
			switch sector.Root {
			case r1, r2, r3, r5, r6:
				if *sector.ContractID != fcid2 {
					t.Fatalf("expected contract ID %v, got %v", fcid2, *sector.ContractID)
				}
			case r4:
				if sector.ContractID != nil {
					t.Fatalf("expected unpinned sector, got %v", *sector.ContractID)
				}
			default:
				t.Fatalf("unexpected root %v", sector.Root)
			}
		}
	}

	if h2Sectors, ok := store.sectors[hk2]; !ok {
		t.Fatalf("expected sectors for host %v", hk2)
	} else {
		for _, sector := range h2Sectors {
			switch sector.Root {
			case r7, r8:
				if *sector.ContractID != fcid3 {
					t.Fatalf("expected contract ID %v, got %v", fcid3, *sector.ContractID)
				}
			default:
				t.Fatalf("unexpected root %v", sector.Root)
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
			if sector.ContractID != nil {
				t.Fatalf("expected unpinned sector, got %v", *sector.ContractID)
			}
		}
	}
}
