package contracts

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type appendSectorCall struct {
	hostPrices proto.HostPrices
	contractID types.FileContractID
	sectors    []types.Hash256
}

func (c *hostClientMock) AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, int, error) {
	if c.failsRPCs {
		return rhp.RPCAppendSectorsResult{}, 0, fmt.Errorf("mocked error")
	}

	c.appendSectorCalls = append(c.appendSectorCalls, appendSectorCall{
		hostPrices: hostPrices,
		contractID: contractID,
		sectors:    slices.Clone(sectors),
	})

	n := len(sectors)
	if c.maxPinnedPerAppend != 0 {
		n = min(c.maxPinnedPerAppend, n)
	}
	appended := make([]types.Hash256, 0, len(sectors))
	for _, sector := range sectors[:n] {
		if _, ok := c.missingSectors[sector]; !ok {
			appended = append(appended, sector) // skip missing sectors
		}
	}

	return rhp.RPCAppendSectorsResult{Sectors: appended}, n, nil
}

func (c *hostClientMock) Settings(ctx context.Context) (proto.HostSettings, error) {
	return proto.HostSettings{}, nil
}

func (s *storeMock) ContractsForPinning(hk types.PublicKey, maxContractSize uint64) ([]types.FileContractID, error) {
	var contracts []Contract
	for _, c := range s.contracts {
		if c.HostKey == hk && !c.RemainingAllowance.IsZero() {
			contracts = append(contracts, c)
		}
	}
	sort.Slice(contracts, func(i, j int) bool {
		// prefer contracts with more empty capacity
		remainingI := contracts[i].Capacity - contracts[i].Size
		remainingJ := contracts[j].Capacity - contracts[j].Size
		return remainingI > remainingJ
	})

	out := make([]types.FileContractID, len(contracts))
	for i, c := range contracts {
		out[i] = c.ID
	}
	return out, nil
}

func (s *storeMock) HostsForPinning() ([]types.PublicKey, error) {
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

func (s *storeMock) PinSectors(contractID types.FileContractID, roots []types.Hash256) error {
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

func (s *storeMock) MarkSectorsLost(hk types.PublicKey, roots []types.Hash256) error {
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

func (s *storeMock) UnpinnedSectors(hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
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
	log := zaptest.NewLogger(t)
	store := newStoreMock()

	// prepare two hosts
	hk1 := types.PublicKey{1}
	h1 := hosts.Host{
		PublicKey: hk1,
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
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host2.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	h2.Settings.Prices.StoragePrice = types.NewCurrency64(456)
	h2.Settings.Prices.TipHeight = 222
	store.hosts[hk2] = h2

	// add two contracts for h1
	fcid1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk1, true, types.FileContractID{2})

	// contracts with greater capacity are preferred
	for i, c := range store.contracts {
		switch c.ID {
		case fcid1:
			store.contracts[i].Capacity = 100
		case fcid2:
			store.contracts[i].Capacity = 200
		}
	}

	// add one contract for h2
	fcid3 := store.addTestContract(t, hk2, true, types.FileContractID{3})

	// prepare roots
	roots := make([]types.Hash256, 8)
	for i := range roots {
		roots[i] = frand.Entropy256()
	}

	// prepare sectors for h1
	store.sectors[hk1] = []sector{
		{root: roots[0]},
		{root: roots[1]},
		{root: roots[2]},
		{root: roots[3]},
		{root: roots[4]},
		{root: roots[5]},
	}

	// prepare sectors for h2
	store.sectors[hk2] = []sector{
		{root: roots[6]},
		{root: roots[7]},
	}

	// prepare dialer
	h1Mock := newHostClientMock()
	h2Mock := newHostClientMock()

	dialer := newDialerMock()
	dialer.clients[hk1] = h1Mock
	dialer.clients[hk2] = h2Mock

	// indicate that root 4 is missing
	dialer.clients[hk1].missingSectors[roots[3]] = struct{}{}

	// prepare hm
	hm := newHostManagerMock(store)
	hm.settings[hk1] = h1.Settings
	hm.settings[hk2] = h2.Settings

	// prepare contract manager
	cm := newContractManager(types.PublicKey{}, nil, nil, store, dialer, hm, nil, nil)
	assertSectorsContract := func(hostKey types.PublicKey, roots []types.Hash256, contractID *types.FileContractID) {
		t.Helper()

		sectors, ok := store.sectors[hostKey]
		if !ok {
			t.Fatalf("expected sectors for host %v", hostKey)
		}
		for _, sector := range sectors {
			if !slices.Contains(roots, sector.root) || (sector.contractID == nil && contractID == nil) {
				// sector not part of the check, or both unpinned
				continue
			} else if contractID == nil && sector.contractID != nil {
				t.Fatalf("expected sector %v to be unpinned, got %v", sector.root, *sector.contractID)
			} else if contractID != nil && sector.contractID == nil {
				t.Fatalf("expected sector %v to be pinned to contract %v, got unpinned", sector.root, *contractID)
			} else if *sector.contractID != *contractID {
				t.Fatalf("expected sector %v to be pinned to contract %v, got %v", sector.root, *contractID, *sector.contractID)
			}
		}
	}

	assertAppendSectorCalled := func(call appendSectorCall, contractID types.FileContractID, prices proto.HostPrices, expectedRoots []types.Hash256) {
		t.Helper()

		if call.contractID != contractID {
			t.Fatalf("expected contract ID %v, got %v", contractID, call.contractID)
		} else if call.hostPrices != prices {
			t.Fatalf("expected host prices %v, got %v", prices, call.hostPrices)
		} else if len(call.sectors) != len(expectedRoots) {
			t.Fatalf("expected %v sectors, got %v", len(expectedRoots), len(call.sectors))
		}
		for _, root := range expectedRoots {
			if !slices.Contains(call.sectors, root) {
				t.Fatalf("expected sector %v to be pinned, got %v", root, call.sectors)
			}
		}
	}

	// pin sectors on h1
	h1Prices := h1.Settings.Prices
	err := cm.performSectorPinningOnHost(context.Background(), h1, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h1Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one call, got %v", len(h1Mock.appendSectorCalls))
	}

	// assert sector pinning on h1
	// both contracts should be attempted, but only fcid2 should have sectors pinned
	assertAppendSectorCalled(h1Mock.appendSectorCalls[0], fcid2, h1Prices, roots[:6]) // called with all stored roots
	h1Pinned := append(slices.Clone(roots[:3]), roots[4:7]...)
	assertSectorsContract(hk1, h1Pinned, &fcid2) // all but the missing root pinned to fcid2
	assertSectorsContract(hk1, roots[3:4], nil)  // missing root remains unpinned

	// pin sectors on h2
	h2Prices := h2.Settings.Prices
	err = cm.performSectorPinningOnHost(context.Background(), h2, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h2Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one calls, got %v", len(h2Mock.appendSectorCalls))
	}
	// all h2 sectors pinned to fcid3
	assertAppendSectorCalled(h2Mock.appendSectorCalls[0], fcid3, h2Prices, roots[6:8])
	assertSectorsContract(hk2, roots[6:8], &fcid3)
}

func TestPerformSectorPinningOnHostOverflow(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newStoreMock()

	// prepare a host with two contracts
	hk1 := types.PublicKey{1}
	h1 := hosts.Host{
		PublicKey: hk1,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host1.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	h1.Settings.Prices.StoragePrice = types.NewCurrency64(123)
	h1.Settings.Prices.TipHeight = 111
	store.hosts[hk1] = h1

	// add two contracts for h1
	fcid1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk1, true, types.FileContractID{2})

	// make fcid2 the better contract for pinning
	for i, c := range store.contracts {
		switch c.ID {
		case fcid1:
			store.contracts[i].Capacity = 100
		case fcid2:
			store.contracts[i].Capacity = 200
		}
	}

	// prepare roots
	roots := make([]types.Hash256, 8)
	for i := range roots {
		roots[i] = frand.Entropy256()
		store.sectors[hk1] = append(store.sectors[hk1], sector{root: roots[i]})
	}

	// prepare dialer
	h1Mock := newHostClientMock()
	h1Mock.missingSectors[roots[3]] = struct{}{}
	h1Mock.maxPinnedPerAppend = 4 // only allow 4 sectors to be pinned per call

	dialer := newDialerMock()
	dialer.clients[hk1] = h1Mock

	// prepare hm
	hm := newHostManagerMock(store)
	hm.settings[hk1] = h1.Settings

	// prepare contract manager
	cm := newContractManager(types.PublicKey{}, nil, nil, store, dialer, hm, nil, nil)
	assertSectorsContract := func(hostKey types.PublicKey, roots []types.Hash256, contractID *types.FileContractID) {
		t.Helper()

		sectors, ok := store.sectors[hostKey]
		if !ok {
			t.Fatalf("expected sectors for host %v", hostKey)
		}
		for _, sector := range sectors {
			if !slices.Contains(roots, sector.root) || sector.contractID == nil && contractID == nil {
				// sector not part of the check, or both unpinned
				continue
			} else if contractID == nil && sector.contractID != nil {
				t.Fatalf("expected sector %v to be unpinned, got %v", sector.root, *sector.contractID)
			} else if contractID != nil && sector.contractID == nil {
				t.Fatalf("expected sector %v to be pinned to contract %v, got unpinned", sector.root, *contractID)
			} else if *sector.contractID != *contractID {
				t.Fatalf("expected sector %v to be pinned to contract %v, got %v", sector.root, *contractID, *sector.contractID)
			}
		}
	}

	assertAppendSectorCalled := func(call appendSectorCall, contractID types.FileContractID, prices proto.HostPrices, expectedRoots []types.Hash256) {
		t.Helper()

		if call.contractID != contractID {
			t.Fatalf("expected contract ID %v, got %v", contractID, call.contractID)
		} else if call.hostPrices != prices {
			t.Fatalf("expected host prices %v, got %v", prices, call.hostPrices)
		} else if len(call.sectors) != len(expectedRoots) {
			t.Fatalf("expected %v sectors, got %v", len(expectedRoots), len(call.sectors))
		}
		for _, root := range expectedRoots {
			if !slices.Contains(call.sectors, root) {
				t.Fatalf("expected sector %v to be pinned, got %v", root, call.sectors)
			}
		}
	}

	// pin sectors on h1
	h1Prices := h1.Settings.Prices
	err := cm.performSectorPinningOnHost(context.Background(), h1, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h1Mock.appendSectorCalls) != 2 {
		t.Fatalf("expected two calls, got %v", len(h1Mock.appendSectorCalls))
	}

	// assert sector pinning on h1
	// both contracts should be attempted, but only fcid2 should have sectors pinned
	assertAppendSectorCalled(h1Mock.appendSectorCalls[0], fcid2, h1Prices, roots)     // called with all stored roots
	assertAppendSectorCalled(h1Mock.appendSectorCalls[1], fcid1, h1Prices, roots[4:]) // called again with remaining unpinned roots

	// the missing sector should not be pinned
	assertSectorsContract(hk1, slices.Clone(roots[:3]), &fcid2) // the first half minus the missing root pinned to fcid2
	assertSectorsContract(hk1, roots[4:], &fcid1)               // the second half pinned to fcid1
	assertSectorsContract(hk1, roots[3:4], nil)                 // missing root remains unpinned
}
