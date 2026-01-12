package contracts_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type sectorRootsCall struct {
	hostPrices proto.HostPrices
	contractID types.FileContractID
	offset     uint64
	length     uint64
}

type freeSectorsCall struct {
	hostPrices proto.HostPrices
	contractID types.FileContractID
	indices    []uint64
}

func (c *hostClientMock) SectorRoots(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	if c.failsRPCs {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("mocked error")
	}

	c.sectorRootsCalls = append(c.sectorRootsCalls, sectorRootsCall{
		hostPrices: hostPrices,
		contractID: contractID,
		offset:     offset,
		length:     length,
	})
	roots, ok := c.sectorRoots[contractID]
	if !ok || offset > uint64(len(roots)) {
		return rhp.RPCSectorRootsResult{}, nil
	}
	roots = roots[offset:]
	if length > uint64(len(roots)) {
		return rhp.RPCSectorRootsResult{}, errors.New("out of bounds")
	}
	return rhp.RPCSectorRootsResult{Roots: roots[:length]}, nil
}

func (c *hostClientMock) FreeSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	if c.failsRPCs {
		return rhp.RPCFreeSectorsResult{}, fmt.Errorf("mocked error")
	}

	c.freeSectorsCalls = append(c.freeSectorsCalls, freeSectorsCall{
		hostPrices: hostPrices,
		contractID: contractID,
		indices:    indices,
	})
	return rhp.RPCFreeSectorsResult{}, nil
}

func TestPerformContractPruningOnHost(t *testing.T) {
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

	// h1 is good
	hk1 := types.PublicKey{1}
	h1 := hosts.Host{
		PublicKey: hk1,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host1.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	h1.Settings.Prices.StoragePrice = types.NewCurrency64(123)
	h1.Settings.Prices.TipHeight = 111
	store.addTestHost(t, h1)
	hmMock.settings[hk1] = h1.Settings

	// h2 is good
	hk2 := types.PublicKey{2}
	h2 := hosts.Host{
		PublicKey: hk2,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host2.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	h2.Settings.Prices.StoragePrice = types.NewCurrency64(456)
	h2.Settings.Prices.TipHeight = 222
	store.addTestHost(t, h2)
	hmMock.settings[hk2] = h2.Settings

	// h3 is bad
	badSettings := goodSettings
	badSettings.AcceptingContracts = false
	hk3 := types.PublicKey{3}
	h3 := hosts.Host{
		PublicKey: hk3,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host3.com"}},
		Settings:  badSettings,
		Usability: hosts.Usability{}, // not usable
	}
	store.addTestHost(t, h3)
	hmMock.settings[hk3] = h3.Settings

	// h4 is good, but upon rescan it's bad
	hk4 := types.PublicKey{4}
	h4 := hosts.Host{
		PublicKey: hk4,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host4.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	store.addTestHost(t, h4)
	hmMock.settings[hk4] = badSettings // bad settings on rescan

	// h5 is good
	hk5 := types.PublicKey{5}
	h5 := hosts.Host{
		PublicKey: hk5,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host5.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	store.addTestHost(t, h5)
	hmMock.settings[hk5] = h5.Settings

	// add two contracts for h1, one for h2 and one for h5 to give it all
	// chances of succeeding, but it won't
	fcid1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk1, true, types.FileContractID{2})
	fcid3 := store.addTestContract(t, hk2, true, types.FileContractID{3})
	fcid4 := store.addTestContract(t, hk5, true, types.FileContractID{4})

	// set remaining allowance for all contracts
	store.setContractRemainingAllowance(t, fcid1, types.Siacoins(1))
	store.setContractRemainingAllowance(t, fcid2, types.Siacoins(1))
	store.setContractRemainingAllowance(t, fcid3, types.Siacoins(1))
	store.setContractRemainingAllowance(t, fcid4, types.Siacoins(1))

	// prepare roots
	r1 := types.Hash256{1}
	r2 := types.Hash256{2}
	r3 := types.Hash256{3}
	r4 := types.Hash256{4}
	r5 := types.Hash256{5}
	r6 := types.Hash256{6}
	r7 := types.Hash256{7}
	r8 := types.Hash256{8}
	r9 := types.Hash256{9}
	r10 := types.Hash256{10}

	// pin sectors to contracts in the database
	// r1, r2 pinned to fcid1 (host has r1, r2, r3 - so r3 is prunable)
	store.addPinnedSectors(t, hk1, fcid1, []types.Hash256{r1, r2})
	// r4, r7, r8 pinned to fcid2 (host has r4, r5, r6, r7, r8 - so r5, r6 are prunable)
	store.addPinnedSectors(t, hk1, fcid2, []types.Hash256{r4, r7, r8})
	// r9 pinned to fcid3 (host has r9 - nothing prunable)
	store.addPinnedSectors(t, hk2, fcid3, []types.Hash256{r9})
	// r10 pinned to fcid4 (host has r10, but RPCs fail)
	store.addPinnedSectors(t, hk5, fcid4, []types.Hash256{r10})

	// prepare dialer
	h1Mock := newHostClientMock(hk1)
	h2Mock := newHostClientMock(hk2)
	h4Mock := newHostClientMock(hk4)
	h5Mock := newHostClientMock(hk5)
	h5Mock.failsRPCs = true

	dialer := newDialerMock()
	dialer.clients[hk1] = h1Mock
	dialer.clients[hk2] = h2Mock
	dialer.clients[hk4] = h4Mock
	dialer.clients[hk5] = h5Mock

	// prepare roots
	h1Mock.sectorRoots[fcid1] = []types.Hash256{r1, r2, r3}
	h1Mock.sectorRoots[fcid2] = []types.Hash256{r4, r5, r6, r7, r8}
	h2Mock.sectorRoots[fcid3] = []types.Hash256{r9}
	h5Mock.sectorRoots[fcid4] = []types.Hash256{r10}

	// set contract sizes and revision filesizes
	store.setContractSize(t, fcid1, proto.SectorSize*uint64(len(h1Mock.sectorRoots[fcid1])))
	store.setRevisionFilesize(t, fcid1, proto.SectorSize*uint64(len(h1Mock.sectorRoots[fcid1])))
	store.setContractSize(t, fcid2, proto.SectorSize*uint64(len(h1Mock.sectorRoots[fcid2])))
	store.setRevisionFilesize(t, fcid2, proto.SectorSize*uint64(len(h1Mock.sectorRoots[fcid2])))
	store.setContractSize(t, fcid3, proto.SectorSize*uint64(len(h2Mock.sectorRoots[fcid3])))
	store.setRevisionFilesize(t, fcid3, proto.SectorSize*uint64(len(h2Mock.sectorRoots[fcid3])))
	store.setContractSize(t, fcid4, proto.SectorSize*uint64(len(h5Mock.sectorRoots[fcid4])))
	store.setRevisionFilesize(t, fcid4, proto.SectorSize*uint64(len(h5Mock.sectorRoots[fcid4])))

	// schedule contracts for pruning
	store.scheduleContractsForPruningHelper(t)

	// prepare contract manager
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, store, dialer, hmMock, nil, nil)

	// prune contracts on h1
	err := cm.PerformContractPruningOnHost(context.Background(), h1, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to perform contract pruning: %v", err)
	}

	// assert rpc calls
	if len(h1Mock.sectorRootsCalls) != 2 {
		t.Fatalf("expected 2 sector roots calls, got %d", len(h1Mock.sectorRootsCalls))
	} else if call := h1Mock.sectorRootsCalls[0]; call.contractID != fcid2 {
		t.Fatalf("expected contract ID %v, got %v", fcid2, call.contractID)
	} else if call.offset != 0 || call.length != 5 {
		t.Fatalf("expected offset 0 and length 5, got offset %d and length %d", call.offset, call.length)
	} else if call = h1Mock.sectorRootsCalls[1]; call.contractID != fcid1 {
		t.Fatalf("expected contract ID %v, got %v", fcid1, call.contractID)
	} else if call.offset != 0 || call.length != 3 {
		t.Fatalf("expected offset 0 and length 3, got offset %d and length %d", call.offset, call.length)
	} else if len(h1Mock.freeSectorsCalls) != 2 {
		t.Fatalf("expected 2 free sectors calls, got %d", len(h1Mock.freeSectorsCalls))
	} else if call := h1Mock.freeSectorsCalls[0]; call.contractID != fcid2 {
		t.Fatalf("expected contract ID %v, got %v", fcid2, call.contractID)
	} else if len(call.indices) != 2 {
		t.Fatalf("expected 2 indices, got %d", len(call.indices))
	} else if call.indices[0] != 1 || call.indices[1] != 2 {
		t.Fatalf("expected indices [1, 2], got %v", call.indices)
	} else if call = h1Mock.freeSectorsCalls[1]; call.contractID != fcid1 {
		t.Fatalf("expected contract ID %v, got %v", fcid1, call.contractID)
	} else if len(call.indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(call.indices))
	} else if call.indices[0] != 2 {
		t.Fatalf("expected index 2, got %v", call.indices)
	}

	// prune contracts on h2
	err = cm.PerformContractPruningOnHost(context.Background(), h2, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to perform contract pruning: %v", err)
	}

	// assert rpc calls
	if len(h2Mock.sectorRootsCalls) != 1 {
		t.Fatalf("expected 1 sector roots calls, got %d", len(h2Mock.sectorRootsCalls))
	} else if call := h2Mock.sectorRootsCalls[0]; call.contractID != fcid3 {
		t.Fatalf("expected contract ID %v, got %v", fcid2, call.contractID)
	} else if call.offset != 0 || call.length != 1 {
		t.Fatalf("expected offset 0 and length 1, got offset %d and length %d", call.offset, call.length)
	} else if len(h2Mock.freeSectorsCalls) != 0 {
		t.Fatalf("expected 0 free sectors calls, got %d", len(h2Mock.freeSectorsCalls))
	}

	// assert contracts are marked as pruned
	if contracts, err := store.ContractsForPruning(hk1); err != nil {
		t.Fatalf("failed to fetch contracts for pruning: %v", err)
	} else if len(contracts) != 0 {
		t.Fatalf("expected no contracts for pruning, got %v", contracts)
	} else if contracts, err := store.ContractsForPruning(hk2); err != nil {
		t.Fatalf("failed to fetch contracts for pruning: %v", err)
	} else if len(contracts) != 0 {
		t.Fatalf("expected no contracts for pruning, got %v", contracts)
	}

	performPruning := func(hostKey types.PublicKey) error {
		return hmMock.WithScannedHost(context.Background(), hostKey, func(h hosts.Host) error {
			return cm.PerformContractPruningOnHost(context.Background(), h, zap.NewNop())
		})
	}

	// prune contracts on h3
	err = performPruning(hk3)
	if !errors.Is(err, hosts.ErrBadHost) {
		t.Fatal("unexpected", err)
	}

	// prune contracts on h4
	err = performPruning(hk4)
	if !errors.Is(err, hosts.ErrBadHost) {
		t.Fatal("unexpected", err)
	}

	// prune contracts on h5
	err = performPruning(hk5)
	if err != nil {
		t.Fatalf("failed to perform contract pruning: %v", err)
	}

	allContracts, err := store.Contracts(0, 10)
	if err != nil {
		t.Fatalf("failed to fetch contracts: %v", err)
	} else if len(allContracts) != 4 {
		t.Fatalf("expected 4 contracts, got %d", len(allContracts))
	}

	success := time.Now().Add(contracts.PruneIntervalSuccess)
	failure := time.Now().Add(contracts.PruneIntervalFailure)
	for _, c := range allContracts {
		switch c.ID {
		case fcid1, fcid2, fcid3:
			if !(c.NextPrune.After(success.Add(-time.Minute)) && c.NextPrune.Before(success.Add(time.Minute))) {
				t.Fatal("expected next prune to be scheduled 24h from now", c.ID, success, c.NextPrune)
			}
		case fcid4:
			if !(c.NextPrune.After(failure.Add(-time.Minute)) && c.NextPrune.Before(failure.Add(time.Minute))) {
				t.Fatal("expected next prune to be scheduled 3h from now", c.ID, failure, c.NextPrune)
			}
		default:
			t.Fatal("unexpected contract ID", c.ID)
		}
	}
}
