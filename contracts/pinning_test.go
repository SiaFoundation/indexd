package contracts_test

import (
	"context"
	"slices"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestPerformSectorPinningOnHost(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cmMock := newChainManagerMock()

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
	store.addTestHost(t, h1)
	hmMock.settings[hk1] = h1.Settings

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

	// add two contracts for h1
	fcid1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk1, true, types.FileContractID{2})

	// set remaining allowance and collateral in raw_revision (must be before
	// setRevisionCapacity since UpdateContractRevision overwrites capacity)
	store.setRevisionRemainingAllowance(t, fcid1, types.Siacoins(1))
	store.setRevisionRemainingAllowance(t, fcid2, types.Siacoins(1))
	store.setRevisionRemainingCollateral(t, fcid1, types.Siacoins(1))
	store.setRevisionRemainingCollateral(t, fcid2, types.Siacoins(1))

	// contracts with greater capacity are preferred (setRevisionCapacity must
	// be last since it does a direct SQL update that won't be overwritten)
	store.setRevisionCapacity(t, fcid1, 100)
	store.setRevisionCapacity(t, fcid2, 200)

	// add one contract for h2
	fcid3 := store.addTestContract(t, hk2, true, types.FileContractID{3})
	store.setRevisionRemainingAllowance(t, fcid3, types.Siacoins(1))
	store.setRevisionRemainingCollateral(t, fcid3, types.Siacoins(1))

	// prepare roots
	roots := make([]types.Hash256, 8)
	for i := range roots {
		roots[i] = frand.Entropy256()
	}

	// prepare sectors for h1
	store.addUnpinnedSectors(t, hk1, roots[:6])

	// prepare sectors for h2
	store.addUnpinnedSectors(t, hk2, roots[6:8])

	// prepare client mock
	mock := newClientMock()
	h1Mock := mock.host(hk1)
	h2Mock := mock.host(hk2)
	h1Mock.prices = h1.Settings.Prices
	h2Mock.prices = h2.Settings.Prices

	// indicate that root 4 is missing
	h1Mock.missingSectors[roots[3]] = struct{}{}

	// prepare contract manager
	rev := contracts.NewRevisionManager(mock, cmMock, store, 1, zaptest.NewLogger(t))
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, cmMock, store, mock, nil, rev, contracts.NewContractLocker(), hmMock, nil, nil)

	assertSectorsContract := func(roots []types.Hash256, contractID *types.FileContractID) {
		t.Helper()

		for _, root := range roots {
			pinned := store.getSectorContractID(t, root)
			if contractID == nil && pinned != nil {
				t.Fatalf("expected sector %v to be unpinned, got %v", root, *pinned)
			} else if contractID != nil && pinned == nil {
				t.Fatalf("expected sector %v to be pinned to contract %v, got unpinned", root, *contractID)
			} else if contractID != nil && pinned != nil && *pinned != *contractID {
				t.Fatalf("expected sector %v to be pinned to contract %v, got %v", root, *contractID, *pinned)
			}
		}
	}

	assertAppendSectorCalled := func(call appendSectorCall, contractID types.FileContractID, expectedRoots []types.Hash256) {
		t.Helper()

		if call.contractID != contractID {
			t.Fatalf("expected contract ID %v, got %v", contractID, call.contractID)
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
	err := cm.PerformSectorPinningOnHost(context.Background(), h1, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h1Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one call, got %v", len(h1Mock.appendSectorCalls))
	}

	// assert sector pinning on h1
	// both contracts should be attempted, but only fcid2 should have sectors pinned
	assertAppendSectorCalled(h1Mock.appendSectorCalls[0], fcid2, roots[:6]) // called with all stored roots
	h1Pinned := append(slices.Clone(roots[:3]), roots[4:6]...)
	assertSectorsContract(h1Pinned, &fcid2) // all but the missing root pinned to fcid2
	assertSectorsContract(roots[3:4], nil)  // missing root remains unpinned

	// pin sectors on h2
	err = cm.PerformSectorPinningOnHost(context.Background(), h2, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h2Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one calls, got %v", len(h2Mock.appendSectorCalls))
	}
	// all h2 sectors pinned to fcid3
	assertAppendSectorCalled(h2Mock.appendSectorCalls[0], fcid3, roots[6:8])
	assertSectorsContract(roots[6:8], &fcid3)
}

func TestPerformSectorPinningOnHostOverflow(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cmMock := newChainManagerMock()

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
	store.addTestHost(t, h1)
	hmMock.settings[hk1] = h1.Settings

	// add two contracts for h1
	fcid1 := store.addTestContract(t, hk1, true, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk1, true, types.FileContractID{2})

	// fcid2 gets tiny allowance/collateral (just above 0 for DB query filter)
	// so it can only use pre-allocated capacity (no funded sectors)
	store.setRevisionRemainingAllowance(t, fcid2, types.NewCurrency64(1))
	store.setRevisionRemainingCollateral(t, fcid2, types.NewCurrency64(1))

	// fcid1 gets enough allowance/collateral to fund additional sectors
	store.setRevisionRemainingAllowance(t, fcid1, types.Siacoins(1))
	store.setRevisionRemainingCollateral(t, fcid1, types.Siacoins(1))

	// fcid2 has higher capacity (preferred by ContractsForPinning) but
	// limited to exactly 4 pre-allocated sectors above filesize
	// (setRevisionCapacity must be after remaining allowance/collateral
	// updates since UpdateContractRevision overwrites the capacity column)
	store.setRevisionCapacity(t, fcid2, 100+4*proto.SectorSize)

	// prepare roots
	roots := make([]types.Hash256, 8)
	for i := range roots {
		roots[i] = frand.Entropy256()
	}
	store.addUnpinnedSectors(t, hk1, roots)

	// prepare client mock
	mock := newClientMock()
	h1Mock := mock.host(hk1)
	h1Mock.prices = h1.Settings.Prices
	h1Mock.missingSectors[roots[3]] = struct{}{}

	// prepare contract manager
	rev := contracts.NewRevisionManager(mock, cmMock, store, 1, zaptest.NewLogger(t))
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, cmMock, store, mock, nil, rev, contracts.NewContractLocker(), hmMock, nil, nil)

	assertSectorsContract := func(roots []types.Hash256, contractID *types.FileContractID) {
		t.Helper()

		for _, root := range roots {
			pinned := store.getSectorContractID(t, root)
			if contractID == nil && pinned != nil {
				t.Fatalf("expected sector %v to be unpinned, got %v", root, *pinned)
			} else if contractID != nil && pinned == nil {
				t.Fatalf("expected sector %v to be pinned to contract %v, got unpinned", root, *contractID)
			} else if contractID != nil && pinned != nil && *pinned != *contractID {
				t.Fatalf("expected sector %v to be pinned to contract %v, got %v", root, *contractID, *pinned)
			}
		}
	}

	assertAppendSectorCalled := func(call appendSectorCall, contractID types.FileContractID, expectedRoots []types.Hash256) {
		t.Helper()

		if call.contractID != contractID {
			t.Fatalf("expected contract ID %v, got %v", contractID, call.contractID)
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
	err := cm.PerformSectorPinningOnHost(context.Background(), h1, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h1Mock.appendSectorCalls) != 2 {
		t.Fatalf("expected two calls, got %v", len(h1Mock.appendSectorCalls))
	}

	// assert sector pinning on h1
	// both contracts should be attempted, but only fcid2 should have sectors pinned
	assertAppendSectorCalled(h1Mock.appendSectorCalls[0], fcid2, roots[:4]) // called with first 4 roots (capacity limited)
	assertAppendSectorCalled(h1Mock.appendSectorCalls[1], fcid1, roots[4:]) // called again with remaining unpinned roots

	// the missing sector should not be pinned
	assertSectorsContract(slices.Clone(roots[:3]), &fcid2) // the first half minus the missing root pinned to fcid2
	assertSectorsContract(roots[4:], &fcid1)               // the second half pinned to fcid1
	assertSectorsContract(roots[3:4], nil)                 // missing root remains unpinned
}
