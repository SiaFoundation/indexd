package contracts_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
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

func TestPerformSectorPinningOnHost(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

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

	// contracts with greater capacity are preferred
	store.setContractCapacity(t, fcid1, 100)
	store.setContractCapacity(t, fcid2, 200)

	// set remaining allowance so contracts are eligible for pinning
	store.setContractRemainingAllowance(t, fcid1, types.Siacoins(1))
	store.setContractRemainingAllowance(t, fcid2, types.Siacoins(1))

	// add one contract for h2
	fcid3 := store.addTestContract(t, hk2, true, types.FileContractID{3})
	store.setContractRemainingAllowance(t, fcid3, types.Siacoins(1))

	// prepare roots
	roots := make([]types.Hash256, 8)
	for i := range roots {
		roots[i] = frand.Entropy256()
	}

	// prepare sectors for h1
	store.addUnpinnedSectors(t, hk1, roots[:6])

	// prepare sectors for h2
	store.addUnpinnedSectors(t, hk2, roots[6:8])

	// prepare dialer
	h1Mock := newHostClientMock(hk1)
	h2Mock := newHostClientMock(hk2)

	dialer := newDialerMock()
	dialer.clients[hk1] = h1Mock
	dialer.clients[hk2] = h2Mock

	// indicate that root 4 is missing
	dialer.clients[hk1].missingSectors[roots[3]] = struct{}{}

	// prepare contract manager
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, store, dialer, hmMock, nil, nil)

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
	err := cm.PerformSectorPinningOnHost(context.Background(), h1, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h1Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one call, got %v", len(h1Mock.appendSectorCalls))
	}

	// assert sector pinning on h1
	// both contracts should be attempted, but only fcid2 should have sectors pinned
	assertAppendSectorCalled(h1Mock.appendSectorCalls[0], fcid2, h1Prices, roots[:6]) // called with all stored roots
	h1Pinned := append(slices.Clone(roots[:3]), roots[4:6]...)
	assertSectorsContract(h1Pinned, &fcid2) // all but the missing root pinned to fcid2
	assertSectorsContract(roots[3:4], nil)  // missing root remains unpinned

	// pin sectors on h2
	h2Prices := h2.Settings.Prices
	err = cm.PerformSectorPinningOnHost(context.Background(), h2, log.Named("pin"))
	if err != nil {
		t.Fatal(err)
	} else if len(h2Mock.appendSectorCalls) != 1 {
		t.Fatalf("expected one calls, got %v", len(h2Mock.appendSectorCalls))
	}
	// all h2 sectors pinned to fcid3
	assertAppendSectorCalled(h2Mock.appendSectorCalls[0], fcid3, h2Prices, roots[6:8])
	assertSectorsContract(roots[6:8], &fcid3)
}

func TestPerformSectorPinningOnHostOverflow(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

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

	// make fcid2 the better contract for pinning
	store.setContractCapacity(t, fcid1, 100)
	store.setContractCapacity(t, fcid2, 200)

	// set remaining allowance so contracts are eligible for pinning
	store.setContractRemainingAllowance(t, fcid1, types.Siacoins(1))
	store.setContractRemainingAllowance(t, fcid2, types.Siacoins(1))

	// prepare roots
	roots := make([]types.Hash256, 8)
	for i := range roots {
		roots[i] = frand.Entropy256()
	}
	store.addUnpinnedSectors(t, hk1, roots)

	// prepare dialer
	h1Mock := newHostClientMock(hk1)
	h1Mock.missingSectors[roots[3]] = struct{}{}
	h1Mock.maxPinnedPerAppend = 4 // only allow 4 sectors to be pinned per call

	dialer := newDialerMock()
	dialer.clients[hk1] = h1Mock

	// prepare contract manager
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, store, dialer, hmMock, nil, nil)

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
	err := cm.PerformSectorPinningOnHost(context.Background(), h1, log.Named("pin"))
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
	assertSectorsContract(slices.Clone(roots[:3]), &fcid2) // the first half minus the missing root pinned to fcid2
	assertSectorsContract(roots[4:], &fcid1)               // the second half pinned to fcid1
	assertSectorsContract(roots[3:4], nil)                 // missing root remains unpinned
}
