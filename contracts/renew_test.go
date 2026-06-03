package contracts_test

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestPerformContractRenewals(t *testing.T) {
	amMock := newAccountsManagerMock()
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}

	const (
		period      = 50
		renewWindow = 10
	)

	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

	// prepare hosts

	// first one is good with a good contract and a bad one
	good := goodHost(1)
	store.addTestHost(t, good)
	hmMock.settings[good.PublicKey] = goodSettings

	// second one is bad since it's not accepting contracts with a good contract
	badSettings := hosts.Host{}.Settings // zero value
	bad := goodHost(2)
	bad.Settings = badSettings
	bad.Usability = hosts.Usability{} // mark as not usable
	store.addTestHost(t, bad)
	hmMock.settings[bad.PublicKey] = badSettings

	// add contracts
	blockHeight := cmMock.TipState().Index.Height
	fcid1 := store.addTestContract(t, good.PublicKey, true, types.FileContractID{1})  // will renew
	fcid2 := store.addTestContract(t, good.PublicKey, false, types.FileContractID{2}) // won't renew
	fcid3 := store.addTestContract(t, bad.PublicKey, true, types.FileContractID{3})   // won't renew

	// update contracts with proof height within renew window
	store.setContractProofHeight(t, fcid1, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid1, 9999)
	store.setContractProofHeight(t, fcid2, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid2, 9999)
	store.setContractProofHeight(t, fcid3, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid3, 9999)

	mock := newClientMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	rev := contracts.NewRevisionManager(mock, cmMock, store, 1, zaptest.NewLogger(t))
	contractsMgr := contracts.NewTestContractManager(renterKey, amMock, nil, cmMock, store, mock, nil, rev, contracts.NewContractLocker(), hmMock, syncerMock, wallet)

	assertRenewal := func(renewedFrom types.FileContractID, proofHeight uint64, call renewContractCall) {
		t.Helper()
		if call.params.Contract.ID != renewedFrom {
			t.Fatalf("expected renewedFrom %v, got %v", renewedFrom, call.params.Contract.ID)
		} else if call.params.ProofHeight != proofHeight {
			t.Fatalf("expected proof height %v, got %v", proofHeight, call.params.ProofHeight)
		}
	}

	// perform renewals when no contract is ready for it
	if err := contractsMgr.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(mock.host(good.PublicKey).renewCalls) != 0 {
		t.Fatal("expected good host to not be dialed")
	} else if len(mock.host(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}

	cmMock.mu.Lock()
	cmMock.state.Index.Height++
	blockHeight = cmMock.state.Index.Height
	cmMock.mu.Unlock()

	if err := contractsMgr.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(mock.host(good.PublicKey).renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(mock.host(good.PublicKey).renewCalls))
	} else if len(mock.host(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
	assertRenewal(types.FileContractID{1}, blockHeight+period, mock.host(good.PublicKey).renewCalls[0])

	// assert renewal made it into the store
	allContracts, err := store.Contracts(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(allContracts) != 4 {
		t.Fatalf("expected 4 contracts, got %v", len(allContracts))
	}
	for _, c := range allContracts {
		switch c.ID {
		case types.FileContractID{1}:
			if c.RenewedTo == (types.FileContractID{}) {
				t.Fatal("contract should be renewed")
			}
		case types.FileContractID{2}, types.FileContractID{3}:
			if c.RenewedTo != (types.FileContractID{}) {
				t.Fatal("contract shouldn't be renewed")
			}
		default:
			if c.RenewedFrom != (types.FileContractID{1}) {
				t.Fatal("renewed contract should be renewed from first contract")
			} else if c.ProofHeight != blockHeight+period {
				t.Fatalf("renewed contract should have proof height %d, got %d", blockHeight+period, c.ProofHeight)
			} else if c.ExpirationHeight != c.ProofHeight+144 {
				t.Fatalf("renewed contract should have expiration height %d, got %d", c.ProofHeight+144, c.ExpirationHeight)
			} else if !c.ContractPrice.Equals(types.Siacoins(1)) {
				t.Fatalf("renewed contract should have contract price %v, got %v", types.Siacoins(1), c.ContractPrice)
			}
		}
	}

	// assert consecutive calls don't keep renewing the same contract
	if err := contractsMgr.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(mock.host(good.PublicKey).renewCalls) != 1 {
		t.Fatalf("expected one renewal, got %v", len(mock.host(good.PublicKey).renewCalls))
	} else if len(mock.host(bad.PublicKey).renewCalls) != 0 {
		t.Fatal("expected bad host to not be dialed")
	}
}

func TestPerformContractRenewalsEmptyContracts(t *testing.T) {
	amMock := newAccountsManagerMock()
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}

	const (
		period      = 50
		renewWindow = 10
	)

	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

	// four good hosts. hostA and hostB each have multiple empty contracts that
	// are all within their renew window; exactly one per host should be renewed
	// to keep a single empty contract per host - the rest are left to expire so
	// we don't lock up collateral on multiple empty contracts with the same host.
	// hostC additionally has an empty contract that is too early to renew, which
	// is already the host's retained empty contract, so none of hostC's empty
	// contracts in their renew window should be renewed.
	hostA := goodHost(1)
	store.addTestHost(t, hostA)
	hmMock.settings[hostA.PublicKey] = goodSettings
	hostB := goodHost(2)
	store.addTestHost(t, hostB)
	hmMock.settings[hostB.PublicKey] = goodSettings
	hostC := goodHost(3)
	store.addTestHost(t, hostC)
	hmMock.settings[hostC.PublicKey] = goodSettings
	hostD := goodHost(4)
	store.addTestHost(t, hostD)
	hmMock.settings[hostD.PublicKey] = goodSettings

	blockHeight := cmMock.TipState().Index.Height

	// add empty (capacity 0) contracts that are all within their renew window:
	// two with each of hostA, hostB and hostC.
	emptyContracts := map[types.PublicKey][]types.FileContractID{
		hostA.PublicKey: {{1}, {2}},
		hostB.PublicKey: {{3}, {4}},
		hostC.PublicKey: {{5}, {6}},
	}
	for hk, fcids := range emptyContracts {
		for _, fcid := range fcids {
			store.addTestContract(t, hk, true, fcid)
			store.setContractSize(t, fcid, 0)     // zero the stored size
			store.setRevisionCapacity(t, fcid, 0) // and the paid-for capacity, making the contract empty
			store.setContractProofHeight(t, fcid, blockHeight+renewWindow+1)
			store.setContractExpirationHeight(t, fcid, 9999)
		}
	}

	// hostC also has an empty contract that is too early to renew. it remains
	// active as hostC's retained empty contract, so none of hostC's empty
	// contracts in their renew window should be renewed.
	retainedFCID := types.FileContractID{7}
	store.addTestContract(t, hostC.PublicKey, true, retainedFCID)
	store.setContractSize(t, retainedFCID, 0)
	store.setRevisionCapacity(t, retainedFCID, 0)
	store.setContractProofHeight(t, retainedFCID, blockHeight+renewWindow+100)
	store.setContractExpirationHeight(t, retainedFCID, 9999)

	// hostD reproduces a "pruned" contract: all of its sectors were freed, so its
	// size is 0 but its already-paid-for capacity is still non-zero. renewing
	// such a contract produces an empty, capacity-0 contract (the renewal sets
	// the new capacity to the old size), so it must be treated as empty when
	// limiting renewals to one empty contract per host. hostD also has a fully
	// empty contract; both are in their renew window, but only one should be renewed.
	prunedFCID := types.FileContractID{8}
	store.addTestContract(t, hostD.PublicKey, true, prunedFCID)
	store.setContractSize(t, prunedFCID, 0) // size 0, capacity left non-zero
	store.setContractProofHeight(t, prunedFCID, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, prunedFCID, 9999)
	emptyDFCID := types.FileContractID{9}
	store.addTestContract(t, hostD.PublicKey, true, emptyDFCID)
	store.setContractSize(t, emptyDFCID, 0)
	store.setRevisionCapacity(t, emptyDFCID, 0)
	store.setContractProofHeight(t, emptyDFCID, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, emptyDFCID, 9999)

	mock := newClientMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	rev := contracts.NewRevisionManager(mock, cmMock, store, 1, zaptest.NewLogger(t))
	contractsMgr := contracts.NewTestContractManager(renterKey, amMock, nil, cmMock, store, mock, nil, rev, contracts.NewContractLocker(), hmMock, syncerMock, wallet)

	cmMock.mu.Lock()
	cmMock.state.Index.Height++
	cmMock.mu.Unlock()

	// exactly one empty contract per host should be renewed for hostA and hostB,
	// while hostC already has a retained empty contract so none should be renewed.
	if err := contractsMgr.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if got := len(mock.host(hostA.PublicKey).renewCalls); got != 1 {
		t.Fatalf("expected exactly one empty contract to be renewed for hostA, got %v", got)
	} else if got := len(mock.host(hostB.PublicKey).renewCalls); got != 1 {
		t.Fatalf("expected exactly one empty contract to be renewed for hostB, got %v", got)
	} else if got := len(mock.host(hostC.PublicKey).renewCalls); got != 0 {
		t.Fatalf("expected no empty contract to be renewed for hostC, got %v", got)
	} else if got := len(mock.host(hostD.PublicKey).renewCalls); got != 1 {
		t.Fatalf("expected exactly one empty contract to be renewed for hostD, got %v", got)
	}

	// a subsequent run shouldn't renew another empty contract for any host,
	// since hostA and hostB now have a freshly renewed (still empty) contract
	// retained for uploads and hostC still has its original retained empty contract.
	if err := contractsMgr.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if got := len(mock.host(hostA.PublicKey).renewCalls); got != 1 {
		t.Fatalf("expected no further empty renewals for hostA, got %v total", got)
	} else if got := len(mock.host(hostB.PublicKey).renewCalls); got != 1 {
		t.Fatalf("expected no further empty renewals for hostB, got %v total", got)
	} else if got := len(mock.host(hostC.PublicKey).renewCalls); got != 0 {
		t.Fatalf("expected no further empty renewals for hostC, got %v total", got)
	} else if got := len(mock.host(hostD.PublicKey).renewCalls); got != 1 {
		t.Fatalf("expected no further empty renewals for hostD, got %v total", got)
	}
}

func TestRenewalAllowance(t *testing.T) {
	amMock := newAccountsManagerMock()
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}

	const (
		period      = 50
		renewWindow = 10
	)

	store := newTestStore(t)
	hmMock := newHostManagerMock(store)

	// prepare hosts
	good := goodHost(1)
	store.addTestHost(t, good)
	hmMock.settings[good.PublicKey] = goodSettings

	blockHeight := cmMock.TipState().Index.Height

	// add contracts
	fcid1 := store.addTestContract(t, good.PublicKey, true, types.FileContractID{1})  // will renew
	fcid2 := store.addTestContract(t, good.PublicKey, false, types.FileContractID{2}) // won't renew

	// update contracts with proof height within renew window
	store.setContractProofHeight(t, fcid1, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid1, 9999)
	store.setContractProofHeight(t, fcid2, blockHeight+renewWindow+1)
	store.setContractExpirationHeight(t, fcid2, 9999)

	mock := newClientMock()
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	rev := contracts.NewRevisionManager(mock, cmMock, store, 1, zaptest.NewLogger(t))
	cm := contracts.NewTestContractManager(renterKey, amMock, nil, cmMock, store, mock, nil, rev, contracts.NewContractLocker(), hmMock, syncerMock, wallet)

	assertRenewal := func(allowance types.Currency, call renewContractCall) {
		t.Helper()
		if call.params.Allowance != allowance {
			t.Fatalf("expected allowance %v, got %v", allowance, call.params.Allowance)
		}
	}

	cmMock.mu.Lock()
	cmMock.state.Index.Height++
	cmMock.mu.Unlock()

	store.setActiveAccountsCount(t, 1000)
	if err := cm.PerformContractRenewals(context.Background(), period, renewWindow, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	allowance, err := cm.ContractFundTarget(context.Background(), good, contracts.MinAllowance)
	if err != nil {
		t.Fatal(err)
	}
	// allowance is doubled to allow for two account funding cycles before next refresh
	assertRenewal(allowance.Mul64(2), mock.host(good.PublicKey).renewCalls[0])
}
