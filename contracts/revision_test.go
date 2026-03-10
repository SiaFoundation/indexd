package contracts_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/contracts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type latestRevisionMock struct {
	fn func(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error)
}

func (m *latestRevisionMock) LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return m.fn(ctx, hostKey, contractID)
}

// persistErrorStore wraps a RevisionStore and injects a persist error for a
// specific contract ID, matching the old client_test.go pattern.
type persistErrorStore struct {
	testStore
	errorContractID types.FileContractID
}

func (s *persistErrorStore) UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error {
	if contract.ID == s.errorContractID {
		return errors.New("persist error")
	}
	return s.testStore.UpdateContractRevision(contract, usage)
}

func TestWithRevision(t *testing.T) {
	store := newTestStore(t)
	chain := newChainManagerMock()
	revisionSubmissionBuffer := uint64(10)
	hk := types.PublicKey{1}
	store.addTestHost(t, goodHost(1))

	latest := &latestRevisionMock{}
	rm := contracts.NewRevisionManager(latest, chain, store, revisionSubmissionBuffer, zaptest.NewLogger(t))

	addContract := func(fcid types.FileContractID) {
		t.Helper()
		if err := store.AddFormedContract(hk, fcid, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 1, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(10)}, HostOutput: types.SiacoinOutput{Value: types.Siacoins(110)}, MissedHostValue: types.Siacoins(5), TotalCollateral: types.Siacoins(100)}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
			t.Fatal(err)
		}
	}

	fcid1 := types.FileContractID{1}
	fcid2 := types.FileContractID{2}
	fcid3 := types.FileContractID{3}
	fcid4 := types.FileContractID{4}

	// renewed contract
	addContract(fcid1)
	if err := store.AddRenewedContract(fcid1, types.FileContractID{1, 1}, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 1}, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}

	// not revisable
	if err := store.AddFormedContract(hk, fcid2, types.V2FileContract{HostPublicKey: hk, ProofHeight: 100 + revisionSubmissionBuffer, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(10)}, HostOutput: types.SiacoinOutput{Value: types.Siacoins(110)}, MissedHostValue: types.Siacoins(5), TotalCollateral: types.Siacoins(100)}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}

	addContract(fcid3)
	addContract(types.FileContractID{12})

	noopFn := func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, nil
	}
	invalidSigFn := func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, proto.ErrInvalidSignature
	}

	// assert ErrContractRenewed is returned for a renewed contract
	lc := contracts.NewContractLocker()
	lock := func(fcid types.FileContractID) *contracts.LockedContract {
		lockedContract, unlock := lc.LockContract(fcid)
		unlock() // immediately unlock since we don't need the lock for this test
		return lockedContract
	}
	err := rm.WithRevision(context.Background(), lock(fcid1), noopFn)
	if !errors.Is(err, contracts.ErrContractRenewed) {
		t.Fatalf("expected ErrContractRenewed, got: %v", err)
	}

	// assert ErrContractNotRevisable is returned for a contract that can't be revised
	err = rm.WithRevision(context.Background(), lock(fcid2), noopFn)
	if !errors.Is(err, contracts.ErrContractNotRevisable) {
		t.Fatalf("expected ErrContractNotRevisable, got: %v", err)
	}

	// assert withRevision errors out if the revision can't be found
	err = rm.WithRevision(context.Background(), lock(types.FileContractID{4, 0, 4}), noopFn)
	if err == nil || !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("expected error for non-existent contract, got: %v", err)
	}

	// assert withRevision only updates the store if the revised revision number is greater than the local revision number
	rev3, _, err := store.ContractRevision(fcid3)
	if err != nil {
		t.Fatal(err)
	}
	err = rm.WithRevision(context.Background(), lock(fcid3), func(rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{ID: fcid3, Revision: rev3.Revision}, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rev3, _, err = store.ContractRevision(fcid3)
	if err != nil {
		t.Fatal(err)
	}
	if rev3.Revision.ProofHeight != 200 {
		t.Fatalf("expected proof height to be 200, got: %v", rev3.Revision.ProofHeight)
	}

	// assert withRevision errors out if the revise function returns an unexpected error
	addContract(fcid4)
	errUnexpected := errors.New("unexpected error")
	if err := rm.WithRevision(context.Background(), lock(fcid4), func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, errUnexpected
	}); err != errUnexpected {
		t.Fatalf("expected unexpected error, got: %v", err)
	}

	// assert withRevision persists the revision if no error occurs and we don't need to sync the revision
	update := frand.Uint64n(math.MaxInt32)
	err = rm.WithRevision(context.Background(), lock(fcid4), func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber = update
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rev4, _, err := store.ContractRevision(fcid4)
	if err != nil {
		t.Fatal(err)
	}
	if rev4.Revision.RevisionNumber != update {
		t.Fatalf("expected revision to be updated, got: %v", rev4.Revision.RevisionNumber)
	}

	// assert withRevision returns an error if the update fails to persist
	fcid5 := types.FileContractID{5, 0, 0}
	addContract(fcid5)
	errStore := &persistErrorStore{testStore: store, errorContractID: fcid5}
	rmErr := contracts.NewRevisionManager(latest, chain, errStore, revisionSubmissionBuffer, zaptest.NewLogger(t))
	err = rmErr.WithRevision(context.Background(), lock(fcid5), func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber = update
		return contract, proto.Usage{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "persist error") {
		t.Fatalf("expected persist error, instead error was %v", err)
	}

	// assert withRevision returns an error if the latest revision cannot be
	// found when trying to sync the revision with the host
	latest.fn = func(context.Context, types.PublicKey, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{}, errors.New("latest revision not found")
	}
	addContract(types.FileContractID{6})
	err = rm.WithRevision(context.Background(), lock(types.FileContractID{6}), invalidSigFn)
	if err == nil || !strings.Contains(err.Error(), "latest revision not found") {
		t.Fatal("unexpected error", err)
	}

	// assert withRevision returns an error if the local revision is newer than
	// the host revision, it should also have marked the contract as bad
	fcid7 := types.FileContractID{7}
	if err := store.AddFormedContract(hk, fcid7, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 2, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(1)}}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}

	latest.fn = func(context.Context, types.PublicKey, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, RevisionNumber: 1},
			Revisable: true,
			Renewed:   false,
		}, nil
	}
	err = rm.WithRevision(context.Background(), lock(fcid7), invalidSigFn)
	if err == nil || !strings.Contains(err.Error(), "local revision is newer than host revision") {
		t.Fatal("unexpected error", err)
	} else if contract, err := store.Contract(fcid7); err != nil {
		t.Fatal(err)
	} else if contract.Good {
		t.Fatal("expected contract to be marked as bad")
	}

	// assert withRevision updates the revision in the database after syncing it with the host
	fcid8 := types.FileContractID{8}
	if err := store.AddFormedContract(hk, fcid8, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 1, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(2)}}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}
	revision, _, _ := store.ContractRevision(fcid8)
	remaining := revision.Revision.RenterOutput
	remaining.Value = remaining.Value.Div64(2)
	latest.fn = func(context.Context, types.PublicKey, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, RevisionNumber: update, ProofHeight: 200, RenterOutput: remaining},
			Revisable: true,
			Renewed:   false,
		}, nil
	}
	err = rm.WithRevision(context.Background(), lock(fcid8), func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		if contract.Revision.RevisionNumber != update {
			return rhp.ContractRevision{}, proto.Usage{}, proto.ErrInvalidSignature
		}
		contract.Revision.RevisionNumber++
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	rev8, _, err := store.ContractRevision(fcid8)
	if err != nil {
		t.Fatal(err)
	} else if rev8.Revision.RevisionNumber != update+1 {
		t.Fatalf("expected revision number to be updated, got: %v, revision: %v", err, rev8.Revision.RevisionNumber)
	} else if !rev8.Revision.RenterOutput.Value.Equals(remaining.Value) {
		t.Fatalf("expected renter output to be updated, got: %v, revision: %v", err, rev8.Revision.RenterOutput.Value)
	}
	host, err := store.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if !host.TotalSpent.Equals(remaining.Value) {
		t.Fatalf("expected funds spent to be updated, got: %v, expected: %v", host.TotalSpent, remaining.Value)
	}

	// assert withRevision returns an error if it turns out the contract was renewed
	latest.fn = func(context.Context, types.PublicKey, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, RevisionNumber: 1},
			Revisable: false,
			Renewed:   true,
		}, nil
	}
	fcid9 := types.FileContractID{9}
	addContract(fcid9)
	err = rm.WithRevision(context.Background(), lock(fcid9), invalidSigFn)
	if !errors.Is(err, contracts.ErrContractRenewed) {
		t.Fatalf("expected ErrContractRenewed, got: %v", err)
	}

	// assert withRevision returns an error if it turns out the contract is not revisable
	latest.fn = func(context.Context, types.PublicKey, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, ProofHeight: revisionSubmissionBuffer + 1, RevisionNumber: 1},
			Revisable: false,
			Renewed:   false,
		}, nil
	}
	fcid10 := types.FileContractID{10}
	addContract(fcid10)
	err = rm.WithRevision(context.Background(), lock(fcid10), invalidSigFn)
	if !errors.Is(err, contracts.ErrContractNotRevisable) {
		t.Fatalf("expected ErrContractNotRevisable, got: %v", err)
	}

	// assert withRevision doesn't persist the revision if the revise function renewed the contract
	fcid11 := types.FileContractID{11}
	addContract(fcid11)
	err = rm.WithRevision(context.Background(), lock(fcid11), func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// verify revision number hasn't changed
	rev11, _, err := store.ContractRevision(fcid11)
	if err != nil {
		t.Fatal(err)
	}
	if rev11.Revision.RevisionNumber != 1 {
		t.Fatalf("expected revision number to remain 1, got %v", rev11.Revision.RevisionNumber)
	}

	// assert withRevision updates the host usage if the contract was revised
	hostBefore, err := store.Host(hk)
	if err != nil {
		t.Fatal(err)
	}

	err = rm.WithRevision(context.Background(), lock(types.FileContractID{12}), func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber++
		return contract, proto.Usage{RPC: types.NewCurrency64(1)}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	} else if contract, _, err := store.ContractRevision(types.FileContractID{12}); err != nil {
		t.Fatalf("expected revision to be persisted, but found none")
	} else if contract.Revision.RevisionNumber != 2 {
		t.Fatalf("expected revision number to be 2, got: %v", contract.Revision.RevisionNumber)
	}
	hostAfter, err := store.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if !hostAfter.TotalSpent.Equals(hostBefore.TotalSpent.Add(types.NewCurrency64(1))) {
		t.Fatalf("expected funds spent to be updated, got: %v", hostAfter.TotalSpent)
	}
}

func TestIsBeyondMaxRevisionHeight(t *testing.T) {
	for _, tc := range []struct {
		name        string
		blockHeight uint64
		proofHeight uint64
		expected    bool
	}{
		{
			name:        "before buffer",
			blockHeight: 1000,
			proofHeight: 1000 + contracts.DefaultRevisionSubmissionBuffer + 1,
			expected:    false,
		},
		{
			name:        "at beginning of buffer",
			blockHeight: 1000,
			proofHeight: 1000 + contracts.DefaultRevisionSubmissionBuffer,
			expected:    true,
		},
		{
			name:        "at end of buffer",
			blockHeight: 1000,
			proofHeight: 1000,
			expected:    true,
		},
		{
			name:        "beyond buffer",
			blockHeight: 1000,
			proofHeight: 1000 - 1,
			expected:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := contracts.IsBeyondMaxRevisionHeight(tc.proofHeight, contracts.DefaultRevisionSubmissionBuffer, tc.blockHeight)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
