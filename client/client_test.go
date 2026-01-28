package client_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type mockChainManager struct {
	state consensus.State
}

func (c *mockChainManager) TipState() consensus.State {
	return c.state
}

func (c *mockChainManager) V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error) {
	return types.ChainIndex{}, nil, nil
}

type testStore struct {
	testutils.TestStore
}

func newTestStore(t testing.TB) testStore {
	s := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() {
		s.Close()
	})

	return testStore{s}
}

func (s *testStore) UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error {
	if contract.ID == (types.FileContractID{5, 0, 0}) {
		return errors.New("persist error")
	}
	return s.TestStore.UpdateContractRevision(contract, usage)
}

func TestWithRevision(t *testing.T) {
	s := newTestStore(t)
	cm := &mockChainManager{
		state: consensus.State{
			Index: types.ChainIndex{Height: 100},
		},
	}
	revisionSubmissionBuffer := uint64(10)
	hk := types.PublicKey{1}
	s.AddTestHost(t, hosts.Host{PublicKey: hk})

	c := client.NewHostClient(hk, cm, nil, nil, &s, revisionSubmissionBuffer, zap.NewNop())

	addContract := func(fcid types.FileContractID) {
		t.Helper()
		if err := s.AddFormedContract(hk, fcid, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 1}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
			t.Fatal(err)
		}
	}

	fcid1 := types.FileContractID{1}
	fcid2 := types.FileContractID{2}
	fcid3 := types.FileContractID{3}
	fcid4 := types.FileContractID{4}

	// renewed contract
	addContract(fcid1)
	if err := s.AddRenewedContract(fcid1, types.FileContractID{1, 1}, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 1}, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}

	// not revisable
	if err := s.AddFormedContract(hk, fcid2, types.V2FileContract{HostPublicKey: hk, ProofHeight: 100 + revisionSubmissionBuffer}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
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
	err := c.WithRevision(context.Background(), fcid1, noopFn)
	if !errors.Is(err, client.ErrContractRenewed) {
		t.Fatalf("expected ErrContractRenewed, got: %v", err)
	}

	// assert ErrContractNotRevisable is returned for a contract that can't be revised
	err = c.WithRevision(context.Background(), fcid2, noopFn)
	if !errors.Is(err, client.ErrContractNotRevisable) {
		t.Fatalf("expected ErrContractNotRevisable, got: %v", err)
	}

	// assert withRevision errors out if the revision can't be found
	err = c.WithRevision(context.Background(), types.FileContractID{4, 0, 4}, noopFn)
	if err == nil || !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("expected error for non-existent contract, got: %v", err)
	}

	// assert withRevision only updates the store if the revised revision number is greater than the local revision number
	rev3, _, err := s.ContractRevision(fcid3)
	if err != nil {
		t.Fatal(err)
	}
	err = c.WithRevision(context.Background(), fcid3, func(rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{ID: fcid3, Revision: rev3.Revision}, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rev3, _, err = s.ContractRevision(fcid3)
	if err != nil {
		t.Fatal(err)
	}
	if rev3.Revision.ProofHeight != 200 {
		t.Fatalf("expected proof height to be 200, got: %v", rev3.Revision.ProofHeight)
	}

	// assert withRevision errors out if the revise function returns an unexpected error
	addContract(fcid4)
	errUnexpected := errors.New("unexpected error")
	if err := c.WithRevision(context.Background(), fcid4, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, errUnexpected
	}); err != errUnexpected {
		t.Fatalf("expected unexpected error, got: %v", err)
	}

	// assert withRevision persists the revision if no error occurs and we don't need to sync the revision
	update := frand.Uint64n(math.MaxInt32)
	err = c.WithRevision(context.Background(), fcid4, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber = update
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rev4, _, err := s.ContractRevision(fcid4)
	if err != nil {
		t.Fatal(err)
	}
	if rev4.Revision.RevisionNumber != update {
		t.Fatalf("expected revision to be updated, got: %v", rev4.Revision.RevisionNumber)
	}

	// assert withRevision returns an error if the update fails to persist
	addContract(types.FileContractID{5, 0, 0})
	err = c.WithRevision(context.Background(), types.FileContractID{5, 0, 0}, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber = update
		return contract, proto.Usage{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "persist error") {
		t.Fatalf("expected persist error, instead error was %v", err)
	}

	// assert withRevision returns an error if the latest revision cannot be
	// found when trying to sync the revision with the host
	c.SetLatestRevisionFn(func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{}, errors.New("latest revision not found")
	})
	addContract(types.FileContractID{6})
	err = c.WithRevision(context.Background(), types.FileContractID{6}, invalidSigFn)
	if err == nil || !strings.Contains(err.Error(), "latest revision not found") {
		t.Fatal("unexpected error", err)
	}

	// assert withRevision returns an error if the local revision is newer than
	// the host revision, it should also have marked the contract as bad
	fcid7 := types.FileContractID{7}
	if err := s.AddFormedContract(hk, fcid7, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 2, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(1)}}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}

	c.SetLatestRevisionFn(func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, RevisionNumber: 1},
			Revisable: true,
			Renewed:   false,
		}, nil
	})
	err = c.WithRevision(context.Background(), fcid7, invalidSigFn)
	if err == nil || !strings.Contains(err.Error(), "local revision is newer than host revision") {
		t.Fatal("unexpected error", err)
	} else if contract, err := s.Contract(fcid7); err != nil {
		t.Fatal(err)
	} else if contract.Good {
		t.Fatal("expected contract to be marked as bad")
	}

	// assert withRevision updates the revision in the database after syncing it with the host
	fcid8 := types.FileContractID{8}
	if err := s.AddFormedContract(hk, fcid8, types.V2FileContract{HostPublicKey: hk, ProofHeight: 200, RevisionNumber: 1, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(2)}}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	}
	revision, _, _ := s.ContractRevision(fcid8)
	remaining := revision.Revision.RenterOutput
	remaining.Value = remaining.Value.Div64(2)
	c.SetLatestRevisionFn(func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, RevisionNumber: update, ProofHeight: 200, RenterOutput: remaining},
			Revisable: true,
			Renewed:   false,
		}, nil
	})
	err = c.WithRevision(context.Background(), fcid8, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		if contract.Revision.RevisionNumber != update {
			return rhp.ContractRevision{}, proto.Usage{}, proto.ErrInvalidSignature
		}
		contract.Revision.RevisionNumber++
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	rev8, _, err := s.ContractRevision(fcid8)
	if err != nil {
		t.Fatal(err)
	} else if rev8.Revision.RevisionNumber != update+1 {
		t.Fatalf("expected revision number to be updated, got: %v, revision: %v", err, rev8.Revision.RevisionNumber)
	} else if !rev8.Revision.RenterOutput.Value.Equals(remaining.Value) {
		t.Fatalf("expected renter output to be updated, got: %v, revision: %v", err, rev8.Revision.RenterOutput.Value)
	}
	host, err := s.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if !host.TotalSpent.Equals(remaining.Value) {
		t.Fatalf("expected funds spent to be updated, got: %v, expected: %v", host.TotalSpent, remaining.Value)
	}

	// assert withRevision returns an error if it turns out the contract was renewed
	c.SetLatestRevisionFn(func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, RevisionNumber: 1},
			Revisable: false,
			Renewed:   true,
		}, nil
	})
	fcid9 := types.FileContractID{9}
	addContract(fcid9)
	err = c.WithRevision(context.Background(), fcid9, invalidSigFn)
	if !errors.Is(err, client.ErrContractRenewed) {
		t.Fatalf("expected ErrContractRenewed, got: %v", err)
	}

	// assert withRevision returns an error if it turns out the contract is not revisable
	c.SetLatestRevisionFn(func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{HostPublicKey: hk, ProofHeight: revisionSubmissionBuffer + 1, RevisionNumber: 1},
			Revisable: false,
			Renewed:   false,
		}, nil
	})
	fcid10 := types.FileContractID{10}
	addContract(fcid10)
	err = c.WithRevision(context.Background(), fcid10, invalidSigFn)
	if !errors.Is(err, client.ErrContractNotRevisable) {
		t.Fatalf("expected ErrContractNotRevisable, got: %v", err)
	}

	// assert withRevision doesn't persist the revision if the revise function renewed the contract
	fcid11 := types.FileContractID{11}
	addContract(fcid11)
	err = c.WithRevision(context.Background(), fcid11, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// verify revision number hasn't changed
	rev11, _, err := s.ContractRevision(fcid11)
	if err != nil {
		t.Fatal(err)
	}
	if rev11.Revision.RevisionNumber != 1 {
		t.Fatalf("expected revision number to remain 1, got %v", rev11.Revision.RevisionNumber)
	}

	// assert withRevision updates the host usage if the contract was revised
	hostBefore, err := s.Host(hk)
	if err != nil {
		t.Fatal(err)
	}

	err = c.WithRevision(context.Background(), types.FileContractID{12}, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber++
		return contract, proto.Usage{RPC: types.NewCurrency64(1)}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	} else if contract, _, err := s.ContractRevision(types.FileContractID{12}); err != nil {
		t.Fatalf("expected revision to be persisted, but found none")
	} else if contract.Revision.RevisionNumber != 2 {
		t.Fatalf("expected revision number to be 2, got: %v", contract.Revision.RevisionNumber)
	}
	hostAfter, err := s.Host(hk)
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
			proofHeight: 1000 + client.DefaultRevisionSubmissionBuffer + 1,
			expected:    false,
		},
		{
			name:        "at beginning of buffer",
			blockHeight: 1000,
			proofHeight: 1000 + client.DefaultRevisionSubmissionBuffer,
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
			result := client.IsBeyondMaxRevisionHeight(tc.proofHeight, client.DefaultRevisionSubmissionBuffer, tc.blockHeight)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
