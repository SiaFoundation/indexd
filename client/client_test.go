package client

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
	"go.uber.org/zap"
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

type mockStore struct {
	fundsSpent map[types.PublicKey]types.Currency
	revisions  map[types.FileContractID]types.V2FileContract
	renewed    map[types.FileContractID]bool
	good       map[types.FileContractID]bool
}

func (s *mockStore) ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error) {
	if contractID == (types.FileContractID{4, 0, 4}) {
		return rhp.ContractRevision{}, false, errors.New("revision not found")
	}
	rev, ok := s.revisions[contractID]
	if !ok {
		rev = types.V2FileContract{ProofHeight: 1000, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(1)}}
	}
	return rhp.ContractRevision{ID: contractID, Revision: rev}, s.renewed[contractID], nil
}

func (s *mockStore) UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error {
	if contract.ID == (types.FileContractID{5, 0, 0}) {
		return errors.New("persist error")
	}
	s.fundsSpent[contract.Revision.HostPublicKey] = s.fundsSpent[contract.Revision.HostPublicKey].Add(usage.RenterCost())
	s.revisions[contract.ID] = contract.Revision
	return nil
}

func (s *mockStore) MarkContractBad(contractID types.FileContractID) error {
	s.good[contractID] = false
	return nil
}

func TestWithRevision(t *testing.T) {
	db := &mockStore{
		fundsSpent: make(map[types.PublicKey]types.Currency),
		revisions:  make(map[types.FileContractID]types.V2FileContract),
		renewed:    make(map[types.FileContractID]bool),
		good:       make(map[types.FileContractID]bool),
	}
	cm := &mockChainManager{
		state: consensus.State{
			Index: types.ChainIndex{Height: 100},
		},
	}
	revisionSubmissionBuffer := uint64(10)
	c := newHostClient(types.PublicKey{}, cm, nil, nil, db, revisionSubmissionBuffer, zap.NewNop())

	fcid1 := types.FileContractID{1}
	fcid2 := types.FileContractID{2}
	fcid3 := types.FileContractID{3}
	fcid4 := types.FileContractID{4}

	db.renewed[fcid1] = true                                                                // renewed
	db.revisions[fcid2] = types.V2FileContract{ProofHeight: 100 + revisionSubmissionBuffer} // not revisable
	db.revisions[fcid3] = types.V2FileContract{ProofHeight: 200, RevisionNumber: 1}
	db.revisions[types.FileContractID{12}] = types.V2FileContract{ProofHeight: 200, RevisionNumber: 1}

	noopFn := func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, nil
	}
	invalidSigFn := func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, proto.ErrInvalidSignature
	}

	// assert ErrContractRenewed is returned for a renewed contract
	err := c.withRevision(context.Background(), fcid1, noopFn)
	if !errors.Is(err, ErrContractRenewed) {
		t.Fatalf("expected ErrContractRenewed, got: %v", err)
	}

	// assert ErrContractNotRevisable is returned for a contract that can't be revised
	err = c.withRevision(context.Background(), fcid2, noopFn)
	if !errors.Is(err, ErrContractNotRevisable) {
		t.Fatalf("expected ErrContractNotRevisable, got: %v", err)
	}

	// assert withRevision errors out if the revision can't be found
	err = c.withRevision(context.Background(), types.FileContractID{4, 0, 4}, noopFn)
	if err == nil || !strings.Contains(err.Error(), "revision not found") {
		t.Fatalf("expected error for non-existent contract, got: %v", err)
	}

	// assert withRevision only updates the store if the revised revision number is greater than the local revision number
	err = c.withRevision(context.Background(), fcid3, func(rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{ID: fcid3, Revision: db.revisions[fcid3]}, proto.Usage{}, nil
	})
	if err != nil || db.revisions[fcid3].ProofHeight != 200 {
		t.Fatalf("expected no error and revision to not be updated, got: %v, revision: %v", err, db.revisions[fcid3])
	}

	// assert withRevision errors out if the revise function returns an unexpected error
	errUnexpected := errors.New("unexpected error")
	if err := c.withRevision(context.Background(), fcid4, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return rhp.ContractRevision{}, proto.Usage{}, errUnexpected
	}); err != errUnexpected {
		t.Fatalf("expected unexpected error, got: %v", err)
	}

	// assert withRevision persists the revision if no error occurs and we don't need to sync the revision
	update := frand.Uint64n(math.MaxUint64)
	err = c.withRevision(context.Background(), fcid4, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber = update
		return contract, proto.Usage{}, nil
	})
	if err != nil || db.revisions[fcid4].RevisionNumber != update {
		t.Fatalf("expected no error and revision to be updated, got: %v, revision: %v", err, db.revisions[types.FileContractID{4}])
	}

	// assert withRevision returns an error if the update fails to persist
	err = c.withRevision(context.Background(), types.FileContractID{5, 0, 0}, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber = update
		return contract, proto.Usage{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "persist error") {
		t.Fatalf("expected persist error, instead error was %v", err)
	}

	// assert withRevision returns an error if the latest revision cannot be
	// found when trying to sync the revision with the host
	c.latestRevisionFn = func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{}, errors.New("latest revision not found")
	}
	err = c.withRevision(context.Background(), types.FileContractID{6}, invalidSigFn)
	if err == nil || !strings.Contains(err.Error(), "latest revision not found") {
		t.Fatal("unexpected error", err)
	}

	// assert withRevision returns an error if the local revision is newer than
	// the host revision, it should also have marked the contract as bad
	db.revisions[types.FileContractID{7}] = types.V2FileContract{ProofHeight: 200, RevisionNumber: 2, RenterOutput: types.SiacoinOutput{Value: types.Siacoins(1)}}
	db.good[types.FileContractID{7}] = true
	c.latestRevisionFn = func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{RevisionNumber: 1},
			Revisable: true,
			Renewed:   false,
		}, nil
	}
	err = c.withRevision(context.Background(), types.FileContractID{7}, invalidSigFn)
	if err == nil || !strings.Contains(err.Error(), "local revision is newer than host revision") {
		t.Fatal("unexpected error", err)
	} else if db.good[types.FileContractID{7}] {
		t.Fatal("expected contract to be marked as bad")
	}

	// assert withRevision updates the revision in the database after syncing it with the host
	revision, _, _ := db.ContractRevision(types.FileContractID{8})
	remaining := revision.Revision.RenterOutput
	remaining.Value = remaining.Value.Div64(2)
	c.latestRevisionFn = func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{RevisionNumber: update, ProofHeight: 200, RenterOutput: remaining},
			Revisable: true,
			Renewed:   false,
		}, nil
	}
	err = c.withRevision(context.Background(), types.FileContractID{8}, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		if contract.Revision.RevisionNumber != update {
			return rhp.ContractRevision{}, proto.Usage{}, proto.ErrInvalidSignature
		}
		contract.Revision.RevisionNumber++
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	} else if db.revisions[types.FileContractID{8}].RevisionNumber != update+1 {
		t.Fatalf("expected revision number to be updated, got: %v, revision: %v", err, db.revisions[types.FileContractID{8}])
	} else if !db.revisions[types.FileContractID{8}].RenterOutput.Value.Equals(remaining.Value) {
		t.Fatalf("expected renter output to be updated, got: %v, revision: %v", err, db.revisions[types.FileContractID{8}])
	} else if !db.fundsSpent[c.hostKey].Equals(remaining.Value) {
		t.Fatalf("expected funds spent to be updated, got: %v, expected: %v", db.fundsSpent[c.hostKey], remaining.Value)
	}

	// assert withRevision returns an error if it turns out the contract was renewed
	c.latestRevisionFn = func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{RevisionNumber: 1},
			Revisable: false,
			Renewed:   true,
		}, nil
	}
	err = c.withRevision(context.Background(), types.FileContractID{9}, invalidSigFn)
	if !errors.Is(err, ErrContractRenewed) {
		t.Fatalf("expected ErrContractRenewed, got: %v", err)
	}

	// assert withRevision returns an error if it turns out the contract is not revisable
	c.latestRevisionFn = func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
		return proto.RPCLatestRevisionResponse{
			Contract:  types.V2FileContract{ProofHeight: revisionSubmissionBuffer + 1},
			Revisable: false,
			Renewed:   false,
		}, nil
	}
	err = c.withRevision(context.Background(), types.FileContractID{10}, invalidSigFn)
	if !errors.Is(err, ErrContractNotRevisable) {
		t.Fatalf("expected ErrContractNotRevisable, got: %v", err)
	}

	// assert withRevision doesn't persist the revision if the revise function renewed the contract
	err = c.withRevision(context.Background(), types.FileContractID{11}, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		return contract, proto.Usage{}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	} else if _, exists := db.revisions[types.FileContractID{11}]; exists {
		t.Fatalf("expected no revision to be persisted, but found one")
	}

	// assert withRevision updates the host usage if the contract was revised
	before := db.fundsSpent[c.hostKey]
	err = c.withRevision(context.Background(), types.FileContractID{12}, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		contract.Revision.RevisionNumber++
		return contract, proto.Usage{RPC: types.NewCurrency64(1)}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	} else if contract, exists := db.revisions[types.FileContractID{12}]; !exists {
		t.Fatalf("expected revision to be persisted, but found none")
	} else if contract.RevisionNumber != 2 {
		t.Fatalf("expected revision number to be 2, got: %v", contract.RevisionNumber)
	} else if db.fundsSpent[c.hostKey] != before.Add(types.NewCurrency64(1)) {
		t.Fatalf("expected funds spent to be updated, got: %v", db.fundsSpent[c.hostKey])
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
			proofHeight: 1000 + defaultRevisionSubmissionBuffer + 1,
			expected:    false,
		},
		{
			name:        "at beginning of buffer",
			blockHeight: 1000,
			proofHeight: 1000 + defaultRevisionSubmissionBuffer,
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
			result := isBeyondMaxRevisionHeight(tc.proofHeight, defaultRevisionSubmissionBuffer, tc.blockHeight)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
