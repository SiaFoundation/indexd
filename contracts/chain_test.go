package contracts

import (
	"errors"
	"reflect"
	"testing"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

// mockUpdateTx is a mocked implementation of UpdateTx which allows for unit
// testing the contract manager's chain updates without a full database.
type mockUpdateTx struct {
	contracts map[types.FileContractID]types.V2FileContractElement
	state     map[types.FileContractID]ContractState
}

func newMockUpdateTx() *mockUpdateTx {
	return &mockUpdateTx{
		contracts: make(map[types.FileContractID]types.V2FileContractElement),
		state:     make(map[types.FileContractID]ContractState),
	}
}

func (tx *mockUpdateTx) AddContract(fce types.V2FileContractElement) {
	tx.contracts[fce.ID] = fce
	tx.state[fce.ID] = ContractStatePending
}

func (tx *mockUpdateTx) Contract(contractID types.FileContractID) (types.V2FileContractElement, ContractState) {
	fce, ok := tx.contracts[contractID]
	if !ok {
		panic("contract not found")
	}
	state, ok := tx.state[contractID]
	if !ok {
		panic("contract state not found")
	}
	return fce, state
}

func (tx *mockUpdateTx) IsKnownContract(contractID types.FileContractID) (bool, error) {
	_, ok := tx.contracts[contractID]
	return ok, nil
}

func (tx *mockUpdateTx) UpdateContractElement(fce types.V2FileContractElement) error {
	tx.contracts[fce.ID] = fce
	return nil
}

func (tx *mockUpdateTx) UpdateContractElementProofs(updater wallet.ProofUpdater) error {
	for i := range tx.contracts {
		fce := tx.contracts[i]
		updater.UpdateElementProof(&fce.StateElement)
		tx.contracts[i] = fce
	}
	return nil
}

func (tx *mockUpdateTx) UpdateContractState(contractID types.FileContractID, state ContractState) error {
	if _, ok := tx.contracts[contractID]; !ok {
		return errors.New("contract not found")
	}
	tx.state[contractID] = state
	return nil
}

func TestApplyRevertDiff(t *testing.T) {
	contracts, err := NewManager()
	if err != nil {
		t.Fatal(err)
	}

	// create a contract
	contractID := types.FileContractID{1, 2, 3}
	fce := types.V2FileContractElement{
		ID: contractID,
		StateElement: types.StateElement{
			LeafIndex:   1,
			MerkleProof: []types.Hash256{{123}},
		},
		V2FileContract: types.V2FileContract{
			HostPublicKey:   types.PublicKey{1},
			RenterPublicKey: types.PublicKey{1},
		},
	}

	// mock the update tx
	mock := newMockUpdateTx()
	mock.AddContract(fce)
	updateTx := &updateTx{
		UpdateTx:       mock,
		knownContracts: make(map[types.FileContractID]bool),
	}

	// helper to apply/revert diff
	applyDiff := func(diff consensus.V2FileContractElementDiff) {
		t.Helper()
		err := contracts.applyContractDiff(updateTx, diff)
		if err != nil {
			t.Fatal(err)
		}
	}
	revertDiff := func(diff consensus.V2FileContractElementDiff) {
		t.Helper()
		err := contracts.revertContractDiff(updateTx, diff)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertContract := func(state ContractState) {
		t.Helper()
		storedFCE, storedState := mock.Contract(contractID)
		if storedState != state {
			t.Fatalf("expected state %v, got %v", state, storedState)
		} else if !reflect.DeepEqual(storedFCE, fce) {
			t.Fatalf("expected contract %v, got %v", fce, storedFCE)
		}
	}

	// initial state
	assertContract(ContractStatePending)

	// confirm the contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: fce,
	})
	assertContract(ContractStateActive)

	// revise contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Revision:              &fce.V2FileContract,
	})
	assertContract(ContractStateActive)

	// resolve contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2StorageProof{},
	})
	assertContract(ContractStateResolved)

	// revert resolution
	fce.V2FileContract.RevisionNumber--
	revertDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2StorageProof{},
	})
	assertContract(ContractStateActive)

	// revert revision
	fce.V2FileContract.RevisionNumber--
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Revision:              &fce.V2FileContract,
	})
	assertContract(ContractStateActive)

	// revert contract
	fce.V2FileContract.RevisionNumber--
	revertDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: fce,
	})
	assertContract(ContractStatePending)
}
