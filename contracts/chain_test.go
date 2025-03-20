package contracts

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"slices"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
)

type mockProofUpdater struct {
	updateFn func(*types.StateElement)
}

func (u *mockProofUpdater) UpdateElementProof(stateElement *types.StateElement) {
	u.updateFn(stateElement)
}

type storeMock struct {
	toBroadcast []types.V2FileContractElement
	pruneCalls  int
	rejectCalls int
	settings    MaintenanceSettings
}

func (s *storeMock) ContractElementsForBroadcast(ctx context.Context, maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error) {
	return slices.Clone(s.toBroadcast), nil
}

func (s *storeMock) MaintenanceSettings(ctx context.Context) (MaintenanceSettings, error) {
	return s.settings, nil
}

func (s *storeMock) RejectPendingContracts(_ context.Context, t time.Time) error {
	if t.IsZero() {
		panic("invalid time")
	}
	s.rejectCalls++
	return nil
}

func (s *storeMock) PruneExpiredContractElements(ctx context.Context, maxBlocksSinceExpiry uint64) error {
	if maxBlocksSinceExpiry == 0 {
		panic("invalid maxBlocksSinceExpiry")
	}
	s.pruneCalls++
	return nil
}

// mockUpdateTx is a mocked implementation of UpdateTx which allows for unit
// testing the contract manager's chain updates without a full database.
type mockUpdateTx struct {
	contracts map[types.FileContractID]types.V2FileContractElement
	state     map[types.FileContractID]ContractState
}

// newMockUpdateTx creates a new mock UpdateTx.
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

func (tx *mockUpdateTx) ContractElements() ([]types.V2FileContractElement, error) {
	var stateElements []types.V2FileContractElement
	for _, fce := range tx.contracts {
		stateElements = append(stateElements, fce)
	}
	return stateElements, nil
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

func (tx *mockUpdateTx) UpdateContractElements(fces ...types.V2FileContractElement) error {
	for _, fce := range fces {
		tx.contracts[fce.ID] = fce
	}
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

type chainManagerMock struct {
	mu    sync.Mutex
	tpool []types.V2Transaction
}

func (cm *chainManagerMock) AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error) {
	cm.mu.Lock()
	cm.tpool = append(cm.tpool, txns...)
	cm.mu.Unlock()
	return false, nil
}

func (cm *chainManagerMock) TipState() consensus.State {
	return consensus.State{
		PrevTimestamps: [11]time.Time{time.Now()},
	}
}

func (cm *chainManagerMock) V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error) {
	return basis, []types.V2Transaction{txn}, nil
}

func (cm *chainManagerMock) V2PoolTransactions() []types.V2Transaction {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return slices.Clone(cm.tpool)
}

func (cm *chainManagerMock) RecommendedFee() types.Currency {
	return types.ZeroCurrency
}

type syncerMock struct {
	mu          sync.Mutex
	broadcasted []types.V2Transaction
}

func (s *syncerMock) BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) {
	s.mu.Lock()
	s.broadcasted = append(s.broadcasted, txns...)
	s.mu.Unlock()
}

func (s *syncerMock) BroadcastedSets() []types.V2Transaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.broadcasted)
}

func (s *syncerMock) Peers() []*syncer.Peer {
	return []*syncer.Peer{{}}
}

type walletMock struct {
}

func (w *walletMock) FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error) {
	return types.ChainIndex{}, nil, nil
}
func (w *walletMock) ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) {}
func (w *walletMock) SignV2Inputs(txn *types.V2Transaction, toSign []int)                  {}

func TestApplyRevertDiff(t *testing.T) {
	contracts := newContractManager(nil, nil, nil, nil)

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

func TestUpdateContractElementProofs(t *testing.T) {
	contract := types.V2FileContractElement{
		ID: types.FileContractID{1},
		StateElement: types.StateElement{
			LeafIndex:   uint64(1),
			MerkleProof: []types.Hash256{{1}},
		},
		V2FileContract: types.V2FileContract{
			HostPublicKey:   types.PublicKey{1},
			RenterPublicKey: types.PublicKey{1},
		},
	}

	// mock the update tx and add a contract
	mock := newMockUpdateTx()
	mock.AddContract(contract)
	updateTx := &updateTx{
		UpdateTx:       mock,
		knownContracts: make(map[types.FileContractID]bool),
	}

	// check initial contract
	if fce, _ := mock.Contract(contract.ID); !reflect.DeepEqual(fce, contract) {
		t.Fatalf("mismatch \n%+v\n%+v", fce, contract)
	}

	// update proof on contract
	contract2 := contract
	contract2.StateElement.MerkleProof = []types.Hash256{{2}}
	updateContractElementProofs(updateTx, &mockProofUpdater{updateFn: func(stateElement *types.StateElement) {
		stateElement.MerkleProof = contract2.StateElement.MerkleProof
	}})
	if fce, _ := mock.Contract(contract2.ID); !reflect.DeepEqual(fce, contract2) {
		t.Fatalf("mismatch \n%+v\n%+v", fce, contract2)
	}
}

func TestProcessActions(t *testing.T) {
	cmMock := &chainManagerMock{}
	syncerMock := &syncerMock{}
	store := &storeMock{}
	contracts := newContractManager(cmMock, store, syncerMock, &walletMock{})

	contract := types.V2FileContractElement{
		ID: types.FileContractID{1},
		V2FileContract: types.V2FileContract{
			ExpirationHeight: 100,
		},
	}

	// assert asserts the number of txns in the mocked pool, the number of
	// broadcasted transactions in the mocked syncer, the number of resolutions
	// in the latest broadcasted transactions and the contract elements in the
	// store.
	assert := func(poolTxns, broadcastedTxns, resolutions, pruneCalls, rejectCalls int) {
		t.Helper()
		if len(cmMock.V2PoolTransactions()) != poolTxns {
			t.Fatalf("expected %v contract in tpool, got %v", poolTxns, len(cmMock.tpool))
		} else if sets := syncerMock.BroadcastedSets(); len(sets) != broadcastedTxns {
			t.Fatalf("expected %v broadcasted contracts, got %v", broadcastedTxns, len(syncerMock.broadcasted))
		} else if broadcastedTxns > 0 && len(sets[broadcastedTxns-1].FileContractResolutions) != resolutions {
			t.Fatalf("expected %v contract resolution in broadcast, got %v", resolutions, len(sets[0].FileContracts))
		} else if store.pruneCalls != pruneCalls {
			t.Fatalf("expected %v calls to PruneExpiredContractElements, got %v", pruneCalls, store.pruneCalls)
		} else if store.rejectCalls != rejectCalls {
			t.Fatalf("expected %v calls to RejectPendingContracts, got %v", rejectCalls, store.rejectCalls)
		}
	}

	// broadcast when no contract should be broadcasted
	if err := contracts.ProcessActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(0, 0, 0, 1, 1)

	// broadcast with 1 contract to broadcast
	store.toBroadcast = []types.V2FileContractElement{contract}
	if err := contracts.ProcessActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(1, 1, 1, 2, 2)
}
