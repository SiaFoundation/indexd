package contracts

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"slices"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/hosts"
)

type mockProofUpdater struct {
	updateFn func(*types.StateElement)
}

func (u *mockProofUpdater) UpdateElementProof(stateElement *types.StateElement) {
	u.updateFn(stateElement)
}

type storeMock struct {
	mu                        sync.Mutex
	contracts                 []Contract
	revisions                 []rhp.ContractRevision
	toBroadcast               []types.V2FileContractElement
	pruneCalls                int
	pruneContractSectorsCalls int
	rejectCalls               int
	activeAccounts            uint64
	settings                  MaintenanceSettings
	hosts                     map[types.PublicKey]hosts.Host
	sectors                   map[types.PublicKey][]sector
}

type sector struct {
	root       types.Hash256
	contractID *types.FileContractID
}

func newStoreMock() *storeMock {
	return &storeMock{
		hosts:          make(map[types.PublicKey]hosts.Host),
		sectors:        make(map[types.PublicKey][]sector),
		activeAccounts: 1,
	}
}

func (s *storeMock) ActiveAccounts(threshold time.Time) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeAccounts, nil
}

func (s *storeMock) AddFormedContract(hostKey types.PublicKey, contractID types.FileContractID, revision types.V2FileContract, contractPrice, allowance, minerFee types.Currency, _ proto.Usage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contracts = append(s.contracts, Contract{
		ID:      contractID,
		HostKey: hostKey,

		Formation:        time.Now(),
		ProofHeight:      revision.ProofHeight,
		ExpirationHeight: revision.ExpirationHeight,
		State:            ContractStatePending,

		RemainingAllowance: allowance,
		TotalCollateral:    revision.TotalCollateral,

		ContractPrice:    contractPrice,
		InitialAllowance: allowance,
		MinerFee:         minerFee,

		Good: true,
	})
	s.revisions = append(s.revisions, rhp.ContractRevision{ID: contractID, Revision: revision})
	return nil
}

func (s *storeMock) AddRenewedContract(renewedFrom, renewedTo types.FileContractID, revision types.V2FileContract, contractPrice, minerFee types.Currency, _ proto.Usage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var source *Contract
	for i := range s.contracts {
		if s.contracts[i].ID == renewedFrom {
			s.contracts[i].RenewedTo = renewedTo
			source = &s.contracts[i]
			break
		}
	}
	if source == nil {
		return ErrNotFound
	}
	s.contracts = append(s.contracts, Contract{
		ID:      renewedTo,
		HostKey: source.HostKey,

		Capacity:    source.Size,
		Size:        source.Size,
		RenewedFrom: source.ID,

		Formation:        time.Now(),
		ProofHeight:      revision.ProofHeight,
		ExpirationHeight: revision.ExpirationHeight,
		State:            ContractStatePending,

		RemainingAllowance: revision.RenterOutput.Value,
		UsedCollateral:     revision.RiskedCollateral(),
		TotalCollateral:    revision.TotalCollateral,

		ContractPrice:    contractPrice,
		InitialAllowance: revision.RenterOutput.Value,
		MinerFee:         minerFee,

		Good: true,
	})
	s.revisions = append(s.revisions, rhp.ContractRevision{ID: renewedTo, Revision: revision})
	return nil
}

func (s *storeMock) BlockHosts(hostKeys []types.PublicKey, reasons []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hostKey := range hostKeys {
		host, ok := s.hosts[hostKey]
		if !ok {
			return hosts.ErrNotFound
		}
		if !host.Blocked {
			host.Blocked = true
			host.BlockedReasons = reasons
			s.hosts[hostKey] = host
		}

		for i := range s.contracts {
			if s.contracts[i].HostKey == hostKey {
				s.contracts[i].Good = false
			}
		}
	}

	return nil
}

func (s *storeMock) ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var renewed bool
	for i, c := range s.contracts {
		if c.ID == contractID {
			renewed = c.RenewedTo != (types.FileContractID{})
			return s.revisions[i], renewed, nil
		}
	}
	return rhp.ContractRevision{}, false, errors.New("contract not found")
}

func (s *storeMock) ContractElement(contractID types.FileContractID) (types.V2FileContractElement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range s.contracts {
		if c.ID == contractID {
			return types.V2FileContractElement{
				ID: contractID,
				StateElement: types.StateElement{
					LeafIndex:   1,
					MerkleProof: []types.Hash256{{1}},
				},
				V2FileContract: types.V2FileContract{
					HostPublicKey: c.HostKey,
					Capacity:      c.Capacity,
					Filesize:      c.Size,
				},
			}, nil
		}
	}
	return types.V2FileContractElement{}, ErrNotFound
}

func (s *storeMock) ContractElementsForBroadcast(maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.toBroadcast), nil
}

func (s *storeMock) Contract(contractID types.FileContractID) (Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.contracts {
		if c.ID == contractID {
			return c, nil
		}
	}
	return Contract{}, ErrNotFound
}

func (s *storeMock) Contracts(offset, limit int, queryOpts ...ContractQueryOpt) ([]Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var opts ContractQueryOpts
	for _, opt := range queryOpts {
		opt(&opts)
	}

	filtered := make([]Contract, 0, len(s.contracts))
	for _, c := range s.contracts {
		if opts.Revisable != nil {
			isRevisable := (c.State == ContractStatePending || c.State == ContractStateActive) && c.RenewedTo == (types.FileContractID{})
			if isRevisable != *opts.Revisable {
				continue
			}
		}
		if opts.Good != nil {
			if c.Good != *opts.Good {
				continue
			}
		}
		filtered = append(filtered, c)
	}

	if offset > len(filtered) {
		return nil, nil
	}
	filtered = filtered[offset:]

	if limit > 0 && limit < len(filtered) {
		filtered = filtered[:limit]
	}

	return filtered, nil
}

func (s *storeMock) Host(hostKey types.PublicKey) (hosts.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	host, ok := s.hosts[hostKey]
	if !ok {
		return hosts.Host{}, hosts.ErrNotFound
	}
	return host, nil
}

func (s *storeMock) Hosts(offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := slices.Collect(maps.Values(s.hosts))
	slices.SortFunc(copied, func(a, b hosts.Host) int {
		// sort by public key to make order in testing deterministic
		return strings.Compare(a.PublicKey.String(), b.PublicKey.String())
	})
	opts := hosts.DefaultHostsQueryOpts
	for _, opt := range queryOpts {
		opt(&opts)
	}
	filter := copied[:0]
	for _, h := range copied {
		keep := true
		if opts.Usable != nil {
			keep = h.Usability.Usable() == *opts.Usable
		}
		if opts.Blocked != nil {
			keep = keep && h.Blocked == *opts.Blocked
		}
		if opts.ActiveContracts != nil {
			keep = keep && *opts.ActiveContracts == slices.ContainsFunc(s.contracts, func(contract Contract) bool {
				return contract.HostKey == h.PublicKey
			})
		}
		if keep {
			filter = append(filter, h)
		}
	}
	return filter, nil
}

func (s *storeMock) HostsWithUnpinnableSectors() (hks []types.PublicKey, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, host := range s.hosts {
		hasContract := slices.ContainsFunc(s.contracts, func(c Contract) bool {
			return c.HostKey == host.PublicKey
		})
		_, hasSector := s.sectors[host.PublicKey]
		if !hasContract && hasSector {
			hks = append(hks, host.PublicKey)
		}
	}
	return hks, nil
}

func (s *storeMock) LastScannedIndex() (ci types.ChainIndex, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return types.ChainIndex{}, nil
}

func (s *storeMock) MaintenanceSettings() (MaintenanceSettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settings, nil
}

func (s *storeMock) UpdateMaintenanceSettings(ms MaintenanceSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings = ms
	return nil
}

func (s *storeMock) MarkUnrenewableContractsBad(minProofHeight uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.contracts {
		if s.contracts[i].ProofHeight <= minProofHeight {
			s.contracts[i].Good = false
		}
	}
	return nil
}

func (s *storeMock) MarkSectorsUnpinnable(threshold time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *storeMock) MarkBroadcastAttempt(contractID types.FileContractID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.contracts {
		if s.contracts[i].ID == contractID {
			s.contracts[i].LastBroadcastAttempt = time.Now()
		}
	}
	return nil
}

func (s *storeMock) RejectPendingContracts(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.IsZero() {
		panic("invalid time")
	}
	s.rejectCalls++
	return nil
}

func (s *storeMock) PruneExpiredContractElements(maxBlocksSinceExpiry uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxBlocksSinceExpiry == 0 {
		panic("invalid maxBlocksSinceExpiry")
	}
	s.pruneCalls++
	return nil
}

func (s *storeMock) PruneContractSectorsMap(maxBlocksSinceExpiry uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxBlocksSinceExpiry == 0 {
		panic("invalid maxBlocksSinceExpiry")
	}
	s.pruneContractSectorsCalls++
	return nil
}

func (s *storeMock) ScheduleAccountForFunding(hostKey types.PublicKey, account proto.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *storeMock) UpdateHostSettings(hostKey types.PublicKey, settings proto.HostSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hosts[hostKey]
	if !ok {
		return hosts.ErrNotFound
	}
	h.Settings = settings
	h.Usability.AcceptingContracts = settings.AcceptingContracts
	s.hosts[hostKey] = h
	return nil
}

func (s *storeMock) UpdateContractRevision(contract rhp.ContractRevision, _ proto.Usage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, c := range s.contracts {
		if c.ID == contract.ID {
			s.revisions[i] = contract
			return nil
		}
	}
	return errors.New("contract not found")
}

func (s *storeMock) UsabilitySettings() (hosts.UsabilitySettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return hosts.DefaultUsabilitySettings, nil
}

func (s *storeMock) addTestContract(t *testing.T, hk types.PublicKey, good bool, fcids ...types.FileContractID) types.FileContractID {
	t.Helper()

	var fcid types.FileContractID
	switch len(fcids) {
	case 0:
		fcid = types.FileContractID(hk)
	case 1:
		fcid = fcids[0]
	default:
		panic("developer error")
	}

	revision := newTestRevision(hk)
	err := s.AddFormedContract(hk, fcid, revision, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), proto.Usage{})
	if err != nil {
		t.Fatal(err)
	}

	s.contracts[len(s.contracts)-1].Good = good
	return fcid
}

// mockUpdateTx is a mocked implementation of UpdateTx which allows for unit
// testing the contract manager's chain updates without a full database.
type mockUpdateTx struct {
	contracts map[types.FileContractID]types.V2FileContractElement
	state     map[types.FileContractID]ContractState
	renewedTo map[types.FileContractID]*types.FileContractID
}

// newMockUpdateTx creates a new mock UpdateTx.
func newMockUpdateTx() *mockUpdateTx {
	return &mockUpdateTx{
		contracts: make(map[types.FileContractID]types.V2FileContractElement),
		state:     make(map[types.FileContractID]ContractState),
		renewedTo: make(map[types.FileContractID]*types.FileContractID),
	}
}

func (tx *mockUpdateTx) AddContract(fce types.V2FileContractElement) {
	tx.contracts[fce.ID] = fce
	tx.state[fce.ID] = ContractStatePending
	tx.renewedTo[fce.ID] = nil
}

func (tx *mockUpdateTx) ContractElements() ([]types.V2FileContractElement, error) {
	var stateElements []types.V2FileContractElement
	for _, fce := range tx.contracts {
		stateElements = append(stateElements, fce)
	}
	return stateElements, nil
}

func (tx *mockUpdateTx) RenewedTo(contractID types.FileContractID) *types.FileContractID {
	renewedTo, ok := tx.renewedTo[contractID]
	if !ok {
		panic("renewed to ID not found")
	}
	return renewedTo
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
		tx.contracts[fce.ID] = fce.Copy()
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

func (tx *mockUpdateTx) UpdateContractRenewedTo(contractID types.FileContractID, renewedTo *types.FileContractID) error {
	if _, ok := tx.contracts[contractID]; !ok {
		return errors.New("contract not found")
	}
	tx.renewedTo[contractID] = renewedTo
	return nil
}

type chainManagerMock struct {
	mu    sync.Mutex
	tpool []types.V2Transaction
	state consensus.State
}

func newChainManagerMock() *chainManagerMock {
	return &chainManagerMock{
		state: consensus.State{
			Index: types.ChainIndex{
				Height: 100,
				ID:     types.BlockID{1, 2, 3},
			},
			PrevTimestamps: [11]time.Time{time.Now()},
		},
	}
}

func (cm *chainManagerMock) AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error) {
	cm.mu.Lock()
	cm.tpool = append(cm.tpool, txns...)
	cm.mu.Unlock()
	return false, nil
}

func (cm *chainManagerMock) Block(id types.BlockID) (types.Block, bool) {
	return types.Block{Timestamp: time.Now()}, true
}

func (cm *chainManagerMock) TipState() consensus.State {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.state
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

type syncerMock struct{}

func (s *syncerMock) Peers() []*syncer.Peer {
	return []*syncer.Peer{{}}
}

type walletMock struct {
	mu          sync.Mutex
	broadcasted []types.V2Transaction
}

func (w *walletMock) BroadcastedSets() []types.V2Transaction {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.broadcasted)
}

func (w *walletMock) SplitUTXO(n int, minValue types.Currency) (types.V2Transaction, error) {
	return types.V2Transaction{}, nil
}

func (w *walletMock) Address() types.Address {
	return types.Address{1, 2, 3}
}

func (w *walletMock) BroadcastV2TransactionSet(_ types.ChainIndex, txns []types.V2Transaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.broadcasted = append(w.broadcasted, txns...)
	return nil
}

func (w *walletMock) FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error) {
	return types.ChainIndex{}, nil, nil
}
func (w *walletMock) RecommendedFee() types.Currency {
	return types.ZeroCurrency
}
func (w *walletMock) ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) {}
func (w *walletMock) SignV2Inputs(txn *types.V2Transaction, toSign []int)                  {}

func TestApplyRevertDiff(t *testing.T) {
	contracts := newContractManager(types.PublicKey{}, nil, nil, nil, nil, nil, nil, nil)

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
	revision := fce.V2FileContract
	revision.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Revision:              &revision,
	})
	fce.V2FileContract.RevisionNumber = revision.RevisionNumber
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
	revertDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Revision:              &revision,
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
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}
	walletMock := &walletMock{}
	store := &storeMock{}
	contracts := newContractManager(types.PublicKey{}, amMock, cmMock, store, nil, nil, syncerMock, walletMock)

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
	assert := func(broadcastedTxns, resolutions, pruneCalls, pruneContractSectorsCalls, rejectCalls int) {
		t.Helper()
		if sets := walletMock.BroadcastedSets(); len(sets) != broadcastedTxns {
			t.Fatalf("expected %v broadcasted contracts, got %v", broadcastedTxns, len(sets))
		} else if broadcastedTxns > 0 && len(sets[broadcastedTxns-1].FileContractResolutions) != resolutions {
			t.Fatalf("expected %v contract resolution in broadcast, got %v", resolutions, len(sets[0].FileContracts))
		} else if store.pruneCalls != pruneCalls {
			t.Fatalf("expected %v calls to PruneExpiredContractElements, got %v", pruneCalls, store.pruneCalls)
		} else if store.pruneContractSectorsCalls != pruneContractSectorsCalls {
			t.Fatalf("expected %v calls to PruneExpiredContractElements, got %v", pruneContractSectorsCalls, store.pruneContractSectorsCalls)
		} else if store.rejectCalls != rejectCalls {
			t.Fatalf("expected %v calls to RejectPendingContracts, got %v", rejectCalls, store.rejectCalls)
		}
	}

	// broadcast when no contract should be broadcasted
	if err := contracts.ProcessActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(0, 0, 1, 1, 1)

	// broadcast with 1 contract to broadcast
	store.toBroadcast = []types.V2FileContractElement{contract}
	if err := contracts.ProcessActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(1, 1, 2, 2, 2)
}

func TestApplyRevertRenewedTo(t *testing.T) {
	contracts := newContractManager(types.PublicKey{}, nil, nil, nil, nil, nil, nil, nil)

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

	assertContract := func(fce types.V2FileContractElement, state ContractState, renewedTo *types.FileContractID) {
		t.Helper()
		storedFCE, storedState := mock.Contract(fce.ID)
		storedRenewedTo := mock.RenewedTo(fce.ID)
		if storedState != state {
			t.Fatalf("expected state %v, got %v", state, storedState)
		} else if !reflect.DeepEqual(storedFCE, fce) {
			t.Fatalf("expected contract %v, got %v", fce, storedFCE)
		} else if !reflect.DeepEqual(storedRenewedTo, renewedTo) {
			t.Fatalf("expected renewed to %v, got %v", storedRenewedTo, renewedTo)
		}
	}

	// initial state
	assertContract(fce, ContractStatePending, nil)

	// confirm the contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: fce,
	})
	assertContract(fce, ContractStateActive, nil)

	// resolve contract
	fce.V2FileContract.RevisionNumber++

	newFCE := fce
	newFCE.ID = fce.ID.V2RenewalID()
	newFCE.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2FileContractRenewal{},
	})

	mock.AddContract(newFCE)
	assertContract(newFCE, ContractStatePending, nil)

	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: newFCE,
	})

	assertContract(fce, ContractStateResolved, &newFCE.ID)
	assertContract(newFCE, ContractStateActive, nil)

	// revert resolution
	fce.V2FileContract.RevisionNumber--
	revertDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2FileContractRenewal{},
	})
	revertDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: newFCE,
	})
	assertContract(fce, ContractStateActive, nil)
	assertContract(newFCE, ContractStatePending, nil)

	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2FileContractRenewal{},
	})
	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: newFCE,
	})

	assertContract(fce, ContractStateResolved, &newFCE.ID)
	assertContract(newFCE, ContractStateActive, nil)
}
