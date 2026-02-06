package contracts_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type sqlCurrency types.Currency

func (c sqlCurrency) Value() (driver.Value, error) {
	return types.Currency(c).ExactString(), nil
}

func (c *sqlCurrency) Scan(src any) error {
	switch src := src.(type) {
	case string:
		return (*types.Currency)(c).UnmarshalText([]byte(src))
	case []byte:
		return (*types.Currency)(c).UnmarshalText(src)
	default:
		return fmt.Errorf("cannot scan %T to Currency", src)
	}
}

type sqlHash256 types.Hash256

func (h *sqlHash256) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlHash256{}) {
			return fmt.Errorf("failed to scan source into Hash256 due to invalid number of bytes %v != %v: %v", len(src), len(sqlHash256{}), src)
		}
		copy(h[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Hash256", src)
	}
}

func (h sqlHash256) Value() (driver.Value, error) {
	return h[:], nil
}

type sqlPublicKey types.PublicKey

func (pk sqlPublicKey) Value() (driver.Value, error) {
	return pk[:], nil
}

func (pk *sqlPublicKey) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlPublicKey{}) {
			return fmt.Errorf("failed to scan source into PublicKey due to invalid number of bytes %v != %v: %v", len(src), len(sqlPublicKey{}), src)
		}
		copy(pk[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to PublicKey", src)
	}
}

type mockProofUpdater struct {
	updateFn func(*types.StateElement)
}

func (u *mockProofUpdater) UpdateElementProof(stateElement *types.StateElement) {
	u.updateFn(stateElement)
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

// addTestHost adds a host to the database for testing with optional location.
func (ts testStore) addTestHost(t testing.TB, host hosts.Host) {
	t.Helper()

	if err := ts.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(host.PublicKey, host.Addresses, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	loc := geoip.Location{
		CountryCode: host.CountryCode,
		Latitude:    host.Latitude,
		Longitude:   host.Longitude,
	}
	if err := ts.UpdateHostScan(host.PublicKey, host.Settings, loc, host.Usability.Usable(), time.Now()); err != nil {
		t.Fatal(err)
	}
}

// addTestContract adds a contract to the database for testing and returns its ID.
func (ts testStore) addTestContract(t testing.TB, hk types.PublicKey, good bool, fcid types.FileContractID) types.FileContractID {
	t.Helper()

	revision := newTestRevision(hk)
	err := ts.AddFormedContract(hk, fcid, revision, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), proto.Usage{})
	if err != nil {
		t.Fatal(err)
	}

	if !good {
		_, err := ts.Exec(t.Context(), `UPDATE contracts SET good = false WHERE contract_id = $1`, sqlHash256(fcid))
		if err != nil {
			t.Fatal(err)
		}
	}

	return fcid
}

// setContractSize updates the size of a contract.
func (ts testStore) setContractSize(t testing.TB, fcid types.FileContractID, size uint64) {
	t.Helper()

	// also update capacity to ensure capacity >= size constraint is satisfied
	_, err := ts.Exec(t.Context(), `UPDATE contracts SET size = $1, capacity = GREATEST(capacity, $1) WHERE contract_id = $2`, size, sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractCapacity updates the capacity of a contract.
func (ts testStore) setContractCapacity(t testing.TB, fcid types.FileContractID, capacity uint64) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET capacity = $1 WHERE contract_id = $2`, capacity, sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractRemainingAllowance updates the remaining allowance of a contract.
func (ts testStore) setContractRemainingAllowance(t testing.TB, fcid types.FileContractID, allowance types.Currency) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET remaining_allowance = $1 WHERE contract_id = $2`, sqlCurrency(allowance), sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractProofHeight updates the proof height of a contract.
func (ts testStore) setContractProofHeight(t testing.TB, fcid types.FileContractID, height uint64) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET proof_height = $1 WHERE contract_id = $2`, height, sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractExpirationHeight updates the expiration height of a contract.
func (ts testStore) setContractExpirationHeight(t testing.TB, fcid types.FileContractID, height uint64) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET expiration_height = $1 WHERE contract_id = $2`, height, sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractState updates the state of a contract.
func (ts testStore) setContractState(t testing.TB, fcid types.FileContractID, state contracts.ContractState) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET state = $1 WHERE contract_id = $2`, state, sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractRenewedTo updates the renewed_to field of a contract.
func (ts testStore) setContractRenewedTo(t testing.TB, fcid, renewedTo types.FileContractID) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET renewed_to = $1 WHERE contract_id = $2`, sqlHash256(renewedTo), sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// setContractLastBroadcastAttempt updates the last broadcast attempt of a contract.
func (ts testStore) setContractLastBroadcastAttempt(t testing.TB, fcid types.FileContractID, lastAttempt time.Time) {
	t.Helper()

	_, err := ts.Exec(t.Context(), `UPDATE contracts SET last_broadcast_attempt = $1 WHERE contract_id = $2`, lastAttempt, sqlHash256(fcid))
	if err != nil {
		t.Fatal(err)
	}
}

// addContractElement adds a contract element for a contract. This is needed for broadcasting.
func (ts testStore) addContractElement(t testing.TB, fce types.V2FileContractElement) {
	t.Helper()

	if err := ts.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractElements(fce)
	}); err != nil {
		t.Fatal(err)
	}
}

// setRevisionFilesize sets the filesize in the raw revision of a contract.
func (ts testStore) setRevisionFilesize(t testing.TB, fcid types.FileContractID, filesize uint64) {
	t.Helper()

	// fetch the current revision
	rev, _, err := ts.ContractRevision(fcid)
	if err != nil {
		t.Fatal(err)
	}

	// get current remaining allowance to preserve it
	var allowance types.Currency
	if err := ts.QueryRow(t.Context(), `SELECT remaining_allowance FROM contracts WHERE contract_id = $1`, sqlHash256(fcid)).Scan((*sqlCurrency)(&allowance)); err != nil {
		t.Fatal(err)
	}
	rev.Revision.RenterOutput.Value = allowance

	// update filesize and capacity
	rev.Revision.Filesize = filesize
	if rev.Revision.Capacity < filesize {
		rev.Revision.Capacity = filesize
	}
	rev.Revision.RevisionNumber++

	// save back
	if err := ts.UpdateContractRevision(rev, proto.Usage{}); err != nil {
		t.Fatal(err)
	}
}

// addUnpinnedSectors adds unpinned sectors to the database.
func (ts testStore) addUnpinnedSectors(t testing.TB, hk types.PublicKey, roots []types.Hash256) {
	t.Helper()

	var inserted int64
	for _, root := range roots {
		// add sector to the sectors table associated with the host
		res, err := ts.Exec(t.Context(), `
			INSERT INTO sectors (sector_root, host_id, next_integrity_check)
			SELECT $1, h.id, NOW() + INTERVAL '1 day'
			FROM hosts h WHERE h.public_key = $2
			ON CONFLICT (sector_root) DO NOTHING
		`, sqlHash256(root), sqlPublicKey(hk))
		if err != nil {
			t.Fatal(err)
		}
		inserted += res.RowsAffected()
	}

	// update stats for newly inserted sectors
	if inserted > 0 {
		_, err := ts.Exec(t.Context(), `UPDATE stats SET num_unpinned_sectors = num_unpinned_sectors + $1`, inserted)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ts.Exec(t.Context(), `UPDATE hosts SET unpinned_sectors = unpinned_sectors + $1 WHERE public_key = $2`, inserted, sqlPublicKey(hk))
		if err != nil {
			t.Fatal(err)
		}
	}
}

// getSectorContractID returns the contract ID that a sector is pinned to, or nil if unpinned.
func (ts testStore) getSectorContractID(t testing.TB, root types.Hash256) *types.FileContractID {
	t.Helper()

	var fcid types.FileContractID
	err := ts.QueryRow(t.Context(), `
		SELECT csm.contract_id
		FROM sectors s
		INNER JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
		WHERE s.sector_root = $1
	`, sqlHash256(root)).Scan((*sqlHash256)(&fcid))
	if err != nil {
		return nil // not pinned or not found
	}
	return &fcid
}

// addPinnedSectors adds sectors to a host and pins them to a specific contract.
func (ts testStore) addPinnedSectors(t testing.TB, hk types.PublicKey, fcid types.FileContractID, roots []types.Hash256) {
	t.Helper()

	// first add as unpinned
	ts.addUnpinnedSectors(t, hk, roots)

	// then pin to the contract
	if err := ts.PinSectors(fcid, roots); err != nil {
		t.Fatal(err)
	}
}

// resetNextFund resets the next_fund timestamp for all account hosts.
func (ts testStore) resetNextFund(t testing.TB) {
	t.Helper()
	if _, err := ts.Exec(t.Context(), `UPDATE account_hosts SET next_fund = NOW()`); err != nil {
		t.Fatal(err)
	}
}

// hostAccounts returns all host accounts with their next_fund timestamps.
func (ts testStore) hostAccounts(t testing.TB) (result []accounts.HostAccount) {
	t.Helper()

	rows, err := ts.Query(t.Context(), `SELECT next_fund FROM account_hosts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var account accounts.HostAccount
		if err := rows.Scan(&account.NextFund); err != nil {
			t.Fatal(err)
		}
		result = append(result, account)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return
}

// scheduleContractsForPruningHelper marks all contracts as ready for pruning (test helper).
func (ts testStore) scheduleContractsForPruningHelper(t testing.TB) {
	t.Helper()

	// set next_prune to past so ContractsForPruning (next_prune < NOW()) returns them
	_, err := ts.Exec(t.Context(), `UPDATE contracts SET next_prune = NOW() - INTERVAL '1 hour' WHERE good = TRUE AND state IN (0, 1)`)
	if err != nil {
		t.Fatal(err)
	}
}

// setActiveAccountsCount inserts dummy accounts to simulate having n active accounts.
func (ts testStore) setActiveAccountsCount(t testing.TB, n uint64) {
	t.Helper()

	// first clear existing accounts
	_, err := ts.Exec(t.Context(), `DELETE FROM accounts`)
	if err != nil {
		t.Fatal(err)
	}

	// create a connect key for the dummy accounts
	var connectKeyID int64
	err = ts.QueryRow(t.Context(), `
		INSERT INTO app_connect_keys (app_key, user_secret, use_description, remaining_uses, max_pinned_data)
		VALUES ('test-connect-key', $1, 'test connect key', 0, 1000000000000)
		ON CONFLICT (app_key) DO UPDATE SET app_key = EXCLUDED.app_key
		RETURNING id
	`, frand.Bytes(32)).Scan(&connectKeyID)
	if err != nil {
		t.Fatal(err)
	}

	// insert n dummy accounts
	for i := uint64(0); i < n; i++ {
		pk := types.PublicKey{byte(i % 256), byte(i / 256)}
		_, err := ts.Exec(t.Context(), `
			INSERT INTO accounts (public_key, connect_key_id, last_used, pinned_data, max_pinned_data)
			VALUES ($1, $2, NOW(), 0, 0)
		`, sqlPublicKey(pk), connectKeyID)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// mockUpdateTx is a mocked implementation of UpdateTx which allows for unit
// testing the contract manager's chain updates without a full database.
type mockUpdateTx struct {
	contracts map[types.FileContractID]types.V2FileContractElement
	state     map[types.FileContractID]contracts.ContractState
	renewedTo map[types.FileContractID]*types.FileContractID
}

// newMockUpdateTx creates a new mock UpdateTx.
func newMockUpdateTx() *mockUpdateTx {
	return &mockUpdateTx{
		contracts: make(map[types.FileContractID]types.V2FileContractElement),
		state:     make(map[types.FileContractID]contracts.ContractState),
		renewedTo: make(map[types.FileContractID]*types.FileContractID),
	}
}

func (tx *mockUpdateTx) AddContract(fce types.V2FileContractElement) {
	tx.contracts[fce.ID] = fce
	tx.state[fce.ID] = contracts.ContractStatePending
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

func (tx *mockUpdateTx) DeleteContractElements(contractIDs ...types.FileContractID) error {
	for _, contractID := range contractIDs {
		delete(tx.contracts, contractID)
	}
	return nil
}

func (tx *mockUpdateTx) Contract(contractID types.FileContractID) (types.V2FileContractElement, contracts.ContractState) {
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

func (tx *mockUpdateTx) ContractState(contractID types.FileContractID) contracts.ContractState {
	state, ok := tx.state[contractID]
	if !ok {
		panic("contract state not found")
	}
	return state
}

func (tx *mockUpdateTx) IsKnownContract(contractID types.FileContractID) (bool, error) {
	_, ok := tx.state[contractID]
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

func (tx *mockUpdateTx) UpdateContractState(contractID types.FileContractID, state contracts.ContractState) error {
	// checking state, not contract since resolved elements are removed from the map
	if _, ok := tx.state[contractID]; !ok {
		return errors.New("contract not found")
	}
	tx.state[contractID] = state
	return nil
}

func (tx *mockUpdateTx) UpdateContractRenewedTo(contractID types.FileContractID, renewedTo *types.FileContractID) error {
	// checking state, not contract since resolved elements are removed from the map
	if _, ok := tx.state[contractID]; !ok {
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
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, nil, nil, nil, nil, nil)

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
	updateTx := contracts.NewTestUpdateTx(mock)

	// helper to apply/revert diff
	applyDiff := func(diff consensus.V2FileContractElementDiff) {
		t.Helper()
		err := cm.ApplyV2ContractDiffs(updateTx, []consensus.V2FileContractElementDiff{diff})
		if err != nil {
			t.Fatal(err)
		}
	}
	revertDiff := func(diff consensus.V2FileContractElementDiff) {
		t.Helper()
		err := cm.RevertV2ContractDiffs(updateTx, []consensus.V2FileContractElementDiff{diff})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertContractState := func(state contracts.ContractState) {
		t.Helper()
		storedState := mock.ContractState(contractID)
		if storedState != state {
			t.Fatalf("expected state %v, got %v", state, storedState)
		}
	}

	assertContract := func(state contracts.ContractState) {
		t.Helper()
		storedFCE, storedState := mock.Contract(contractID)
		if storedState != state {
			t.Fatalf("expected state %v, got %v", state, storedState)
		} else if !reflect.DeepEqual(storedFCE, fce) {
			t.Fatalf("expected contract %v, got %v", fce, storedFCE)
		}
	}

	// initial state
	assertContract(contracts.ContractStatePending)

	// confirm the contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: fce,
	})
	assertContract(contracts.ContractStateActive)

	// revise contract
	revision := fce.V2FileContract
	revision.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Revision:              &revision,
	})
	fce.V2FileContract.RevisionNumber = revision.RevisionNumber
	assertContract(contracts.ContractStateActive)

	// resolve contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2StorageProof{},
	})
	assertContractState(contracts.ContractStateResolved)

	// revert resolution
	fce.V2FileContract.RevisionNumber--
	revertDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2StorageProof{},
	})
	assertContract(contracts.ContractStateActive)

	// revert revision
	fce.V2FileContract.RevisionNumber--
	revertDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Revision:              &revision,
	})
	assertContract(contracts.ContractStateActive)

	// revert contract
	fce.V2FileContract.RevisionNumber--
	revertDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: fce,
	})
	assertContractState(contracts.ContractStatePending)
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
	updateTx := contracts.NewTestUpdateTx(mock)

	// check initial contract
	if fce, _ := mock.Contract(contract.ID); !reflect.DeepEqual(fce, contract) {
		t.Fatalf("mismatch \n%+v\n%+v", fce, contract)
	}

	// update proof on contract
	contract2 := contract
	contract2.StateElement.MerkleProof = []types.Hash256{{2}}
	contracts.UpdateContractElementProofs(updateTx, &mockProofUpdater{updateFn: func(stateElement *types.StateElement) {
		stateElement.MerkleProof = contract2.StateElement.MerkleProof
	}})
	if fce, _ := mock.Contract(contract2.ID); !reflect.DeepEqual(fce, contract2) {
		t.Fatalf("mismatch \n%+v\n%+v", fce, contract2)
	}
}

func TestProcessActions(t *testing.T) {
	store := newTestStore(t)
	amMock := &accountsManagerMock{}
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}
	walletMock := &walletMock{}
	cm := contracts.NewTestContractManager(types.PublicKey{}, amMock, nil, cmMock, store, nil, nil, syncerMock, walletMock)

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
	assert := func(broadcastedTxns, resolutions int) {
		t.Helper()
		if sets := walletMock.BroadcastedSets(); len(sets) != broadcastedTxns {
			t.Fatalf("expected %v broadcasted contracts, got %v", broadcastedTxns, len(sets))
		} else if broadcastedTxns > 0 && len(sets[broadcastedTxns-1].FileContractResolutions) != resolutions {
			t.Fatalf("expected %v contract resolution in broadcast, got %v", resolutions, len(sets[0].FileContracts))
		}
	}

	// broadcast when no contract should be broadcasted
	if err := cm.ProcessActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(0, 0)

	// add a host
	hk := types.PublicKey{1, 2, 3}
	store.addTestHost(t, hosts.Host{PublicKey: hk})

	// add a contract
	store.addTestContract(t, hk, true, contract.ID)

	// set expiration height to match contract element
	store.setContractExpirationHeight(t, contract.ID, contract.V2FileContract.ExpirationHeight)

	// add contract element and set state to active
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return errors.Join(
			tx.UpdateContractElements(contract),
			tx.UpdateContractState(contract.ID, contracts.ContractStateActive),
		)
	}); err != nil {
		t.Fatal(err)
	}

	// set height past expiration + broadcast buffer (default 144)
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(types.ChainIndex{Height: contract.V2FileContract.ExpirationHeight + 144})
	}); err != nil {
		t.Fatal(err)
	}

	// broadcast with 1 contract to broadcast
	if err := cm.ProcessActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(1, 1)
}

func TestApplyRevertRenewedTo(t *testing.T) {
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, nil, nil, nil, nil, nil)

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
	updateTx := contracts.NewTestUpdateTx(mock)

	// helper to apply/revert diff
	applyDiff := func(diff consensus.V2FileContractElementDiff) {
		t.Helper()
		err := cm.ApplyV2ContractDiffs(updateTx, []consensus.V2FileContractElementDiff{diff})
		if err != nil {
			t.Fatal(err)
		}
	}
	revertDiff := func(diff consensus.V2FileContractElementDiff) {
		t.Helper()
		err := cm.RevertV2ContractDiffs(updateTx, []consensus.V2FileContractElementDiff{diff})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertContractState := func(contractID types.FileContractID, state contracts.ContractState, renewedTo *types.FileContractID) {
		t.Helper()
		storedState := mock.ContractState(contractID)
		if storedState != state {
			t.Fatalf("expected state %v, got %v", state, storedState)
		}
		storedRenewedTo := mock.RenewedTo(contractID)
		if !reflect.DeepEqual(storedRenewedTo, renewedTo) {
			t.Fatalf("expected renewed to %v, got %v", renewedTo, storedRenewedTo)
		}
	}

	assertContract := func(fce types.V2FileContractElement, state contracts.ContractState, renewedTo *types.FileContractID) {
		t.Helper()
		storedFCE, _ := mock.Contract(fce.ID)
		if !reflect.DeepEqual(storedFCE, fce) {
			t.Fatalf("expected contract %v, got %v", fce, storedFCE)
		}
		assertContractState(fce.ID, state, renewedTo)
	}

	// initial state
	assertContract(fce, contracts.ContractStatePending, nil)

	// confirm the contract
	fce.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: fce,
	})
	assertContract(fce, contracts.ContractStateActive, nil)

	// resolve contract
	fce.V2FileContract.RevisionNumber++

	newFCE := fce
	newFCE.ID = fce.ID.V2RenewalID()
	newFCE.V2FileContract.RevisionNumber++
	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2FileContractRenewal{},
	})
	assertContractState(fce.ID, contracts.ContractStateResolved, &newFCE.ID)

	mock.AddContract(newFCE)
	assertContract(newFCE, contracts.ContractStatePending, nil)

	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: newFCE,
	})

	assertContractState(fce.ID, contracts.ContractStateResolved, &newFCE.ID)
	assertContract(newFCE, contracts.ContractStateActive, nil)

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
	assertContract(fce, contracts.ContractStateActive, nil)
	assertContractState(newFCE.ID, contracts.ContractStatePending, nil)

	applyDiff(consensus.V2FileContractElementDiff{
		V2FileContractElement: fce,
		Resolution:            &types.V2FileContractRenewal{},
	})
	applyDiff(consensus.V2FileContractElementDiff{
		Created:               true,
		V2FileContractElement: newFCE,
	})

	assertContractState(fce.ID, contracts.ContractStateResolved, &newFCE.ID)
	assertContract(newFCE, contracts.ContractStateActive, nil)
}
