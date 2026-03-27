package slabs_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

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

var goodSettings = proto.HostSettings{
	AcceptingContracts: true,
	RemainingStorage:   math.MaxUint32,
	Prices: proto.HostPrices{
		ContractPrice: types.Siacoins(1),
		Collateral:    types.NewCurrency64(10),
		StoragePrice:  types.NewCurrency64(1),
		EgressPrice:   types.NewCurrency64(1),
		ValidUntil:    time.Now().Add(100 * 24 * time.Hour),
	},
	MaxContractDuration: 90 * 144,
	MaxCollateral:       types.Siacoins(1000),
}

type mockChainManager struct {
	mu  sync.Mutex
	tip types.ChainIndex
}

func (m *mockChainManager) AdvanceToHeight(height uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tip.Height = height
	m.tip.ID = frand.Entropy256()
}

func (m *mockChainManager) Tip() types.ChainIndex {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tip
}

func newMockChainManager() *mockChainManager {
	return &mockChainManager{
		tip: types.ChainIndex{
			Height: 0,
			ID:     frand.Entropy256(),
		},
	}
}

type testStore struct {
	testutils.TestStore
}

func newMockStore(t testing.TB) *testStore {
	store := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	// override global settings to ensure hosts are always usable
	if err := store.UpdateUsabilitySettings(hosts.UsabilitySettings{
		MaxEgressPrice:     types.MaxCurrency,
		MaxIngressPrice:    types.MaxCurrency,
		MaxStoragePrice:    types.MaxCurrency,
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: [3]byte{0, 0, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateMaintenanceSettings(contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	return &testStore{
		TestStore: store,
	}
}

func (s *testStore) setSectorsForCheck(t testing.TB, hk types.PublicKey, roots []types.Hash256) {
	s.AddTestHost(t, hosts.Host{PublicKey: hk, Settings: goodSettings, Usability: hosts.GoodUsability})
	for _, root := range roots {
		_, err := s.Exec(context.Background(), `
            INSERT INTO sectors (sector_root, host_id, next_integrity_check, uploaded_at)
            SELECT $1, id, $3, NOW()
            FROM hosts WHERE public_key = $2
            ON CONFLICT (sector_root) DO UPDATE SET next_integrity_check = $3
        `, sqlHash256(root), hk[:], time.Now().Add(-24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
	}

	if _, err := s.Exec(context.Background(), "UPDATE stats SET stat_value = stat_value + $1 WHERE stat_name = 'num_unpinned_sectors'", len(roots)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(context.Background(), "UPDATE hosts SET unpinned_sectors = unpinned_sectors + $1 WHERE public_key = $2", len(roots), hk[:]); err != nil {
		t.Fatal(err)
	}
}

func (s *testStore) failedChecks(t testing.TB, hk types.PublicKey) map[types.Hash256]int {
	rows, err := s.Query(context.Background(), `
        SELECT sector_root, consecutive_failed_checks
        FROM sectors s
        JOIN hosts h ON s.host_id = h.id
        WHERE h.public_key = $1
    `, hk[:])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	result := make(map[types.Hash256]int)
	for rows.Next() {
		var root types.Hash256
		var count int
		if err := rows.Scan((*sqlHash256)(&root), &count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			result[root] = count
		}
	}
	return result
}

func (s *testStore) lostSectors(t testing.TB) map[types.Hash256]struct{} {
	rows, err := s.Query(context.Background(), `
		SELECT sector_root FROM sectors WHERE host_id IS NULL AND contract_sectors_map_id IS NULL
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	result := make(map[types.Hash256]struct{})
	for rows.Next() {
		var root types.Hash256
		if err := rows.Scan((*sqlHash256)(&root)); err != nil {
			t.Fatal(err)
		}
		result[root] = struct{}{}
	}
	return result
}

func (s *testStore) migratedSectors(t testing.TB, hk types.PublicKey) map[types.Hash256]struct{} {
	rows, err := s.Query(context.Background(), `
        SELECT sector_root FROM sectors s
        JOIN hosts h ON s.host_id = h.id
        WHERE h.public_key = $1 AND s.num_migrated > 0
    `, hk[:])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	result := make(map[types.Hash256]struct{})
	for rows.Next() {
		var root types.Hash256
		if err := rows.Scan((*sqlHash256)(&root)); err != nil {
			t.Fatal(err)
		}
		result[root] = struct{}{}
	}
	return result
}

func (s *testStore) addTestContract(t testing.TB, hk types.PublicKey) {
	t.Helper()

	rev := types.V2FileContract{
		HostPublicKey:    hk,
		Capacity:         math.MaxInt64,
		Filesize:         0,
		FileMerkleRoot:   types.Hash256{},
		ProofHeight:      1000,
		ExpirationHeight: 2000,
		RevisionNumber:   1,
		TotalCollateral:  types.Siacoins(100),
	}
	err := s.AddFormedContract(hk, types.FileContractID(hk), rev, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{})
	if err != nil {
		t.Fatal(err)
	}
}

func (s *testStore) pinSectorToContract(t testing.TB, root types.Hash256, fcid types.FileContractID) {
	t.Helper()
	var hostID int64
	err := s.QueryRow(context.Background(), `
        UPDATE sectors SET contract_sectors_map_id = (SELECT id FROM contract_sectors_map WHERE contract_id = $2)
        WHERE sector_root = $1
        RETURNING host_id
    `, root[:], fcid[:]).Scan(&hostID)
	if err != nil {
		t.Fatalf("failed to pin sector %x to contract %s: %v", root, fcid, err)
	}
	if _, err := s.Exec(context.Background(), "UPDATE stats SET stat_value = stat_value + 1 WHERE stat_name = 'num_pinned_sectors'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(context.Background(), "UPDATE stats SET stat_value = stat_value - 1 WHERE stat_name = 'num_unpinned_sectors'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(context.Background(), "UPDATE hosts SET unpinned_sectors = unpinned_sectors - 1 WHERE id = $1", hostID); err != nil {
		t.Fatal(err)
	}
}

type mockAccountManager struct {
	mu              sync.Mutex
	serviceAccounts map[proto.Account]map[types.PublicKey]types.Currency
}

func newMockAccountManager() *mockAccountManager {
	return &mockAccountManager{
		serviceAccounts: make(map[proto.Account]map[types.PublicKey]types.Currency),
	}
}

func (m *mockAccountManager) RegisterServiceAccount(account proto.Account) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.serviceAccounts[account]; !ok {
		m.serviceAccounts[account] = make(map[types.PublicKey]types.Currency)
	}
}

func (m *mockAccountManager) ResetAccountBalance(hostKey types.PublicKey, account proto.Account) error {
	return m.UpdateServiceAccountBalance(hostKey, account, types.ZeroCurrency)
}

func (m *mockAccountManager) ServiceAccountBalance(hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hostAccounts, ok := m.serviceAccounts[account]; ok {
		if balance, ok := hostAccounts[hostKey]; ok {
			return balance, nil
		}
		return types.ZeroCurrency, nil
	}
	return types.ZeroCurrency, nil
}

func (m *mockAccountManager) UpdateServiceAccountBalance(hostKey types.PublicKey, account proto.Account, balance types.Currency) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	hostAccounts, ok := m.serviceAccounts[account]
	if !ok {
		hostAccounts = make(map[types.PublicKey]types.Currency)
		m.serviceAccounts[account] = hostAccounts
	}
	hostAccounts[hostKey] = balance
	return nil
}

func (m *mockAccountManager) DebitServiceAccount(hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hostAccounts, ok := m.serviceAccounts[account]; ok {
		if balance, ok := hostAccounts[hostKey]; ok {
			if balance.Cmp(amount) < 0 {
				hostAccounts[hostKey] = types.ZeroCurrency
			} else {
				hostAccounts[hostKey] = balance.Sub(amount)
			}
		}
	}
	return nil
}

type mockContractManager struct {
	mu               sync.Mutex
	contracts        []contracts.Contract
	triggeredRefills map[proto.Account]int
}

func newMockContractManager() *mockContractManager {
	return &mockContractManager{
		triggeredRefills: make(map[proto.Account]int),
	}
}

func (m *mockContractManager) TriggerAccountRefill(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggeredRefills[account]++
	return nil
}

func (m *mockContractManager) ContractsForAppend() ([]contracts.Contract, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.contracts), nil
}

type mockhostManager struct {
	mu            sync.Mutex
	hosts         map[types.PublicKey]hosts.Host
	unusable      map[types.PublicKey]struct{}
	refreshPrices bool
}

func newMockHostManager() *mockhostManager {
	return &mockhostManager{
		hosts:    make(map[types.PublicKey]hosts.Host),
		unusable: make(map[types.PublicKey]struct{}),
	}
}

func (mock *mockhostManager) Usable(ctx context.Context, hk types.PublicKey) (bool, error) {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	_, ok := mock.unusable[hk]
	return !ok, nil
}

func (mock *mockhostManager) WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error {
	mock.mu.Lock()
	host, ok := mock.hosts[hk]
	mock.mu.Unlock()
	if !ok {
		return hosts.ErrNotFound
	} else if !host.IsGood() {
		return hosts.ErrBadHost
	}
	err := fn(host)
	if mock.refreshPrices {
		host.Settings.Prices.ValidUntil = time.Now().Add(time.Hour)
		mock.hosts[hk] = host
	}
	return err
}

// A mockHostClient is a mock implementation of hosts.HostClient for testing.
type mockHostClient struct {
	mu              sync.Mutex
	hostSectors     map[types.PublicKey]map[types.Hash256][proto.SectorSize]byte
	slowHosts       map[types.PublicKey]time.Duration
	integrityErrors map[types.Hash256]error
	readHooks       map[types.Hash256]func()
	hostKeys        map[types.PublicKey]types.PrivateKey
	hostSettings    map[types.PublicKey]proto.HostSettings
	unusable        map[types.PublicKey]struct{}
}

func (m *mockHostClient) resetStorage() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hostSectors = make(map[types.PublicKey]map[types.Hash256][proto.SectorSize]byte)
}

func (m *mockHostClient) addTestHost(sk types.PrivateKey) hosts.Host {
	m.mu.Lock()
	defer m.mu.Unlock()

	h := newTestHost(sk.PublicKey())
	m.hostSettings[sk.PublicKey()] = h.Settings
	m.hostKeys[sk.PublicKey()] = sk
	return h
}

// Prices is a mock implementation that returns the preset host settings.
func (m *mockHostClient) Prices(ctx context.Context, hostKey types.PublicKey) (proto.HostPrices, error) {
	m.mu.Lock()
	delay := m.slowHosts[hostKey]
	m.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return proto.HostPrices{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	prices := m.hostSettings[hostKey].Prices
	prices.ValidUntil = time.Now().Add(1 * time.Hour)
	prices.Signature = m.hostKeys[hostKey].SignHash(prices.SigHash())
	return prices, nil
}

// WriteSector is a mock implementation that writes a sector to the mock host.
func (m *mockHostClient) WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (rhp.RPCWriteSectorResult, error) {
	m.mu.Lock()
	delay := m.slowHosts[hostKey]
	m.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return rhp.RPCWriteSectorResult{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	var sector [proto.SectorSize]byte
	copy(sector[:], data)
	root := proto.SectorRoot(&sector)

	usage := m.hostSettings[hostKey].Prices.RPCWriteSectorCost(uint64(len(data)))
	m.mu.Lock()
	if _, ok := m.hostSectors[hostKey]; !ok {
		m.hostSectors[hostKey] = make(map[types.Hash256][proto.SectorSize]byte)
	}
	m.hostSectors[hostKey][root] = sector
	m.mu.Unlock()
	return rhp.RPCWriteSectorResult{
		Root:  root,
		Usage: usage,
	}, nil
}

// ReadSector is a mock implementation that reads a sector from the mock host.
func (m *mockHostClient) ReadSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, root types.Hash256, w io.Writer, offset, length uint64) (rhp.RPCReadSectorResult, error) {
	m.mu.Lock()
	sector, ok := m.hostSectors[hostKey][root]
	if !ok {
		m.mu.Unlock()
		return rhp.RPCReadSectorResult{}, proto.ErrSectorNotFound
	}
	hook, ok := m.readHooks[root]
	if ok {
		hook()
	}
	err, ok := m.integrityErrors[root]
	if ok && err != nil {
		m.mu.Unlock()
		return rhp.RPCReadSectorResult{}, err
	}
	delay := m.slowHosts[hostKey]
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return rhp.RPCReadSectorResult{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	if _, err := w.Write(sector[offset : offset+length]); err != nil {
		return rhp.RPCReadSectorResult{}, err
	}
	usage := m.hostSettings[hostKey].Prices.RPCReadSectorCost(length)
	return rhp.RPCReadSectorResult{
		Usage: usage,
	}, nil
}

// Prioritize is a mock implementation that returns the hosts with
// unusable hosts filtered out.
func (m *mockHostClient) Prioritize(hosts []types.PublicKey) []types.PublicKey {
	m.mu.Lock()
	defer m.mu.Unlock()

	hosts = slices.Clone(hosts)
	filtered := hosts[:0]
	for _, hk := range hosts {
		if _, ok := m.unusable[hk]; !ok {
			filtered = append(filtered, hk)
		}
	}

	return filtered
}

func (m *mockHostClient) setSlowHost(hostKey types.PublicKey, delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if delay == 0 {
		delete(m.slowHosts, hostKey)
	} else {
		m.slowHosts[hostKey] = delay
	}
}

func newMockHostClient() *mockHostClient {
	return &mockHostClient{
		hostSectors:     make(map[types.PublicKey]map[types.Hash256][proto.SectorSize]byte),
		slowHosts:       make(map[types.PublicKey]time.Duration),
		integrityErrors: make(map[types.Hash256]error),
		readHooks:       make(map[types.Hash256]func()),
		hostKeys:        make(map[types.PublicKey]types.PrivateKey),
		hostSettings:    make(map[types.PublicKey]proto.HostSettings),
		unusable:        make(map[types.PublicKey]struct{}),
	}
}
