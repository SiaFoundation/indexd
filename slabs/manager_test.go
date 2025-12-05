package slabs

import (
	"context"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"lukechampine.com/frand"
)

var goodSettings = proto.HostSettings{
	AcceptingContracts: true,
	RemainingStorage:   math.MaxUint32,
	Prices: proto.HostPrices{
		ContractPrice: types.Siacoins(1),
		Collateral:    types.NewCurrency64(1),
		StoragePrice:  types.NewCurrency64(1),
		EgressPrice:   types.NewCurrency64(1),
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

type mockStore struct {
	mu              sync.Mutex
	accounts        map[proto.Account]struct{}
	contracts       map[types.PublicKey]contracts.Contract
	failedChecks    map[types.PublicKey]map[types.Hash256]int
	hosts           map[types.PublicKey]hosts.Host
	lostSectors     map[types.PublicKey]map[types.Hash256]struct{}
	migratedSectors map[types.PublicKey]map[types.Hash256]struct{}
	pinnedSlabs     map[proto.Account]map[SlabID]Slab
	sectorsForCheck []types.Hash256
	serviceAccounts map[proto.Account]map[types.PublicKey]types.Currency
}

func newMockStore() *mockStore {
	return &mockStore{
		accounts:        make(map[proto.Account]struct{}),
		contracts:       make(map[types.PublicKey]contracts.Contract),
		failedChecks:    make(map[types.PublicKey]map[types.Hash256]int),
		hosts:           make(map[types.PublicKey]hosts.Host),
		lostSectors:     make(map[types.PublicKey]map[types.Hash256]struct{}),
		migratedSectors: make(map[types.PublicKey]map[types.Hash256]struct{}),
		pinnedSlabs:     make(map[proto.Account]map[SlabID]Slab),
		serviceAccounts: make(map[proto.Account]map[types.PublicKey]types.Currency),
	}
}

func (s *mockStore) SharedObject(key types.Hash256) (SharedObject, error) {
	panic("not implemented")
}

func (s *mockStore) AddAccount(account types.PublicKey, meta accounts.AppMeta, opts ...accounts.AddAccountOption) error {
	s.accounts[proto.Account(account)] = struct{}{}
	return nil
}

func (s *mockStore) AddServiceAccount(account types.PublicKey, meta accounts.AppMeta, opts ...accounts.AddAccountOption) error {
	s.accounts[proto.Account(account)] = struct{}{}
	return nil
}

func (s *mockStore) Contracts(offset, limit int, opts ...contracts.ContractQueryOpt) ([]contracts.Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	opt := contracts.ContractQueryOpts{}
	for _, o := range opts {
		o(&opt)
	}

	var contracts []contracts.Contract
	for _, c := range s.contracts {
		if opt.Good != nil {
			if *opt.Good != c.Good {
				continue
			}
		}
		// NOTE: currently ignores revisable filter
		contracts = append(contracts, c)
	}
	return contracts, nil
}

func (s *mockStore) FailingSectors(hostKey types.PublicKey, minChecks, limit int) ([]types.Hash256, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var roots []types.Hash256
	for root, failures := range s.failedChecks[hostKey] {
		if failures >= minChecks {
			roots = append(roots, root)
		}
	}
	return roots, nil
}

func (s *mockStore) Hosts(offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	opt := hosts.DefaultHostsQueryOpts
	for _, o := range queryOpts {
		o(&opt)
	}

	var hosts []hosts.Host
	for _, h := range s.hosts {
		if opt.Usable != nil {
			if *opt.Usable != h.Usability.Usable() {
				continue
			}
		}
		// NOTE: currently ignores blocked filter
		hosts = append(hosts, h)
	}

	return hosts, nil
}

func (s *mockStore) HostsForIntegrityChecks(maxLastCheck time.Time, limit int) (result []types.PublicKey, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil, nil
}

func (s *mockStore) HostsWithLostSectors() (hks []types.PublicKey, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hk, lostSectors := range s.lostSectors {
		if len(lostSectors) > 0 {
			hks = append(hks, hk)
		}
	}
	return
}

func (s *mockStore) MaintenanceSettings() (contracts.MaintenanceSettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return contracts.DefaultMaintenanceSettings, nil
}

func (s *mockStore) MarkFailingSectorsLost(hostKey types.PublicKey, maxFailedIntegrityChecks uint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for root, failures := range s.failedChecks[hostKey] {
		if failures >= int(maxFailedIntegrityChecks) {
			s.lostSectors[hostKey][root] = struct{}{}
		}
	}
	return nil
}

func (s *mockStore) MarkSectorsLost(hostKey types.PublicKey, roots []types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.lostSectors[hostKey]; !ok {
		s.lostSectors[hostKey] = make(map[types.Hash256]struct{})
	}
	for _, root := range roots {
		s.lostSectors[hostKey][root] = struct{}{}
	}
	return nil
}

func (s *mockStore) MarkSlabRepaired(slabID SlabID, success bool) error {
	return nil
}

func (s *mockStore) MigrateSector(root types.Hash256, hostKey types.PublicKey) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.migratedSectors[hostKey]
	if !ok {
		s.migratedSectors[hostKey] = make(map[types.Hash256]struct{})
	}
	s.migratedSectors[hostKey][root] = struct{}{}

	for acc := range s.accounts {
		for slabID, slab := range s.pinnedSlabs[acc] {
			for i, sector := range slab.Sectors {
				if sector.Root == root {
					s.pinnedSlabs[acc][slabID].Sectors[i].HostKey = &hostKey
					s.pinnedSlabs[acc][slabID].Sectors[i].ContractID = nil
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (s *mockStore) PinSlabs(account proto.Account, nextIntegrityCheck time.Time, slabs ...SlabPinParams) ([]SlabID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var digests []SlabID
	for _, slab := range slabs {
		slabID := slab.Digest()
		digests = append(digests, slabID)

		sectors := make([]Sector, 0, len(slab.Sectors))
		for _, ss := range slab.Sectors {
			contract, ok := s.contracts[ss.HostKey]
			if !ok {
				sectors = append(sectors, Sector{Root: ss.Root})
				continue
			}
			sectors = append(sectors, Sector{
				Root:       ss.Root,
				ContractID: &contract.ID,
				HostKey:    &contract.HostKey,
			})
		}

		_, ok := s.pinnedSlabs[account]
		if !ok {
			s.pinnedSlabs[account] = make(map[SlabID]Slab)
		}
		s.pinnedSlabs[account][slabID] = Slab{
			ID:            slabID,
			EncryptionKey: slab.EncryptionKey,
			MinShards:     slab.MinShards,
			Sectors:       sectors,
		}
	}
	return digests, nil
}

func (s *mockStore) UnpinSlab(account proto.Account, slabID SlabID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pinnedSlabs[account][slabID]; !ok {
		return ErrSlabNotFound
	}
	delete(s.pinnedSlabs[account], slabID)
	return nil
}

func (s *mockStore) PinnedSlab(account proto.Account, slabID SlabID) (PinnedSlab, error) {
	return PinnedSlab{}, nil
}

func (s *mockStore) SlabIDs(account proto.Account, offset, limit int) ([]SlabID, error) {
	return nil, nil
}

func (s *mockStore) RecordIntegrityCheck(success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.failedChecks[hostKey]; !ok {
		s.failedChecks[hostKey] = make(map[types.Hash256]int)
	}
	for _, root := range roots {
		if success {
			s.failedChecks[hostKey][root] = 0
		} else {
			s.failedChecks[hostKey][root]++
		}
	}
	return nil
}

func (s *mockStore) SectorsForIntegrityCheck(hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sectorsForCheck), nil
}

func (s *mockStore) Slab(slabID SlabID) (Slab, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for acc := range s.accounts {
		if slab, ok := s.pinnedSlabs[acc][slabID]; ok {
			return slab, nil
		}
	}
	return Slab{}, ErrSlabNotFound
}

func (s *mockStore) Slabs(accountID proto.Account, slabIDs []SlabID) ([]Slab, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var slabs []Slab
	for _, slab := range s.pinnedSlabs[accountID] {
		slabs = append(slabs, slab)
	}
	return slabs, nil
}

func (s *mockStore) UnhealthySlabs(limit int) (result []SlabID, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for acc := range s.accounts {
		for _, slab := range s.pinnedSlabs[acc] {
			for _, sector := range slab.Sectors {
				if len(result) >= limit {
					break
				}

				// a slab is unhealthy if any of its sectors are not pinned
				// or are pinned to a bad contract
				if sector.HostKey == nil {
					result = append(result, slab.ID)
				} else if contract, ok := s.contracts[*sector.HostKey]; ok && !contract.Good {
					result = append(result, slab.ID)
				}
			}
		}
	}
	return result, nil
}

func (s *mockStore) PruneSlabs(account proto.Account) error {
	return nil
}

func (s *mockStore) Object(account proto.Account, key types.Hash256) (SealedObject, error) {
	return SealedObject{}, nil
}

func (s *mockStore) DeleteObject(account proto.Account, objectKey types.Hash256) error {
	return nil
}

func (s *mockStore) SaveObject(account proto.Account, obj SealedObject) error {
	return nil
}

func (s *mockStore) ListObjects(account proto.Account, cursor Cursor, limit int) ([]ObjectEvent, error) {
	return nil, nil
}

type mockAccountManager struct {
	mu              sync.Mutex
	serviceAccounts map[proto.Account]struct{}
	store           *mockStore
}

func newMockAccountManager(store *mockStore) *mockAccountManager {
	return &mockAccountManager{
		serviceAccounts: make(map[proto.Account]struct{}),
		store:           store,
	}
}

func (m *mockAccountManager) RegisterServiceAccount(account proto.Account) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.serviceAccounts[account] = struct{}{}
}

func (m *mockAccountManager) ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	return m.UpdateServiceAccountBalance(ctx, hostKey, account, types.ZeroCurrency)
}

func (m *mockAccountManager) ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hostAccounts, ok := m.store.serviceAccounts[account]; ok {
		if balance, ok := hostAccounts[hostKey]; ok {
			return balance, nil
		}
		return types.ZeroCurrency, nil
	}
	return types.ZeroCurrency, nil
}

func (m *mockAccountManager) UpdateServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account, balance types.Currency) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	hostAccounts, ok := m.store.serviceAccounts[account]
	if !ok {
		hostAccounts = make(map[types.PublicKey]types.Currency)
		m.store.serviceAccounts[account] = hostAccounts
	}
	hostAccounts[hostKey] = balance
	return nil
}

func (m *mockAccountManager) DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hostAccounts, ok := m.store.serviceAccounts[account]; ok {
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
	mu       sync.Mutex
	hosts    map[types.PublicKey]hosts.Host
	unusable map[types.PublicKey]struct{}

	refreshPrices bool // reset prices.ValidUntil after each call to WithScannedHost
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
func (m *mockHostClient) Prices(_ context.Context, hostKey types.PublicKey) (proto.HostPrices, error) {
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
	if err, ok := m.integrityErrors[root]; ok {
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

	_, err := w.Write(sector[offset : offset+length])
	if err != nil {
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

func newMockHostClient() *mockHostClient {
	return &mockHostClient{
		hostSectors:     make(map[types.PublicKey]map[types.Hash256][proto.SectorSize]byte),
		slowHosts:       make(map[types.PublicKey]time.Duration),
		integrityErrors: make(map[types.Hash256]error),
		hostKeys:        make(map[types.PublicKey]types.PrivateKey),
		hostSettings:    make(map[types.PublicKey]proto.HostSettings),
		unusable:        make(map[types.PublicKey]struct{}),
	}
}
