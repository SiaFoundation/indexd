package slabs

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"lukechampine.com/frand"
)

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

func (s *mockStore) AddAccount(account types.PublicKey, meta accounts.AccountMeta, opts ...accounts.AddAccountOption) error {
	s.accounts[proto.Account(account)] = struct{}{}
	return nil
}

func (s *mockStore) AddServiceAccount(account types.PublicKey, meta accounts.AccountMeta, opts ...accounts.AddAccountOption) error {
	s.accounts[proto.Account(account)] = struct{}{}
	return nil
}

func (s *mockStore) Contracts(offset, limit int, opts ...contracts.ContractQueryOpt) ([]contracts.Contract, error) {
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
	var roots []types.Hash256
	for root, failures := range s.failedChecks[hostKey] {
		if failures >= minChecks {
			roots = append(roots, root)
		}
	}
	return roots, nil
}

func (s *mockStore) Hosts(offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
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
	return nil, nil
}

func (s *mockStore) HostsWithLostSectors() (hks []types.PublicKey, err error) {
	for hk, lostSectors := range s.lostSectors {
		if len(lostSectors) > 0 {
			hks = append(hks, hk)
		}
	}
	return
}

func (s *mockStore) MaintenanceSettings() (contracts.MaintenanceSettings, error) {
	return contracts.DefaultMaintenanceSettings, nil
}

func (s *mockStore) MarkFailingSectorsLost(hostKey types.PublicKey, maxFailedIntegrityChecks uint) error {
	for root, failures := range s.failedChecks[hostKey] {
		if failures >= int(maxFailedIntegrityChecks) {
			s.lostSectors[hostKey][root] = struct{}{}
		}
	}
	return nil
}

func (s *mockStore) MarkSectorsLost(hostKey types.PublicKey, roots []types.Hash256) error {
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
	_, ok := s.migratedSectors[hostKey]
	if !ok {
		s.migratedSectors[hostKey] = make(map[types.Hash256]struct{})
	}
	s.migratedSectors[hostKey][root] = struct{}{}

	contract, ok := s.contracts[hostKey]
	if !ok {
		return false, errors.New("host contract not found")
	}

	for acc := range s.accounts {
		for slabID, slab := range s.pinnedSlabs[acc] {
			for i, sector := range slab.Sectors {
				if sector.Root == root {
					s.pinnedSlabs[acc][slabID].Sectors[i].HostKey = &hostKey
					s.pinnedSlabs[acc][slabID].Sectors[i].ContractID = &contract.ID
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (s *mockStore) PinSlabs(account proto.Account, nextIntegrityCheck time.Time, slabs ...SlabPinParams) ([]SlabID, error) {
	var digests []SlabID
	for _, slab := range slabs {
		slabID, err := slab.Digest()
		if err != nil {
			return nil, err
		}
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
	return slices.Clone(s.sectorsForCheck), nil
}

func (s *mockStore) Slab(slabID SlabID) (Slab, error) {
	for acc := range s.accounts {
		if slab, ok := s.pinnedSlabs[acc][slabID]; ok {
			return slab, nil
		}
	}
	return Slab{}, ErrSlabNotFound
}

func (s *mockStore) Slabs(accountID proto.Account, slabIDs []SlabID) ([]Slab, error) {
	var slabs []Slab
	for _, slab := range s.pinnedSlabs[accountID] {
		slabs = append(slabs, slab)
	}
	return slabs, nil
}

func (s *mockStore) UnhealthySlabs(limit int) (result []SlabID, _ error) {
	for acc := range s.accounts {
		for _, slab := range s.pinnedSlabs[acc] {
			for _, sector := range slab.Sectors {
				if len(result) >= limit {
					break
				}

				if sector.ContractID == nil || sector.HostKey == nil {
					result = append(result, slab.ID)
					break
				}
				if sector.HostKey != nil {
					hk := *sector.HostKey
					contract, ok := s.contracts[hk]
					if ok && !contract.Good {
						result = append(result, slab.ID)
						break
					}
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
	triggeredRefills map[proto.Account]int
}

func newMockContractManager() *mockContractManager {
	return &mockContractManager{
		triggeredRefills: make(map[proto.Account]int),
	}
}

func (m *mockContractManager) TriggerAccountRefill(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	m.triggeredRefills[account]++
	return nil
}

type mockhostManager struct {
	hosts map[types.PublicKey]hosts.Host

	refreshPrices bool // reset prices.ValidUntil after each call to WithScannedHost
}

func newMockHostManager() *mockhostManager {
	return &mockhostManager{
		hosts: make(map[types.PublicKey]hosts.Host),
	}
}

func (mock *mockhostManager) WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h hosts.Host) error) error {
	host, ok := mock.hosts[hk]
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

type mockDialer struct {
	clients map[types.PublicKey]*mockHostClient
}

func newMockDialer(hosts []hosts.Host) *mockDialer {
	clients := make(map[types.PublicKey]*mockHostClient, len(hosts))
	for _, host := range hosts {
		clients[host.PublicKey] = &mockHostClient{
			sectors:   make(map[types.Hash256][proto.SectorSize]byte),
			integrity: make(map[types.Hash256]error),
			settings:  host.Settings,
		}
	}
	return &mockDialer{clients: clients}
}

func (d *mockDialer) DialHost(ctx context.Context, hostKey types.PublicKey, addrs []chain.NetAddress) (HostClient, error) {
	if client, ok := d.clients[hostKey]; ok {
		return client, nil
	}
	return nil, errors.New("failed to dial host")
}

type mockHostClient struct {
	delay     time.Duration
	sectors   map[types.Hash256][proto.SectorSize]byte
	integrity map[types.Hash256]error
	settings  proto.HostSettings
}

func (c *mockHostClient) ReadSector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (rhp.RPCReadSectorResult, error) {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return rhp.RPCReadSectorResult{}, ctx.Err()
	}

	sector, ok := c.sectors[root]
	if !ok {
		return rhp.RPCReadSectorResult{}, proto.ErrSectorNotFound
	}
	_, err := w.Write(sector[:])
	if err != nil {
		return rhp.RPCReadSectorResult{}, err
	}
	return rhp.RPCReadSectorResult{
		Usage: c.settings.Prices.RPCReadSectorCost(proto.SectorSize),
	}, nil
}

func (c *mockHostClient) WriteSector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, data io.Reader, length uint64) (rhp.RPCWriteSectorResult, error) {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return rhp.RPCWriteSectorResult{}, ctx.Err()
	}

	var sector [proto.SectorSize]byte
	_, err := io.ReadFull(data, sector[:])
	if err != nil {
		return rhp.RPCWriteSectorResult{}, err
	}
	root := proto.SectorRoot(&sector)
	c.sectors[root] = sector
	return rhp.RPCWriteSectorResult{
		Root:  root,
		Usage: c.settings.Prices.RPCWriteSectorCost(proto.SectorSize),
	}, nil
}

func (c *mockHostClient) Settings(context.Context) (proto.HostSettings, error) {
	return c.settings, nil
}
