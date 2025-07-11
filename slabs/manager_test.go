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
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
)

type mockStore struct {
	accounts        map[proto.Account]struct{}
	hosts           map[types.PublicKey]hosts.Host
	lostSectors     map[types.PublicKey]map[types.Hash256]struct{}
	failedChecks    map[types.PublicKey]map[types.Hash256]int
	sectorsForCheck []types.Hash256
	serviceAccounts map[proto.Account]map[types.PublicKey]types.Currency
}

func newMockStore() *mockStore {
	return &mockStore{
		accounts:        make(map[proto.Account]struct{}),
		hosts:           make(map[types.PublicKey]hosts.Host),
		failedChecks:    make(map[types.PublicKey]map[types.Hash256]int),
		lostSectors:     make(map[types.PublicKey]map[types.Hash256]struct{}),
		serviceAccounts: make(map[proto.Account]map[types.PublicKey]types.Currency),
	}
}

func (s *mockStore) AddAccount(ctx context.Context, account types.PublicKey) error {
	s.accounts[proto.Account(account)] = struct{}{}
	return nil
}

func (s *mockStore) FailingSectors(ctx context.Context, hostKey types.PublicKey, minChecks, limit int) ([]types.Hash256, error) {
	var roots []types.Hash256
	for root, failures := range s.failedChecks[hostKey] {
		if failures >= minChecks {
			roots = append(roots, root)
		}
	}
	return roots, nil
}

func (s *mockStore) Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	panic("not implemented")
}

func (s *mockStore) HostsForIntegrityChecks(ctx context.Context, limit int) (result []types.PublicKey, err error) {
	return nil, nil
}

func (s *mockStore) HostsWithLostSectors(ctx context.Context) (hks []types.PublicKey, err error) {
	for hk, lostSectors := range s.lostSectors {
		if len(lostSectors) > 0 {
			hks = append(hks, hk)
		}
	}
	return
}

func (s *mockStore) MarkFailingSectorsLost(ctx context.Context, hostKey types.PublicKey, maxFailedIntegrityChecks uint) error {
	for root, failures := range s.failedChecks[hostKey] {
		if failures >= int(maxFailedIntegrityChecks) {
			s.lostSectors[hostKey][root] = struct{}{}
		}
	}
	return nil
}

func (s *mockStore) MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error {
	if _, ok := s.lostSectors[hostKey]; !ok {
		s.lostSectors[hostKey] = make(map[types.Hash256]struct{})
	}
	for _, root := range roots {
		s.lostSectors[hostKey][root] = struct{}{}
	}
	return nil
}

func (s *mockStore) PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab SlabPinParams) (SlabID, error) {
	panic("not implemented")
}

func (s *mockStore) RecordIntegrityCheck(ctx context.Context, success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error {
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

func (s *mockStore) SectorsForIntegrityCheck(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	return slices.Clone(s.sectorsForCheck), nil
}

func (s *mockStore) Slabs(ctx context.Context, accountID proto.Account, slabIDs []SlabID) ([]Slab, error) {
	panic("not implemented")
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

type mockhostManager struct {
	hosts map[types.PublicKey]hosts.Host
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
	return fn(host)
}

type mockDialer struct {
	clients map[types.PublicKey]*mockHostClient
}

func newMockDialer(hosts []hosts.Host) *mockDialer {
	clients := make(map[types.PublicKey]*mockHostClient, len(hosts))
	for _, host := range hosts {
		clients[host.PublicKey] = &mockHostClient{
			sectors:  make(map[types.Hash256][proto.SectorSize]byte),
			settings: host.Settings,
		}
	}
	return &mockDialer{clients: clients}
}

func (d *mockDialer) DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error) {
	if client, ok := d.clients[hostKey]; ok {
		return client, nil
	}
	return nil, errors.New("failed to dial host")
}

type mockHostClient struct {
	delay    time.Duration
	sectors  map[types.Hash256][proto.SectorSize]byte
	settings proto.HostSettings
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

func (c *mockHostClient) Settings(context.Context, types.PublicKey) (proto.HostSettings, error) {
	return c.settings, nil
}
