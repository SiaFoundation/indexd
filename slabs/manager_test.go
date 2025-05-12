package slabs

import (
	"context"
	"slices"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

type mockStore struct {
	accounts        map[proto.Account]struct{}
	lostSectors     map[types.PublicKey]map[types.Hash256]struct{}
	failedChecks    map[types.PublicKey]map[types.Hash256]int
	sectorsForCheck []types.Hash256
	serviceAccounts map[proto.Account]types.Currency
}

func newMockStore() *mockStore {
	return &mockStore{
		accounts:        make(map[proto.Account]struct{}),
		failedChecks:    make(map[types.PublicKey]map[types.Hash256]int),
		lostSectors:     make(map[types.PublicKey]map[types.Hash256]struct{}),
		serviceAccounts: make(map[proto.Account]types.Currency),
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
	m.serviceAccounts[account] = struct{}{}
}

func (m *mockAccountManager) ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	return m.UpdateServiceAccountBalance(ctx, hostKey, account, types.ZeroCurrency)
}

func (m *mockAccountManager) ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	if balance, ok := m.store.serviceAccounts[account]; ok {
		return balance, nil
	}
	return types.ZeroCurrency, nil
}

func (m *mockAccountManager) UpdateServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account, balance types.Currency) error {
	m.store.serviceAccounts[account] = balance
	return nil
}

func (m *mockAccountManager) DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	balance := m.store.serviceAccounts[account]
	if balance.Cmp(amount) < 0 {
		m.store.serviceAccounts[account] = types.ZeroCurrency
	} else {
		m.store.serviceAccounts[account] = balance.Sub(amount)
	}
	return nil
}
