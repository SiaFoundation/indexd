package slabs

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

type mockStore struct{}

func newMockStore() *mockStore {
	return &mockStore{}
}

func (s *mockStore) AddAccount(ctx context.Context, ak types.PublicKey) error {
	panic("not implemented")
}

func (s *mockStore) FailingSectors(ctx context.Context, hostKey types.PublicKey, minChecks, limit int) ([]types.Hash256, error) {
	panic("not implemented")
}

func (s *mockStore) Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	panic("not implemented")
}

func (s *mockStore) MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error {
	panic("not implemented")
}

func (s *mockStore) PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab SlabPinParams) (SlabID, error) {
	panic("not implemented")
}

func (s *mockStore) RecordIntegrityCheck(ctx context.Context, success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error {
	panic("not implemented")
}

func (s *mockStore) SectorsForIntegrityCheck(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	panic("not implemented")
}

func (s *mockStore) Slabs(ctx context.Context, accountID proto.Account, slabIDs []SlabID) ([]Slab, error) {
	panic("not implemented")
}

type mockAccountManager struct{}

func newMockAccountManager() *mockAccountManager {
	return &mockAccountManager{}
}

func (m *mockAccountManager) RegisterServiceAccount(account proto.Account) {
	panic("not implemented")
}

func (m *mockAccountManager) ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	panic("not implemented")
}

func (m *mockAccountManager) ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	panic("not implemented")
}

func (m *mockAccountManager) DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	panic("not implemented")
}
