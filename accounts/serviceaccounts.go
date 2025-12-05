package accounts

import (
	"context"
	"maps"
	"slices"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// RegisterServiceAccount signals to the account manager that a specific
// account is for internal use. The account first needs to be added. A service
// account will have its balance tracked.
func (m *AccountManager) RegisterServiceAccount(account proto.Account) {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()
	if _, exists := m.serviceAccounts[account]; exists {
		panic("service account already registered") // developer error
	}
	m.serviceAccounts[account] = struct{}{}
}

// ResetAccountBalance resets the account balance of a service account to 0.
// This should only be called when a host reports that an RPC failed due to
// insufficient balance.
func (m *AccountManager) ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	if !m.serviceAccountExists(account) {
		return ErrNotFound
	}
	return m.store.UpdateServiceAccountBalance(hostKey, account, types.ZeroCurrency)
}

// ServiceAccountBalance returns the balance of a locked service account.
func (m *AccountManager) ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	if !m.serviceAccountExists(account) {
		return types.ZeroCurrency, ErrNotFound
	}
	return m.store.ServiceAccountBalance(hostKey, account)
}

// DebitServiceAccount withdraws from a service account. This should be used
// after successfully withdrawing from an account.
func (m *AccountManager) DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	if !m.serviceAccountExists(account) {
		return ErrNotFound
	}
	return m.store.DebitServiceAccount(hostKey, account, amount)
}

func (m *AccountManager) serviceAccountExists(account proto.Account) bool {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()
	_, exists := m.serviceAccounts[account]
	return exists
}

func (m *AccountManager) serviceAccs() []proto.Account {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()
	return slices.Collect(maps.Keys(m.serviceAccounts))
}

// UpdateServiceAccounts updates the balance of all accounts registered as
// service accounts to 'balance'.
func (m *AccountManager) UpdateServiceAccounts(ctx context.Context, accounts []HostAccount, balance types.Currency) error {
	m.serviceAccountsMu.Lock()
	var toUpdate []HostAccount
	for _, account := range accounts {
		_, ok := m.serviceAccounts[account.AccountKey]
		if !ok {
			continue
		}
		toUpdate = append(toUpdate, account)
	}
	m.serviceAccountsMu.Unlock()

	for _, account := range toUpdate {
		if err := m.store.UpdateServiceAccountBalance(account.HostKey, account.AccountKey, balance); err != nil {
			return err
		}
	}
	return nil
}
