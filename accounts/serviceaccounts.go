package accounts

import (
	"context"
	"sync"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	// LockedServiceAccount wraps a service account for use in other packages.
	// It can be obtained by calling 'LockAccount' on the AccountManager.
	LockedServiceAccount struct {
		serviceAccount *serviceAccount

		Account proto.Account
	}

	serviceAccount struct {
		mu sync.Mutex
	}
)

// Unlock unlocks an account.
func (a LockedServiceAccount) Unlock() {
	a.serviceAccount.mu.Unlock()
}

// LockAccount locks an account which allows for updating its balance. The
// returned account needs to be unlocked using its 'Unlock' method.
func (m *AccountManager) LockAccount(account proto.Account) (LockedServiceAccount, error) {
	m.serviceAccountsMu.Lock()
	sa, ok := m.serviceAccounts[account]
	if !ok {
		m.serviceAccountsMu.Unlock()
		return LockedServiceAccount{}, ErrNotFound
	}
	m.serviceAccountsMu.Unlock()
	sa.mu.Lock()

	return LockedServiceAccount{
		Account: account,
	}, nil
}

// RegisterServiceAccount signals to the account manager that a specific
// account is for internal use. The account first needs to be added. A service
// account will have its balance tracked.
func (m *AccountManager) RegisterServiceAccount(account proto.Account) {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()
	if _, exists := m.serviceAccounts[account]; exists {
		panic("service account already registered") // developer error
	}
	m.serviceAccounts[account] = &serviceAccount{}
}

// ServiceAccountBalance returns the balance of a locked service account.
func (m *AccountManager) ServiceAccountBalance(ctx context.Context, account LockedServiceAccount) (types.Currency, error) {
	return m.store.ServiceAccountBalance(ctx, account.Account)
}

// UpdateServiceAccountBalance updates the balance of a locked service account.
func (m *AccountManager) UpdateServiceAccountBalance(ctx context.Context, account LockedServiceAccount, balance types.Currency) error {
	return m.store.UpdateServiceAccountBalance(ctx, account.Account, balance)
}

// lockServiceAccounts locks all service accounts in the list of accounts.
// Non-service accounts are ignored.
func (m *AccountManager) lockServiceAccounts(accounts []HostAccount) []LockedServiceAccount {
	m.serviceAccountsMu.Lock()
	var toLock []LockedServiceAccount
	for _, account := range accounts {
		if serviceAcc, ok := m.serviceAccounts[account.AccountKey]; ok {
			toLock = append(toLock, LockedServiceAccount{
				Account:        account.AccountKey,
				serviceAccount: serviceAcc,
			})
		}
	}
	m.serviceAccountsMu.Unlock()

	// lock accounts outside of the manager's mutex
	for _, account := range toLock {
		account.serviceAccount.mu.Lock()
	}
	return toLock
}
