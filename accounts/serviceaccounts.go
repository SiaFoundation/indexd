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
		HostKey types.PublicKey
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
func (m *AccountManager) LockAccount(account proto.Account, hostKey types.PublicKey) (LockedServiceAccount, error) {
	m.serviceAccountsMu.Lock()
	serviceAccs, ok := m.serviceAccounts[account]
	if !ok {
		m.serviceAccountsMu.Unlock()
		return LockedServiceAccount{}, ErrNotFound
	}
	acc, ok := serviceAccs[hostKey]
	if !ok {
		serviceAccs[hostKey] = &serviceAccount{}
		acc = serviceAccs[hostKey]
	}

	m.serviceAccountsMu.Unlock()
	acc.mu.Lock()

	return LockedServiceAccount{
		serviceAccount: acc,
		Account:        account,
		HostKey:        hostKey,
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
	m.serviceAccounts[account] = make(map[types.PublicKey]*serviceAccount)
}

// ServiceAccountBalance returns the balance of a locked service account.
func (m *AccountManager) ServiceAccountBalance(ctx context.Context, account LockedServiceAccount) (types.Currency, error) {
	return m.store.ServiceAccountBalance(ctx, account)
}

// UpdateServiceAccountBalance updates the balance of a locked service account.
func (m *AccountManager) UpdateServiceAccountBalance(ctx context.Context, account LockedServiceAccount, balance types.Currency) error {
	return m.store.UpdateServiceAccountBalance(ctx, account, balance)
}

// lockServiceAccounts locks all service accounts in the list of accounts.
// Non-service accounts are ignored.
func (m *AccountManager) lockServiceAccounts(accounts []HostAccount) []LockedServiceAccount {
	m.serviceAccountsMu.Lock()
	var toLock []LockedServiceAccount
	for _, account := range accounts {
		serviceAccs, ok := m.serviceAccounts[account.AccountKey]
		if !ok {
			continue
		}
		acc, ok := serviceAccs[account.HostKey]
		if !ok {
			serviceAccs[account.HostKey] = &serviceAccount{}
			acc = serviceAccs[account.HostKey]
		}
		toLock = append(toLock, LockedServiceAccount{
			serviceAccount: acc,
			Account:        account.AccountKey,
			HostKey:        account.HostKey,
		})
	}
	m.serviceAccountsMu.Unlock()

	// lock accounts outside of the manager's mutex
	for _, account := range toLock {
		account.serviceAccount.mu.Lock()
	}
	return toLock
}
