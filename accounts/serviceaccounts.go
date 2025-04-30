package accounts

import (
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
func (m *AccountManager) ServiceAccountBalance(account LockedServiceAccount) (types.Currency, error) {
	return types.ZeroCurrency, nil // read balance from database
}

// UpdateServiceAccountBalance updates the balance of a locked service account.
func (m *AccountManager) UpdateServiceAccountBalance(account LockedServiceAccount) error {
	return nil // update balance in database
}

// lockServiceAccounts locks all service accounts in the list of accounts.
// Non-service accounts are ignored.
func (m *AccountManager) lockServiceAccounts(accounts []HostAccount) []LockedServiceAccount {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()

	var locked []LockedServiceAccount
	for _, account := range accounts {
		if serviceAcc, ok := m.serviceAccounts[account.AccountKey]; ok {
			serviceAcc.mu.Lock()
			locked = append(locked, LockedServiceAccount{
				Account:        account.AccountKey,
				serviceAccount: serviceAcc,
			})
		}
	}
	return locked
}
