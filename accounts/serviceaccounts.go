package accounts

import (
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
	m.serviceAccounts[account] = make(map[types.PublicKey]types.Currency)
}

// ResetAccountBalance resets the account balance of a service account to 0.
// This should only be called when a host reports that an RPC failed due to
// insufficient balance.
func (m *AccountManager) ResetAccountBalance(hostKey types.PublicKey, account proto.Account) error {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()

	if _, exists := m.serviceAccounts[account]; !exists {
		return ErrNotFound
	}
	m.serviceAccounts[account][hostKey] = types.ZeroCurrency
	return nil
}

// ServiceAccountBalance returns the balance of a locked service account.
func (m *AccountManager) ServiceAccountBalance(hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()

	if _, exists := m.serviceAccounts[account]; !exists {
		return types.ZeroCurrency, ErrNotFound
	}
	return m.serviceAccounts[account][hostKey], nil
}

// DebitServiceAccount withdraws from a service account. This should be used
// after successfully withdrawing from an account.
func (m *AccountManager) DebitServiceAccount(hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	m.serviceAccountsMu.Lock()
	defer m.serviceAccountsMu.Unlock()

	if _, exists := m.serviceAccounts[account]; !exists {
		return ErrNotFound
	}

	val, underflow := m.serviceAccounts[account][hostKey].SubWithUnderflow(amount)
	if underflow {
		val = types.ZeroCurrency
	}
	m.serviceAccounts[account][hostKey] = val
	return nil
}

// UpdateServiceAccounts updates the balance of all accounts registered as
// service accounts to 'balance'.
func (m *AccountManager) UpdateServiceAccounts(accounts []HostAccount, balance types.Currency) error {
	m.serviceAccountsMu.Lock()
	for _, account := range accounts {
		_, ok := m.serviceAccounts[account.AccountKey]
		if !ok {
			continue
		}
		m.serviceAccounts[account.AccountKey][account.HostKey] = balance
	}
	m.serviceAccountsMu.Unlock()

	return nil
}
