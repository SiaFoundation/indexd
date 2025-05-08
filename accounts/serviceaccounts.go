package accounts

import (
	"context"

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
	m.serviceAccounts[account] = make(map[types.PublicKey]struct{})
}

// ResetAccountBalance resets the account balance of a service account to 0.
// This should only be called when a host reports that an RPC failed due to
// insufficient balance.
func (m *AccountManager) ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	return m.store.UpdateServiceAccountBalance(ctx, hostKey, account, types.ZeroCurrency)
}

// ServiceAccountBalance returns the balance of a locked service account.
func (m *AccountManager) ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	return m.store.ServiceAccountBalance(ctx, hostKey, account)
}

// updateServiceAccounts updates the balance of all accounts registered as
// service accounts to 'balance'.
func (m *AccountManager) updateServiceAccounts(ctx context.Context, accounts []HostAccount, balance types.Currency) error {
	m.serviceAccountsMu.Lock()
	var toUpdate []HostAccount
	for _, account := range accounts {
		serviceAccs, ok := m.serviceAccounts[account.AccountKey]
		if !ok {
			continue
		}
		_, ok = serviceAccs[account.HostKey]
		if !ok {
			continue
		}
		toUpdate = append(toUpdate, account)
	}
	m.serviceAccountsMu.Unlock()

	for _, account := range toUpdate {
		if err := m.store.UpdateServiceAccountBalance(ctx, account.HostKey, account.AccountKey, balance); err != nil {
			return err
		}
	}
	return nil
}
