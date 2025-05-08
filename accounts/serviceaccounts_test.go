package accounts

import (
	"context"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// UpdateServiceAccountBalance updates the balance of a service account.
func (s *mockStore) UpdateServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account, balance types.Currency) error {
	accs, ok := s.serviceAccounts[account]
	if !ok {
		s.serviceAccounts[account] = make(map[types.PublicKey]types.Currency)
		accs = s.serviceAccounts[account]
	}
	accs[hostKey] = balance
	return nil
}

// ServiceAccountBalance returns the balance of a service account.
func (s *mockStore) ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	accs, ok := s.serviceAccounts[account]
	if !ok {
		return types.ZeroCurrency, nil
	}
	return accs[hostKey], nil
}

//func TestServiceAccounts(t *testing.T) {
//	s := newMockStore()
//
//	// add host
//	host := hosts.Host{
//		PublicKey: types.GeneratePrivateKey().PublicKey(),
//		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "foo"}},
//		Usability: goodUsability,
//	}
//	s.hosts[host.PublicKey] = host
//
//	// add accounts
//	account1 := proto.Account(types.GeneratePrivateKey().PublicKey())
//	account2 := proto.Account(types.GeneratePrivateKey().PublicKey())
//	s.accounts[types.PublicKey(account1)] = struct{}{}
//	s.accounts[types.PublicKey(account2)] = struct{}{}
//
//	f := &mockFunder{}
//	am := NewManager(s, f)
//	defer am.Close()
//
//	// try to fetch the service account before registering it
//	_, err := am.LockAccount(account1, host.PublicKey)
//	if !errors.Is(err, ErrNotFound) {
//		t.Fatal(err)
//	}
//
//	am.RegisterServiceAccount(account1) // register
//
//	// helper to assert the account is locked
//	assertLocked := func(account proto.Account) func() {
//		t.Helper()
//		done := make(chan struct{})
//		go func() {
//			lockedAcc, err := am.LockAccount(account, host.PublicKey)
//			if err != nil {
//				t.Error(err)
//			}
//			lockedAcc.Unlock()
//			done <- struct{}{}
//		}()
//
//		select {
//		case <-time.After(100 * time.Millisecond):
//		case <-done:
//			t.Fatal("expected account to be locked")
//		}
//		return func() {
//			select {
//			case <-time.After(100 * time.Millisecond):
//				t.Fatal("expected account to be unlocked")
//			case <-done:
//			}
//		}
//	}
//
//	// helper to update balance
//	updateBalance := func(account LockedServiceAccount, balance types.Currency) {
//		t.Helper()
//		if err := am.UpdateServiceAccountBalance(context.Background(), account, balance); err != nil {
//			t.Fatal(err)
//		}
//	}
//
//	am.RegisterServiceAccount(account2) // register
//
//	// Lock the account and make sure it is locked and can be unlocked
//	lockedAcc, err := am.LockAccount(account1, host.PublicKey)
//	if err != nil {
//		t.Fatal(err)
//	}
//	wait := assertLocked(account1)
//	updateBalance(lockedAcc, types.Siacoins(1))
//	lockedAcc.Unlock()
//	wait()
//
//	// Same thing with the batched functions
//	lockedAccs := am.lockServiceAccounts([]HostAccount{
//		{
//			// known account
//			AccountKey: account2,
//			HostKey:    host.PublicKey,
//		},
//		{
//			// unknown account
//			AccountKey: proto.Account(frand.Entropy256()),
//			HostKey:    types.PublicKey(frand.Entropy256()),
//		},
//	})
//	if len(lockedAccs) != 1 {
//		t.Fatal("expected 1 locked account")
//	}
//	lockedAcc2 := lockedAccs[0]
//	wait = assertLocked(account2)
//	updateBalance(lockedAcc2, types.Siacoins(2))
//	lockedAcc2.Unlock()
//	wait()
//
//	// Assert balances of accounts
//	assertBalance := func(account proto.Account, expected types.Currency) {
//		t.Helper()
//		lockedAcc, err := am.LockAccount(account, host.PublicKey)
//		if err != nil {
//			t.Fatal(err)
//		}
//		defer lockedAcc.Unlock()
//
//		balance, err := am.ServiceAccountBalance(context.Background(), lockedAcc)
//		if err != nil {
//			t.Fatal(err)
//		} else if !balance.Equals(expected) {
//			t.Fatalf("expected balance %v, got %v", expected, balance)
//		}
//	}
//	assertBalance(account1, types.Siacoins(1))
//	assertBalance(account2, types.Siacoins(2))
//}
