package accounts_test

import (
	"context"
	"errors"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
)

func TestServiceAccounts(t *testing.T) {
	s := newTestStore(t)

	// add host
	host := hosts.Host{
		PublicKey: types.GeneratePrivateKey().PublicKey(),
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "foo"}},
		Usability: goodUsability,
	}
	s.AddTestHost(t, host)

	// add accounts
	account1 := proto.Account(types.GeneratePrivateKey().PublicKey())
	account2 := proto.Account(types.GeneratePrivateKey().PublicKey())
	s.AddTestServiceAccount(t, host.PublicKey, account1)
	s.AddTestServiceAccount(t, host.PublicKey, account2)

	f := &mockFunder{}
	am := accounts.NewManager(s, f)
	defer am.Close()

	// helper to assert balance
	assertBalance := func(account proto.Account, expected types.Currency) {
		t.Helper()
		if balance, err := s.ServiceAccountBalance(host.PublicKey, account); err != nil {
			t.Fatal(err)
		} else if !balance.Equals(expected) {
			t.Fatalf("expected balance %v, got %v", expected, balance)
		}
	}

	// try to user account before registering it
	err := am.ResetAccountBalance(context.Background(), host.PublicKey, account1)
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected ErrNotFound")
	}
	_, err = am.ServiceAccountBalance(context.Background(), host.PublicKey, account1)
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected ErrNotFound")
	}
	err = am.DebitServiceAccount(context.Background(), host.PublicKey, account1, types.Siacoins(1))
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected ErrNotFound")
	}

	// batch updating should ignore the account
	err = am.UpdateServiceAccounts(context.Background(), []accounts.HostAccount{
		{AccountKey: account1, HostKey: host.PublicKey},
		{AccountKey: account2, HostKey: host.PublicKey},
	}, types.Siacoins(1))
	if err != nil {
		t.Fatal(err)
	}
	assertBalance(account1, types.ZeroCurrency)
	assertBalance(account2, types.ZeroCurrency)

	// register account, it's now possible to update the balance of account 1
	am.RegisterServiceAccount(account1)
	err = am.UpdateServiceAccounts(context.Background(), []accounts.HostAccount{
		{AccountKey: account1, HostKey: host.PublicKey},
		{AccountKey: account2, HostKey: host.PublicKey},
	}, types.Siacoins(1))
	if err != nil {
		t.Fatal(err)
	}
	assertBalance(account1, types.Siacoins(1))
	assertBalance(account2, types.ZeroCurrency)

	// other methods should also work now
	err = am.DebitServiceAccount(context.Background(), host.PublicKey, account1, types.Siacoins(1).Div64(2))
	if err != nil {
		t.Fatal(err)
	} else if balance, err := am.ServiceAccountBalance(context.Background(), host.PublicKey, account1); err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.Siacoins(1).Div64(2)) {
		t.Fatalf("expected balance %v, got %v", types.Siacoins(1).Div64(2), balance)
	}
	assertBalance(account1, types.Siacoins(1).Div64(2))
	assertBalance(account2, types.ZeroCurrency)

	err = am.ResetAccountBalance(context.Background(), host.PublicKey, account1)
	if err != nil {
		t.Fatal(err)
	} else if balance, err := am.ServiceAccountBalance(context.Background(), host.PublicKey, account1); err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected balance %v, got %v", types.ZeroCurrency, balance)
	}
	assertBalance(account1, types.ZeroCurrency)
	assertBalance(account2, types.ZeroCurrency)
}
