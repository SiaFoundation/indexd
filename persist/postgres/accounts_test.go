package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk1 := types.GeneratePrivateKey().PublicKey()
	err := store.AddAccount(context.Background(), pk1)
	if err != nil {
		t.Fatal(err)
	}

	pk2 := types.GeneratePrivateKey().PublicKey()
	err = store.AddAccount(context.Background(), pk2)
	if err != nil {
		t.Fatal(err)
	}

	accs, err := store.Accounts(context.Background(), 0, 2)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 2 || accs[0] != pk1 || accs[1] != pk2 {
		t.Fatal("unexpected accounts", accs)
	}

	err = store.AddAccount(context.Background(), pk2)
	if !errors.Is(err, accounts.ErrExists) {
		t.Fatal("unexpected error")
	}
}

func TestAddAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk := types.GeneratePrivateKey().PublicKey()
	err := store.AddAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	}
	err = store.AddAccount(context.Background(), pk)
	if !errors.Is(err, accounts.ErrExists) {
		t.Fatal("expected ErrExists, got", err)
	}
	accs, err := store.Accounts(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 || accs[0] != pk {
		t.Fatal("unexpected accounts", accs)
	}
}

func TestHasAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk := types.GeneratePrivateKey().PublicKey()
	found, err := store.HasAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("expected account to not exist")
	}

	err = store.AddAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	}

	found, err = store.HasAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("expected account to exist")
	}
}

func TestDeleteAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk := types.GeneratePrivateKey().PublicKey()
	err := store.DeleteAccount(context.Background(), pk)
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected [accounts.ErrNotFound]")
	}

	err = store.AddAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	}

	found, err := store.HasAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("expected account to exist")
	}

	accs, err := store.Accounts(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 || accs[0] != pk {
		t.Fatal("unexpected accounts", accs)
	}

	err = store.DeleteAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	}

	accs, err = store.Accounts(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 0 {
		t.Fatal("unexpected accounts", accs)
	}
}

func TestHostAccountsForFunding(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// define helper to count join table entries
	numEAs := func() (cnt int64) {
		t.Helper()
		if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM account_hosts`).Scan(&cnt)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return
	}

	// add a host
	hk1 := types.PublicKey{1}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk1, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// assert there are no accounts to fund
	accounts, err := store.HostAccountsForFunding(context.Background(), hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 0 {
		t.Fatal("expected no accounts")
	}

	// add an account
	ak1 := types.PublicKey{1, 1}
	if err := store.AddAccount(context.Background(), ak1); err != nil {
		t.Fatal(err)
	}

	// assert there's now one account to fund
	accounts, err = store.HostAccountsForFunding(context.Background(), hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("expected one account")
	} else if accounts[0].AccountKey != proto.Account(ak1) {
		t.Fatal("unexpected account key")
	} else if accounts[0].HostKey != hk1 {
		t.Fatal("unexpected host key")
	} else if accounts[0].ConsecutiveFailedFunds != 0 {
		t.Fatal("unexpected consecutive failed funds")
	} else if accounts[0].NextFund.IsZero() {
		t.Fatal("unexpected next fund")
	}

	// assert there's no EAs
	if n := numEAs(); n != 0 {
		t.Fatal("expected no account-host entries", n)
	}

	// update next fund
	accounts[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostAccounts(context.Background(), accounts); err != nil {
		t.Fatal(err)
	}

	// assert the update inserted the account
	if n := numEAs(); n != 1 {
		t.Fatal("expected one account-host entry", n)
	}

	// assert there are no accounts to fund
	accounts, err = store.HostAccountsForFunding(context.Background(), hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 0 {
		t.Fatal("expected no accounts")
	}

	// add another host
	hk2 := types.PublicKey{2}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk2, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add another account
	ak2 := types.PublicKey{2, 2}
	if err := store.AddAccount(context.Background(), ak2); err != nil {
		t.Fatal(err)
	}

	// assert h1 has one account to fund
	accounts, err = store.HostAccountsForFunding(context.Background(), hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("expected one account")
	} else if accounts[0].AccountKey != proto.Account(ak2) {
		t.Fatal("unexpected account key")
	} else if err := store.UpdateHostAccounts(context.Background(), accounts); err != nil {
		t.Fatal(err)
	}

	// assert h2 has two accounts to fund
	accounts, err = store.HostAccountsForFunding(context.Background(), hk2, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 2 {
		t.Fatal("expected two accounts")
	} else if err := store.UpdateHostAccounts(context.Background(), accounts); err != nil {
		t.Fatal(err)
	}

	// assert limit is applied
	accounts, err = store.HostAccountsForFunding(context.Background(), hk2, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("expected one accounts")
	}

	// assert the updates inserted all accounts
	if n := numEAs(); n != 4 {
		t.Fatal("expected 4 account-host entries, got", n)
	}
}

func TestUpdateHostAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.GeneratePrivateKey().PublicKey()
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add an account
	ak := types.GeneratePrivateKey().PublicKey()
	if err := store.AddAccount(context.Background(), ak); err != nil {
		t.Fatal(err)
	}

	// fetch accounts for funding
	accounts, err := store.HostAccountsForFunding(context.Background(), hk, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("expected one account")
	}
	accounts[0].ConsecutiveFailedFunds = frand.Intn(1e3)
	accounts[0].NextFund = time.Now().Add(time.Duration(frand.Uint64n(1e6))).Round(time.Microsecond)

	// update the account
	err = store.UpdateHostAccounts(context.Background(), accounts)
	if err != nil {
		t.Fatal(err)
	}

	// assert the account was upserted
	var updatedFailures int
	var updatedNextFund time.Time
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `SELECT consecutive_failed_funds, next_fund FROM account_hosts`).Scan(&updatedFailures, &updatedNextFund)
		return err
	}); err != nil {
		t.Fatal(err)
	} else if updatedFailures != accounts[0].ConsecutiveFailedFunds {
		t.Fatal("unexpected consecutive failed funds")
	} else if updatedNextFund != accounts[0].NextFund {
		t.Fatal("unexpected next fund", updatedNextFund, accounts[0].NextFund)
	}
}
