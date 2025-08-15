package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	assertAccount := func(t *testing.T, acc accounts.Account, expectedKey types.PublicKey, maxData uint64, isService bool) {
		t.Helper()
		switch {
		case types.PublicKey(acc.AccountKey) != expectedKey:
			t.Fatalf("expected account key %s, got %s", expectedKey, acc.AccountKey)
		case acc.ServiceAccount != isService:
			t.Fatalf("expected service account %t, got %t", isService, acc.ServiceAccount)
		case uint64(acc.MaxPinnedData) != maxData:
			t.Fatalf("expected max data %d, got %d", maxData, acc.MaxPinnedData)
		}
	}

	pk1 := types.GeneratePrivateKey().PublicKey()
	err := store.AddAccount(context.Background(), pk1)
	if err != nil {
		t.Fatal(err)
	}

	pk2 := types.GeneratePrivateKey().PublicKey()
	err = store.AddServiceAccount(context.Background(), pk2)
	if err != nil {
		t.Fatal(err)
	}

	// fetch all
	accs, err := store.Accounts(context.Background(), 0, 2)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 2 {
		t.Fatal("unexpected accounts", accs)
	}
	assertAccount(t, accs[0], pk1, 0, false)
	assertAccount(t, accs[1], pk2, 0, true)

	// fetch all with limit and offset
	accs, err = store.Accounts(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("unexpected accounts", accs)
	}
	assertAccount(t, accs[0], pk2, 0, true)

	// fetch only user accounts
	accs, err = store.Accounts(context.Background(), 0, 2, accounts.WithServiceAccount(false))
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("unexpected accounts", accs)
	}
	assertAccount(t, accs[0], pk1, 0, false)

	// fetch only service accounts
	accs, err = store.Accounts(context.Background(), 0, 2, accounts.WithServiceAccount(true))
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("unexpected accounts", accs)
	}
	assertAccount(t, accs[0], pk2, 0, true)
}

func TestAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	pk := types.GeneratePrivateKey().PublicKey()
	if err := store.AddServiceAccount(context.Background(), pk); err != nil {
		t.Fatal(err)
	}

	acc, err := store.Account(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	} else if acc.AccountKey != proto.Account(pk) {
		t.Fatalf("expected public key %s, got %s", pk, acc.AccountKey)
	} else if !acc.ServiceAccount {
		t.Fatalf("expected service account to be true, got false")
	} else if acc.MaxPinnedData != 0 {
		t.Fatalf("expected max pinned data to be 0, got %d", acc.MaxPinnedData)
	}
}

func TestAddAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	test := func(t *testing.T, addAccount func(types.PublicKey) (bool, error)) {
		pk := types.GeneratePrivateKey().PublicKey()
		serviceAccount, err := addAccount(pk)
		if err != nil {
			t.Fatal(err)
		}
		_, err = addAccount(pk)
		if !errors.Is(err, accounts.ErrExists) {
			t.Fatal("expected ErrExists, got", err)
		}
		accs, err := store.Accounts(context.Background(), 0, 1, accounts.WithServiceAccount(serviceAccount))
		if err != nil {
			t.Fatal(err)
		} else if len(accs) != 1 || types.PublicKey(accs[0].AccountKey) != pk {
			t.Fatal("unexpected accounts", accs)
		}
		acc, err := store.Account(context.Background(), pk)
		if err != nil {
			t.Fatal(err)
		} else if acc.ServiceAccount != serviceAccount {
			t.Fatalf("expected service account %t, got %t", serviceAccount, acc.ServiceAccount)
		}
	}

	t.Run("user account", func(t *testing.T) {
		test(t, func(pk types.PublicKey) (bool, error) {
			if err := store.AddAccount(context.Background(), pk); err != nil {
				return false, err
			}
			return false, nil // 'false' for user account
		})
	})

	t.Run("service account", func(t *testing.T) {
		test(t, func(pk types.PublicKey) (bool, error) {
			if err := store.AddServiceAccount(context.Background(), pk); err != nil {
				return true, err
			}
			return true, nil // 'true' for service account
		})
	})
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
	} else if len(accs) != 1 || types.PublicKey(accs[0].AccountKey) != pk {
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

	// check service account deletion fails
	pk2 := types.GeneratePrivateKey().PublicKey()
	accs, err = store.Accounts(context.Background(), 0, 1, accounts.WithServiceAccount(true))
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 0 {
		t.Fatal("unexpected accounts", accs)
	}
	err = store.AddServiceAccount(context.Background(), pk2)
	if err != nil {
		t.Fatal(err)
	}
	err = store.DeleteAccount(context.Background(), pk2)
	if !errors.Is(err, accounts.ErrServiceAccount) {
		t.Fatal(err)
	}
	accs, err = store.Accounts(context.Background(), 0, 1, accounts.WithServiceAccount(true))
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("unexpected accounts", accs)
	}
}

func TestUpdateAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// define helper to check the database id
	dbID := func(ak types.PublicKey) (accID int64) {
		t.Helper()
		if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			return tx.QueryRow(ctx, `SELECT id FROM accounts WHERE public_key = $1`, sqlPublicKey(ak)).Scan(&accID)
		}); err != nil {
			t.Fatal(err)
		}
		return
	}

	// add an account
	pk1 := types.GeneratePrivateKey().PublicKey()
	err := store.AddAccount(context.Background(), pk1)
	if err != nil {
		t.Fatal(err)
	}
	db1 := dbID(pk1)

	// add another account
	pk2 := types.GeneratePrivateKey().PublicKey()
	err = store.AddAccount(context.Background(), pk2)
	if err != nil {
		t.Fatal(err)
	}

	// assert updating to an existing account fails
	err = store.UpdateAccount(context.Background(), pk1, pk2)
	if !errors.Is(err, accounts.ErrExists) {
		t.Fatal("expected [accounts.ErrExists], got", err)
	}

	// assert updating a non existing account fails
	pk3 := types.GeneratePrivateKey().PublicKey()
	err = store.UpdateAccount(context.Background(), types.GeneratePrivateKey().PublicKey(), pk3)
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected [accounts.ErrNotFound], got", err)
	}

	// update the account key
	err = store.UpdateAccount(context.Background(), pk1, pk3)
	if err != nil {
		t.Fatal(err)
	}

	// check the old account key is gone
	found, err := store.HasAccount(context.Background(), pk1)
	if err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("expected account to not exist")
	}

	// check the new account key exists
	found, err = store.HasAccount(context.Background(), pk3)
	if err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("expected account to exist")
	}

	// check db ids match
	if dbID(pk3) != db1 {
		t.Fatal("expected account id to have remained the same")
	}
}

func TestHasAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk := types.GeneratePrivateKey().PublicKey()
	found, err := store.HasAccount(context.Background(), pk)
	if err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("unexpected")
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

	// add two host
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

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

	// schedule h1 accounts for funding
	err = store.ScheduleAccountsForFunding(context.Background(), hk1)
	if err != nil {
		t.Fatal(err)
	}

	// assert both accounts are returned
	accounts, err = store.HostAccountsForFunding(context.Background(), hk1, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 2 {
		t.Fatal("expected two accounts")
	}
}

func TestUpdateHostAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host and an account
	hk := store.addTestHost(t)
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

// BenchmarkHostAccountsForFunding is a benchmark to ensure the performance of
// HostAccountsForFunding, we prepare the database with a (fixed) number of
// hosts and accounts once. Every iteration fetches two batches of accounts for
// funding, the first one only includes accounts for which there's no account
// host entry yet, the second one selects from the account_hosts table.
func BenchmarkHostAccountsForFunding(b *testing.B) {
	// define parameters
	const (
		batchSize = 1000 // equals max batch size in replenish RPC
		numHosts  = 1000
	)

	// initialize database
	store := initPostgres(b, zap.NewNop())

	// prune is a helper function to delete all rows from a table
	prune := func(table string) {
		b.Helper()

		if _, err := store.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s;`, table)); err != nil {
			b.Fatal(err)
		}
	}

	// insert hosts
	hosts := make([]types.PublicKey, 0, numHosts)
	hostIDs := make(map[types.PublicKey]int64, numHosts)
	if err := store.transaction(b.Context(), func(ctx context.Context, tx *txn) error {
		for range numHosts {
			var hostID int64
			hk := types.GeneratePrivateKey().PublicKey()
			err := tx.QueryRow(ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}
			hosts = append(hosts, hk)
			hostIDs[hk] = hostID
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// run benchmark for different number of accounts
	for _, numAccounts := range []int{10_000, 100_000, 1_000_000} {
		b.Logf("preparing database")

		// prepare accounts
		prune("accounts")
		if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			batch := &pgx.Batch{}
			for range numAccounts {
				pk := types.GeneratePrivateKey().PublicKey()
				batch.Queue(`INSERT INTO accounts (public_key) VALUES ($1);`, sqlPublicKey(pk))
			}
			return tx.SendBatch(ctx, batch).Close()
		}); err != nil {
			b.Fatal(err)
		}

		// prepare account hosts, ensure we have one batch per host
		prune("account_hosts")
		for _, hk := range hosts {
			var accs []accounts.HostAccount
			if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) (err error) {
				accs, err = newHostAccountsForFunding(context.Background(), tx, hk, hostIDs[hk], batchSize)
				return
			}); err != nil {
				b.Fatal(err)
			} else if err := store.UpdateHostAccounts(context.Background(), accs); err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprintf("%d_accounts", numAccounts), func(b *testing.B) {
			// sanity check b.N is never greater than the amount of hosts,
			// because in that case the benchmark results would be skewed
			if b.N > numHosts {
				b.Fatalf("too many iterations, %d > %d", b.N, numHosts)
			}
			for b.Loop() {
				hk := hosts[frand.Intn(numHosts)]
				hostID := hostIDs[hk]

				if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
					// fetch accounts without account_host entry
					if accounts, err := newHostAccountsForFunding(context.Background(), tx, hk, hostID, batchSize); err != nil {
						return err
					} else if len(accounts) != batchSize {
						return fmt.Errorf("expected %d new accounts, got %d", batchSize, len(accounts))
					}

					// fetch accounts with account_host entry
					if accounts, err := existingHostAccountsForFunding(context.Background(), tx, hk, hostID, batchSize); err != nil {
						return err
					} else if len(accounts) != batchSize {
						return fmt.Errorf("expected %d new accounts, got %d", batchSize, len(accounts))
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUpdateHostAccounts is a benchmark to ensure the performance of
// UpdateAccounts, every iteration performs the worst case update where every
// account gets inserted.
func BenchmarkUpdateHostAccounts(b *testing.B) {
	// define parameters
	const (
		batchSize   = 1000 // equals max batch size in replenish RPC
		numAccounts = 1000
		numHosts    = 1000
	)

	// prepare database
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	for range numAccounts {
		_, err := store.pool.Exec(context.Background(), `INSERT INTO accounts (public_key) VALUES ($1);`, sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
		if err != nil {
			b.Fatal(err)
		}
	}
	var hosts []types.PublicKey
	for range numHosts {
		hosts = append(hosts, types.GeneratePrivateKey().PublicKey())
		_, err := store.pool.Exec(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hosts[len(hosts)-1]))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		accounts, err := store.HostAccountsForFunding(context.Background(), hosts[i%numHosts], batchSize)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		err = store.UpdateHostAccounts(context.Background(), accounts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestServiceAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// setup
	account := types.GeneratePrivateKey().PublicKey()
	err := store.AddAccount(context.Background(), account)
	if err != nil {
		t.Fatal(err)
	}
	hk := types.GeneratePrivateKey().PublicKey()
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add account
	err = store.AddAccount(context.Background(), hk)
	if err != nil {
		t.Fatal(err)
	}

	// account not found
	_, err = store.ServiceAccountBalance(context.Background(), hk, proto.Account(account))
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal(err)
	}
	err = store.DebitServiceAccount(context.Background(), hk, proto.Account(account), types.Siacoins(1))
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal(err)
	}

	// helper to assert balance
	assertBalance := func(expected types.Currency) {
		t.Helper()
		balance, err := store.ServiceAccountBalance(context.Background(), hk, proto.Account(account))
		if err != nil {
			t.Fatal(err)
		} else if !balance.Equals(expected) {
			t.Fatal("unexpected balance", balance)
		}
	}

	// set account balance
	expectedBalance := types.Siacoins(1)
	err = store.UpdateServiceAccountBalance(context.Background(), hk, proto.Account(account), expectedBalance)
	if err != nil {
		t.Fatal(err)
	}

	// fetch balance
	assertBalance(expectedBalance)

	// withdraw from it
	withdrawal := types.Siacoins(1).Div64(10)         // 0.1SC
	expectedBalance = expectedBalance.Sub(withdrawal) // 0.9SC
	if err := store.DebitServiceAccount(context.Background(), hk, proto.Account(account), withdrawal); err != nil {
		t.Fatal(err)
	}
	assertBalance(expectedBalance)

	// withdraw more than the balance
	if err := store.DebitServiceAccount(context.Background(), hk, proto.Account(account), types.Siacoins(100)); err != nil {
		t.Fatal(err)
	}
	assertBalance(types.ZeroCurrency)
}

// BenchmarkServiceAccounts benchmarks the service account related methods
func BenchmarkServiceAccounts(b *testing.B) {
	// define parameters
	const (
		numAccounts = 10000
		numHosts    = 1000
	)

	// prepare database
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	for range numAccounts {
		_, err := store.pool.Exec(context.Background(), `INSERT INTO accounts (public_key) VALUES ($1);`, sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
		if err != nil {
			b.Fatal(err)
		}
	}

	account := proto.Account(types.GeneratePrivateKey().PublicKey())
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal(err)
	}

	var hosts []types.PublicKey
	for range numHosts {
		hk := types.GeneratePrivateKey().PublicKey()
		hosts = append(hosts, hk)
		_, err := store.pool.Exec(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hk))
		if err != nil {
			b.Fatal(err)
		}

		// assume that the account is funded with every host
		err = store.UpdateServiceAccountBalance(context.Background(), hk, account, types.Siacoins(1))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("UpdateServiceAccountBalance", func(b *testing.B) {
		for b.Loop() {
			hk := hosts[frand.Intn(numHosts)]
			err := store.UpdateServiceAccountBalance(context.Background(), hk, account, types.NewCurrency64(frand.Uint64n(math.MaxUint64)))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DebitServiceAccount", func(b *testing.B) {
		for b.Loop() {
			hk := hosts[frand.Intn(numHosts)]
			err := store.DebitServiceAccount(context.Background(), hk, account, types.NewCurrency64(1))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ServiceAccountBalance", func(b *testing.B) {
		for b.Loop() {
			hk := hosts[frand.Intn(numHosts)]
			_, err := store.ServiceAccountBalance(context.Background(), hk, account)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
