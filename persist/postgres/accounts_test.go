package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
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

// BenchmarkHostAccountsForFunding is a benchmark to ensure the performance of
// HostAccountsForFunding, we prepare the database with a (fixed) number of
// hosts and accounts once. Every iteration fetches two batches of accounts for
// funding, the first one only includes accounts for which there's no account
// host entry yet, the second one selects from the account_hosts table.
//
// M1 Max | 10k accounts  | 1k hosts | 4ms/op
// M1 Max | 100k accounts | 1k hosts | 12ms/op
// M1 Max | 1M accounts   | 1k hosts | 16ms/op
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
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		for range numHosts {
			var hostID int64
			hk := types.GeneratePrivateKey().PublicKey()
			err := tx.QueryRow(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
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
		aks := make([]any, numAccounts)
		for i := range aks {
			aks[i] = sqlPublicKey(types.GeneratePrivateKey().PublicKey())
		}
		if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			for len(aks) > 0 {
				batchSize := min(len(aks), 5000)
				batch := aks[:batchSize]
				aks = aks[batchSize:]

				var values []string
				for i := range batch {
					values = append(values, fmt.Sprintf("($%d)", i+1))
				}

				_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO accounts (public_key) VALUES %s", strings.Join(values, ",")), batch...)
				if err != nil {
					return fmt.Errorf("failed to insert accounts: %w", err)
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}

		// prepare account hosts, ensure we have one batch per host
		prune("account_hosts")
		for _, hk := range hosts {
			var accs []accounts.HostAccount
			if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) (err error) {
				accs, err = store.newHostAccountsForFunding(context.Background(), tx, hk, hostIDs[hk], batchSize)
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
			for i := 0; i < b.N; i++ {
				hk := hosts[i%numHosts]
				hostID := hostIDs[hk]

				if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
					// fetch accounts without account_host entry
					if accounts, err := store.newHostAccountsForFunding(context.Background(), tx, hk, hostID, batchSize); err != nil {
						return err
					} else if len(accounts) != batchSize {
						return fmt.Errorf("expected %d new accounts, got %d", batchSize, len(accounts))
					}

					// fetch accounts with account_host entry
					if accounts, err := store.existingHostAccountsForFunding(context.Background(), tx, hk, hostID, batchSize); err != nil {
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
//
// M1 Max | 1k accounts | 14 ms/op
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
	for i := 0; i < b.N; i++ {
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
