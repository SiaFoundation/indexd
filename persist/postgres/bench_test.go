package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// BenchmarkHostAccountsForFunding is a benchmark to ensure the performance of
// HostAccountsForFunding, we prepare the database with a (fixed) number of
// hosts and accounts once. Every iteration fetches two batches of accounts for
// funding, the first one only includes accounts for which there's no account
// host entry yet, the second one selects from the account_hosts table.
//
// M1 Max | 10k accounts  | 1k hosts | 5ms/op
// M1 Max | 100k accounts | 1k hosts | 15ms/op
// M1 Max | 1M accounts   | 1k hosts | 16ms/op
func BenchmarkHostAccountsForFunding(b *testing.B) {
	// define parameters
	const (
		batchSize = 1000 // equals max batch size in replenish RPC
		numHosts  = 1000
	)

	// initialize database
	store := initPostgres(b, zap.NewNop())

	// prepare helpers
	assertCount := func(table string, expected int) {
		b.Helper()
		var got int
		if err := store.pool.QueryRow(context.Background(), fmt.Sprintf(`SELECT COUNT(*) FROM %s;`, table)).Scan(&got); err != nil {
			b.Fatal(err)
		} else if got != expected {
			b.Fatalf("expected %d %s, got %d", expected, table, got)
		}
	}
	prune := func(tables ...string) {
		b.Helper()
		for _, table := range tables {
			if _, err := store.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s;`, table)); err != nil {
				b.Fatal(err)
			}
		}
	}

	// run benchmark for different number of accounts
	for _, numAccounts := range []int{10_000, 100_000, 1_000_000} {
		var once sync.Once
		var hosts []types.PublicKey

		b.Run(fmt.Sprintf("%d_accounts", numAccounts), func(b *testing.B) {
			// prepare the database once, this ensures that we don't have to
			// recreate the setup for different values of b.N
			once.Do(func() {
				b.Logf("preparing database")
				prune("accounts", "hosts", "account_hosts")

				accounts := make([]any, numAccounts)
				for i := range accounts {
					accounts[i] = sqlPublicKey(types.GeneratePrivateKey().PublicKey())
				}

				// next prepares the next batch of accounts for insertion
				next := func() (string, []any) {
					batchSize := min(len(accounts), 5000)
					batch := accounts[:batchSize]
					accounts = accounts[batchSize:]

					var values []string
					for i := range batch {
						values = append(values, fmt.Sprintf("($%d)", i+1))
					}
					return fmt.Sprintf("INSERT INTO accounts (public_key) VALUES %s", strings.Join(values, ",")), batch
				}

				start := time.Now()
				if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
					for len(accounts) > 0 {
						query, args := next()
						_, err := tx.Exec(ctx, query, args...)
						if err != nil {
							return fmt.Errorf("failed to insert accounts: %w", err)
						} else if time.Since(start) > 10*time.Second {
							b.Logf("%d accounts remaining", len(accounts))
							start = time.Now()
						}
					}

					start = time.Now()
					for i := range numHosts {
						hosts = append(hosts, types.GeneratePrivateKey().PublicKey())
						_, err := tx.Exec(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hosts[len(hosts)-1]))
						if err != nil {
							return err
						} else if time.Since(start) > 10*time.Second {
							b.Logf("%d hosts remaining", numHosts-i)
							start = time.Now()
						}
					}

					return nil
				}); err != nil {
					b.Fatal(err)
				}

				// update one batch of host accounts to ensure we have one batch
				// of account_host entries per host, this'll allow us to fetch
				// accounts for funding in a way that we cover both parts of the
				// query
				start = time.Now()
				for i, hk := range hosts {
					accounts, err := store.HostAccountsForFunding(context.Background(), hk, batchSize, true)
					if err != nil {
						b.Fatal(err)
					}
					for i := range accounts {
						accounts[i].NextFund = time.Now().Add(-time.Minute)
					}
					if err := store.UpdateHostAccounts(context.Background(), accounts); err != nil {
						b.Fatal(err)
					} else if time.Since(start) > 10*time.Second {
						b.Logf("%d host account batches remaining", numHosts-i)
						start = time.Now()
					}
				}

				b.Logf("database is ready")
			})

			// perform sanity checks
			assertCount("accounts", numAccounts)
			assertCount("hosts", numHosts)

			start := time.Now()
			b.Logf("b.N=%d", b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%100 == 0 && i > 0 {
					b.Logf("%d iterations in %s", i, time.Since(start))
				}

				// fetch accounts without account_host entry
				accounts, err := store.HostAccountsForFunding(context.Background(), hosts[i%numHosts], batchSize, true)
				if err != nil {
					b.Fatal(err)
				} else if len(accounts) != batchSize {
					b.Fatalf("expected %d accounts, got %d", batchSize, len(accounts))
				}

				// fetch accounts with account_host entry
				accounts, err = store.HostAccountsForFunding(context.Background(), hosts[i%numHosts], batchSize, false)
				if err != nil {
					b.Fatal(err)
				} else if len(accounts) != batchSize {
					b.Fatalf("expected %d accounts, got %d", batchSize, len(accounts))
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
		accounts, err := store.HostAccountsForFunding(context.Background(), hosts[i%numHosts], batchSize, true)
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
