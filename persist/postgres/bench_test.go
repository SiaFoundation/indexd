package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// BenchmarkContractsForFunding is a benchmark to ensure the performance of
// ContractsForFunding, we prepare the database with a certain number of
// contracts per host, with a random state, remaining allowance and good status.
//
// M1 Max | 100k contracts | 1.3ms/op
func BenchmarkContractsForFunding(b *testing.B) {
	const (
		numContractsPerHost = 100
		numHosts            = 1000
	)

	// prepare database
	store := initPostgres(b, zap.NewNop())
	hosts := make([]types.PublicKey, 0, numHosts)
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		for range numHosts {
			var hostID int64
			hk := types.GeneratePrivateKey().PublicKey()
			err := tx.QueryRow(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}

			for range numContractsPerHost {
				var id types.FileContractID
				frand.Read(id[:])
				if _, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, proof_height, expiration_height, contract_price, initial_allowance, miner_fee, total_collateral, remaining_allowance, state, good) VALUES ($1, $2, 0, 0, $3, $4, $5, $6, $7, $8, $9)`,
					hostID,
					sqlHash256(id),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.NewCurrency64(frand.Uint64n(5))), // random remaining allowance
					sqlContractState(uint8(frand.Uint64n(5))),          // random state
					frand.Uint64n(2) == 0,                              // random good
				); err != nil {
					return err
				}
			}

			hosts = append(hosts, hk)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if _, err := store.ContractsForFunding(context.Background(), hosts[b.N%numHosts], 50); err != nil {
			b.Fatal(err)
		}
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
