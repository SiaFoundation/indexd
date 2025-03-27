package postgres

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// BenchmarkAccountsForFunding is a benchmark to ensure the performance of
// AccountsForFunding, which ensures a host account exists for the given host
// and every account in the database and then goes on to return all accounts
// with a next scan in the past.
//
// M1 Max | 1_000 accounts | 17.46 ms/op
// M1 Max | 10_000 accounts | 106.27 ms/op
func BenchmarkAccountsForFunding(b *testing.B) {
	// define parameters
	const numAccounts = 10_000

	// prepare database
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	for range numAccounts {
		_, err := store.pool.Exec(context.Background(), `INSERT INTO accounts (public_key) VALUES ($1);`, sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		hk := types.GeneratePrivateKey().PublicKey()
		_, err := store.pool.Exec(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hk))
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		_, err = store.AccountsForFunding(context.Background(), hk, numAccounts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdateAccounts is a benchmark to ensure the performance of
// UpdateAccounts, which updates the consecutive failed funds and next fund
// time for a list of accounts.
//
// M1 Max | 1M entries | 1000 updates | ~5 ms/op | ~2 KB/op
func BenchmarkUpdateAccounts(b *testing.B) {
	// define parameters
	const (
		numAccounts     = 1000
		numHosts        = 1000
		updateBatchSize = 1000
	)

	// prepare database
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	for range numAccounts {
		_, err := store.pool.Exec(context.Background(), `INSERT INTO accounts (public_key) VALUES ($1);`, sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
		if err != nil {
			b.Fatal(err)
		}
	}
	for range numHosts {
		hk := types.GeneratePrivateKey().PublicKey()
		_, err := store.pool.Exec(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hk))
		if err != nil {
			b.Fatal(err)
		}
	}
	_, err := store.pool.Exec(context.Background(), `
INSERT INTO account_hosts (account_id, host_id)
SELECT a.id, h.id 
FROM accounts a 
CROSS JOIN hosts h ON CONFLICT DO NOTHING;`)
	if err != nil {
		b.Fatal(err)
	}

	// read accounts
	var accs []accounts.Account
	rows, err := store.pool.Query(context.Background(), `
SELECT a.public_key, h.public_key, ea.consecutive_failed_funds, ea.next_fund
FROM account_hosts ea
INNER JOIN hosts h ON h.id = ea.host_id
INNER JOIN accounts a ON a.id = ea.account_id`)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var acc accounts.Account
		if err := rows.Scan((*sqlPublicKey)(&acc.AccountKey), (*sqlPublicKey)(&acc.HostKey), &acc.ConsecutiveFailedFunds, &acc.NextFund); err != nil {
			b.Fatal(err)
		}
		accs = append(accs, acc)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		frand.Shuffle(len(accs), func(i, j int) { accs[i], accs[j] = accs[j], accs[i] })
		batch := accs[:updateBatchSize]
		for i := range batch {
			batch[i].ConsecutiveFailedFunds = frand.Intn(1e6)
			batch[i].NextFund = time.Now().Add(time.Duration(frand.Uint64n(1e6))).Round(time.Microsecond)
		}
		b.StartTimer()

		err = store.UpdateAccounts(context.Background(), accs)
		if err != nil {
			b.Fatal(err)
		}
	}
}
