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
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func (s *Store) addTestAccount(t testing.TB, ak types.PublicKey, opts ...accounts.AddAccountOption) {
	connectKey := fmt.Sprintf("test-connect-key-%x", frand.Bytes(8))
	_, err := s.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         connectKey,
		Description: "test connect key",
		Quota:       "default",
	})
	if err != nil {
		t.Fatalf("failed to add app connect key: %v", err)
	}

	err = s.transaction(func(ctx context.Context, tx *txn) error {
		if err := addAccount(ctx, tx, connectKey, ak, accounts.AppMeta{}, opts...); err != nil {
			return fmt.Errorf("failed to add account: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to add account: %v", err)
	}
}

func TestAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	assertAccount := func(t *testing.T, acc accounts.Account, expectedKey types.PublicKey, maxData uint64) {
		t.Helper()
		switch {
		case types.PublicKey(acc.AccountKey) != expectedKey:
			t.Fatalf("expected account key %s, got %s", expectedKey, acc.AccountKey)
		case uint64(acc.MaxPinnedData) != maxData:
			t.Fatalf("expected max data %d, got %d", maxData, acc.MaxPinnedData)
		case acc.Ready:
			t.Fatal("expected account to be not ready")
		}
	}

	pk1 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccount(t, pk1)

	pk3 := types.GeneratePrivateKey().PublicKey()
	store.addTestAccount(t, pk3, accounts.WithMaxPinnedData(100))

	// fetch only user accounts
	accs, err := store.Accounts(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 2 {
		t.Fatal("unexpected accounts", accs)
	}
	assertAccount(t, accs[0], pk1, math.MaxInt64)
	assertAccount(t, accs[1], pk3, 100)

	// add accounts associated with connect key
	// create a test quota with specific limits
	store.addTestQuota(t, "test-100", 100, 10)

	const connectKey = "foobar"
	if _, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         connectKey,
		Description: "test connect key",
		Quota:       "test-100",
	}); err != nil {
		t.Fatal("failed to add app connect key:", err)
	}

	pk4 := types.GeneratePrivateKey().PublicKey()
	if err := store.RegisterAppKey(connectKey, pk4, accounts.AppMeta{}); err != nil {
		t.Fatal("failed to use app connect key:", err)
	}
	pk5 := types.GeneratePrivateKey().PublicKey()
	if err := store.RegisterAppKey(connectKey, pk5, accounts.AppMeta{}); err != nil {
		t.Fatal("failed to use app connect key:", err)
	}

	accs, err = store.Accounts(0, 10, accounts.WithConnectKey(connectKey))
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 2 {
		t.Fatal("unexpected accounts", accs)
	}
	assertAccount(t, accs[0], pk4, math.MaxInt64)
	assertAccount(t, accs[1], pk5, math.MaxInt64)

	_, err = store.Accounts(0, 10, accounts.WithConnectKey("invalidkey"))
	if !errors.Is(err, accounts.ErrKeyNotFound) {
		t.Fatalf("expected %q, got %q", accounts.ErrKeyNotFound, err)
	}
}

func TestAccountReady(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	ak := types.GeneratePrivateKey().PublicKey()
	store.addTestAccount(t, ak)

	assertReady := func(t *testing.T, expected bool) {
		t.Helper()

		acc, err := store.Account(ak)
		if err != nil {
			t.Fatal(err)
		} else if acc.Ready != expected {
			t.Fatalf("expected account ready=%v, got %v", expected, acc.Ready)
		}

		accs, err := store.Accounts(0, 10)
		if err != nil {
			t.Fatal(err)
		} else if len(accs) != 1 {
			t.Fatalf("expected 1 account, got %d", len(accs))
		} else if accs[0].Ready != expected {
			t.Fatalf("expected listed account ready=%v, got %v", expected, accs[0].Ready)
		}
	}

	assertReady(t, false)

	hostAccs := make([]accounts.HostAccount, 0, accounts.ReadyHostThreshold)
	for range accounts.ReadyHostThreshold {
		hostAccs = append(hostAccs, accounts.HostAccount{
			AccountKey: proto.Account(ak),
			HostKey:    store.addTestHost(t),
			NextFund:   time.Now(),
		})
	}
	hostAccs[len(hostAccs)-1].ConsecutiveFailedFunds = 1

	if err := store.UpdateHostAccounts(hostAccs); err != nil {
		t.Fatal(err)
	}
	assertReady(t, false)

	hostAccs[len(hostAccs)-1].ConsecutiveFailedFunds = 0
	if err := store.UpdateHostAccounts(hostAccs[len(hostAccs)-1:]); err != nil {
		t.Fatal(err)
	}
	assertReady(t, true)
}

func TestAddAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	test := func(t *testing.T, addAccount func(types.PublicKey, accounts.AppMeta, ...accounts.AddAccountOption) error) {
		pk := types.GeneratePrivateKey().PublicKey()
		appID := frand.Entropy256()
		err := addAccount(pk, accounts.AppMeta{
			ID:          appID,
			Name:        "name",
			Description: "description",
			LogoURL:     "logoURL",
			ServiceURL:  "serviceURL",
		},
			accounts.WithMaxPinnedData(1000),
		)
		if err != nil {
			t.Fatal(err)
		}
		err = addAccount(pk, accounts.AppMeta{})
		if !errors.Is(err, accounts.ErrExists) {
			t.Fatal("expected ErrExists, got", err)
		}
		accs, err := store.Accounts(0, 1)
		if err != nil {
			t.Fatal(err)
		} else if len(accs) != 1 || types.PublicKey(accs[0].AccountKey) != pk {
			t.Fatal("unexpected accounts", accs)
		}
		acc, err := store.Account(pk)
		if err != nil {
			t.Fatal(err)
		} else if acc.App.ID != appID {
			t.Fatalf("expected app ID %s, got %s", appID, acc.App.ID)
		} else if acc.App.Name != "name" {
			t.Fatalf("expected name %q, got %q", "name", acc.App.Name)
		} else if acc.App.Description != "description" {
			t.Fatalf("expected description %q, got %q", "description", acc.App.Description)
		} else if acc.App.LogoURL != "logoURL" {
			t.Fatalf("expected logo URL %q, got %q", "logoURL", acc.App.LogoURL)
		} else if acc.App.ServiceURL != "serviceURL" {
			t.Fatalf("expected service URL %q, got %q", "serviceURL", acc.App.ServiceURL)
		} else if acc.MaxPinnedData != 1000 {
			t.Fatalf("expected max pinned data %d, got %d", 1000, acc.MaxPinnedData)
		}
	}

	t.Run("user account", func(t *testing.T) {
		connectKey := fmt.Sprintf("test-connect-key-%x", frand.Bytes(8))
		_, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
			Key:         connectKey,
			Description: "test connect key",
			Quota:       "default",
		})
		if err != nil {
			t.Fatal("failed to add app connect key:", err)
		}
		test(t, func(pk types.PublicKey, meta accounts.AppMeta, opts ...accounts.AddAccountOption) error {
			return store.transaction(func(ctx context.Context, tx *txn) error {
				if err := addAccount(ctx, tx, connectKey, pk, meta, opts...); err != nil {
					return fmt.Errorf("failed to add account: %w", err)
				}
				return nil
			})
		})
	})
}

func TestDeleteAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk := types.GeneratePrivateKey().PublicKey()
	err := store.DeleteAccount(proto.Account(pk))
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected [accounts.ErrNotFound]")
	}

	store.addTestAccount(t, pk)

	found, err := store.HasAccount(pk)
	if err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("expected account to exist")
	}

	accs, err := store.Accounts(0, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 || types.PublicKey(accs[0].AccountKey) != pk {
		t.Fatal("unexpected accounts", accs)
	}

	err = store.DeleteAccount(proto.Account(pk))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PruneAccounts(1); err != nil {
		t.Fatal(err)
	}

	accs, err = store.Accounts(0, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 0 {
		t.Fatal("unexpected accounts", accs)
	}
}

func TestHasAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	pk := types.GeneratePrivateKey().PublicKey()
	found, err := store.HasAccount(pk)
	if err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("unexpected")
	}
	store.addTestAccount(t, pk)

	found, err = store.HasAccount(pk)
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
		if err := store.transaction(func(ctx context.Context, tx *txn) error {
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
	threshold := time.Now().Add(-time.Hour)
	accs, err := store.HostAccountsForFunding(hk1, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 0 {
		t.Fatal("expected no accounts")
	}

	// add an account
	ak1 := types.PublicKey{1, 1}
	store.addTestAccount(t, ak1)

	// assert there's now one account to fund
	accs, err = store.HostAccountsForFunding(hk1, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("expected one account")
	} else if accs[0].AccountKey != proto.Account(ak1) {
		t.Fatal("unexpected account key")
	} else if accs[0].HostKey != hk1 {
		t.Fatal("unexpected host key")
	} else if accs[0].ConsecutiveFailedFunds != 0 {
		t.Fatal("unexpected consecutive failed funds")
	} else if accs[0].NextFund.IsZero() {
		t.Fatal("unexpected next fund")
	}

	// assert there's no EAs
	if n := numEAs(); n != 0 {
		t.Fatal("expected no account-host entries", n)
	}

	// update next fund
	accs[0].NextFund = time.Now().Add(time.Hour)
	if err := store.UpdateHostAccounts(accs); err != nil {
		t.Fatal(err)
	}

	// assert the update inserted the account
	if n := numEAs(); n != 1 {
		t.Fatal("expected one account-host entry", n)
	}

	// assert there are no accounts to fund
	accs, err = store.HostAccountsForFunding(hk1, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 0 {
		t.Fatal("expected no accounts")
	}

	// add another account
	ak2 := types.PublicKey{2, 2}
	store.addTestAccount(t, ak2)

	// assert h1 has one account to fund
	accs, err = store.HostAccountsForFunding(hk1, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("expected one account")
	} else if accs[0].AccountKey != proto.Account(ak2) {
		t.Fatal("unexpected account key")
	} else if err := store.UpdateHostAccounts(accs); err != nil {
		t.Fatal(err)
	}

	// assert h2 has two accounts to fund
	accs, err = store.HostAccountsForFunding(hk2, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 2 {
		t.Fatal("expected two accounts")
	} else if err := store.UpdateHostAccounts(accs); err != nil {
		t.Fatal(err)
	}

	// assert limit is applied
	accs, err = store.HostAccountsForFunding(hk2, "default", threshold, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("expected one accounts")
	}

	// assert the updates inserted all accounts
	if n := numEAs(); n != 4 {
		t.Fatal("expected 4 account-host entries, got", n)
	}

	// schedule h1 accounts for funding
	err = store.ScheduleAccountsForFunding(hk1)
	if err != nil {
		t.Fatal(err)
	}

	// if we raise threshold neither account should be returned
	accs, err = store.HostAccountsForFunding(hk1, "default", threshold.Add(2*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 0 {
		t.Fatal("expected zero accounts")
	}

	// assert both accounts are returned
	accs, err = store.HostAccountsForFunding(hk1, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 2 {
		t.Fatal("expected two accounts")
	}

	// set the next funding time for all accounts of hk1 into the future and
	// schedule a refill for ak1
	for i := range accs {
		accs[i].NextFund = time.Now().Add(time.Hour)
	}
	if err := store.UpdateHostAccounts(accs); err != nil {
		t.Fatal(err)
	} else if err := store.ScheduleAccountForFunding(hk1, proto.Account(ak1)); err != nil {
		t.Fatal(err)
	}

	// only ak1 should be returned
	accs, err = store.HostAccountsForFunding(hk1, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accs) != 1 {
		t.Fatal("expected one account")
	} else if accs[0].AccountKey != proto.Account(ak1) {
		t.Fatal("unexpected account")
	}
}

func TestUpdateHostAccounts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host and an account
	hk := store.addTestHost(t)
	ak := types.GeneratePrivateKey().PublicKey()
	store.addTestAccount(t, ak)

	// fetch accounts for funding
	threshold := time.Now().Add(-time.Hour)
	accounts, err := store.HostAccountsForFunding(hk, "default", threshold, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("expected one account")
	}
	accounts[0].ConsecutiveFailedFunds = frand.Intn(1e3)
	accounts[0].NextFund = time.Now().Add(time.Duration(frand.Uint64n(1e6))).Round(time.Microsecond)

	// update the account
	err = store.UpdateHostAccounts(accounts)
	if err != nil {
		t.Fatal(err)
	}

	// assert the account was upserted
	var updatedFailures int
	var updatedNextFund time.Time
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
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
	prune := func(table ...string) {
		b.Helper()

		for _, t := range table {
			if _, err := store.pool.Exec(b.Context(), fmt.Sprintf(`DELETE FROM %s;`, t)); err != nil {
				b.Fatal(err)
			}
		}
	}

	// insert hosts
	hosts := make([]types.PublicKey, 0, numHosts)
	hostIDs := make(map[types.PublicKey]int64, numHosts)
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		hosts = hosts[:0] // reuse same slice if transaction retries
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

	// create a connect key for benchmark accounts
	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "benchmark-connect-key",
		Description: "benchmark connect key",
		Quota:       "default",
	})
	if err != nil {
		b.Fatal(err)
	}

	// run benchmark for different number of accounts
	threshold := time.Now().Add(-time.Hour)
	for _, numAccounts := range []int{10_000, 100_000, 1_000_000} {
		// prepare accounts
		prune("account_slabs", "accounts")
		if err := store.transaction(func(ctx context.Context, tx *txn) error {
			var connectKeyID int64
			if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
				return err
			}
			batch := &pgx.Batch{}
			for range numAccounts {
				pk := types.GeneratePrivateKey().PublicKey()
				batch.Queue(`INSERT INTO accounts (public_key, connect_key_id, max_pinned_data) VALUES ($1, $2, 1000000);`, sqlPublicKey(pk), connectKeyID)
			}
			return tx.SendBatch(ctx, batch).Close()
		}); err != nil {
			b.Fatal(err)
		}

		// prepare account hosts, ensure we have one batch per host
		prune("account_hosts")
		for _, hk := range hosts {
			var accs []accounts.HostAccount
			if err := store.transaction(func(ctx context.Context, tx *txn) (err error) {
				accs, err = newHostAccountsForFunding(ctx, tx, hk, hostIDs[hk], "default", threshold, batchSize)
				return
			}); err != nil {
				b.Fatal(err)
			} else if err := store.UpdateHostAccounts(accs); err != nil {
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

				if err := store.transaction(func(ctx context.Context, tx *txn) error {
					// fetch accounts without account_host entry
					if accounts, err := newHostAccountsForFunding(ctx, tx, hk, hostID, "default", threshold, batchSize); err != nil {
						return err
					} else if len(accounts) != batchSize {
						return fmt.Errorf("expected %d new accounts, got %d", batchSize, len(accounts))
					}

					// fetch accounts with account_host entry
					if accounts, err := existingHostAccountsForFunding(ctx, tx, hk, hostID, "default", threshold, batchSize); err != nil {
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
	store := initPostgres(b, zap.NewNop())

	// create a connect key for benchmark accounts
	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "benchmark-connect-key",
		Description: "benchmark connect key",
		Quota:       "default",
	})
	if err != nil {
		b.Fatal(err)
	}

	var connectKeyID int64
	if err := store.pool.QueryRow(b.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
		b.Fatal(err)
	}

	for range numAccounts {
		_, err := store.pool.Exec(b.Context(), `INSERT INTO accounts (public_key, connect_key_id, max_pinned_data) VALUES ($1, $2, 1000000);`, sqlPublicKey(types.GeneratePrivateKey().PublicKey()), connectKeyID)
		if err != nil {
			b.Fatal(err)
		}
	}
	var hosts []types.PublicKey
	for range numHosts {
		hosts = append(hosts, types.GeneratePrivateKey().PublicKey())
		_, err := store.pool.Exec(b.Context(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hosts[len(hosts)-1]))
		if err != nil {
			b.Fatal(err)
		}
	}

	threshold := time.Now().Add(-time.Hour)
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		accounts, err := store.HostAccountsForFunding(hosts[i%numHosts], "default", threshold, batchSize)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		err = store.UpdateHostAccounts(accounts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPruneAccount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// setup
	acc1 := proto.Account(types.GeneratePrivateKey().PublicKey())
	acc2 := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc1))
	store.addTestAccount(t, types.PublicKey(acc2))

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// add objects for both accounts
	randomSlabs := func(n int) []slabs.SlabPinParams {
		s := make([]slabs.SlabPinParams, n)
		for i := range s {
			s[i] = slabs.SlabPinParams{
				EncryptionKey: frand.Entropy256(),
				MinShards:     1,
				Sectors: []slabs.PinnedSector{
					{
						Root:    frand.Entropy256(),
						HostKey: hk,
					},
				},
			}
		}
		return s
	}

	pinSlabs := func(acc proto.Account, params []slabs.SlabPinParams) []slabs.SlabSlice {
		t.Helper()

		var ss []slabs.SlabSlice
		for _, p := range params {
			_, err := store.PinSlabs(acc, time.Time{}, p)
			if err != nil {
				t.Fatal(err)
			}
			ss = append(ss, p.Slice(10, 120))
		}
		return ss
	}

	// pin two objects to each account
	obj1Slabs := randomSlabs(3)
	pinSlabs(acc1, obj1Slabs)
	pinSlabs(acc2, obj1Slabs)
	obj1Acc1 := store.pinRandomObject(t, acc1, pinSlabs(acc1, obj1Slabs))

	obj1Acc2 := obj1Acc1
	obj1Acc2.EncryptedDataKey = frand.Bytes(72)
	obj1Acc2.DataSignature = (types.Signature)(frand.Bytes(64))
	obj1Acc2.EncryptedMetadataKey = frand.Bytes(72)
	obj1Acc2.MetadataSignature = (types.Signature)(frand.Bytes(64))
	if err := store.PinObject(acc2, obj1Acc2.PinRequest()); err != nil {
		t.Fatal(err)
	}

	obj2Slabs := randomSlabs(3)
	pinSlabs(acc1, obj2Slabs)
	pinSlabs(acc2, obj2Slabs)
	obj2Acc1 := store.pinRandomObject(t, acc1, pinSlabs(acc1, obj2Slabs))

	obj2Acc2 := obj2Acc1
	obj2Acc2.EncryptedDataKey = frand.Bytes(72)
	obj2Acc2.DataSignature = (types.Signature)(frand.Bytes(64))
	obj2Acc2.EncryptedMetadataKey = frand.Bytes(72)
	obj2Acc2.MetadataSignature = (types.Signature)(frand.Bytes(64))
	if err := store.PinObject(acc2, obj2Acc2.PinRequest()); err != nil {
		t.Fatal(err)
	}

	assertObjects := func(acc proto.Account, expected int) {
		t.Helper()

		var got int
		err := store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM objects WHERE account_id = (SELECT id FROM accounts WHERE public_key = $1)`, sqlPublicKey(acc)).Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if expected != got {
			t.Fatalf("expected %d objects, got %d", expected, got)
		}
	}

	assertObjects(acc1, 2)
	assertObjects(acc2, 2)

	if err := store.DeleteAccount(acc1); err != nil {
		t.Fatal(err)
	}

	assertObjects(acc1, 2)
	assertObjects(acc2, 2)

	// should delete 1 object on acc1
	if err := store.PruneAccounts(1); err != nil {
		t.Fatal(err)
	}

	assertObjects(acc1, 1)
	assertObjects(acc2, 2)

	// should delete last object on acc1
	if err := store.PruneAccounts(1); err != nil {
		t.Fatal(err)
	}

	// delete all the slabs (6) and and thus delete acc1
	if err := store.PruneAccounts(10); err != nil {
		t.Fatal(err)
	}

	// acc1 should be deleted now so calling again will result in error
	if err := store.PruneAccounts(1); !errors.Is(err, accounts.ErrNotFound) {
		t.Fatalf("expected %v, got %v", accounts.ErrNotFound, err)
	}
	assertObjects(acc2, 2)

	if err := store.DeleteAccount(acc2); err != nil {
		t.Fatal(err)
	}

	// this should delete all 2 of acc2's objects and acc2 at once
	if err := store.PruneAccounts(10); err != nil {
		t.Fatal(err)
	}

	// acc2 should be deleted now so calling again will result in error
	if err := store.PruneAccounts(1); !errors.Is(err, accounts.ErrNotFound) {
		t.Fatalf("expected %v, got %v", accounts.ErrNotFound, err)
	}
}

func TestAccountFundingInfo(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create a connect key for test accounts
	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "test-connect-key",
		Description: "test connect key",
		Quota:       "default",
	})
	if err != nil {
		t.Fatal(err)
	}

	var connectKeyID int64
	if err := store.pool.QueryRow(t.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	insert := func(d time.Duration) {
		lastUsed := now.Add(-d)
		if _, err := store.pool.Exec(
			t.Context(),
			`INSERT INTO accounts (public_key, connect_key_id, last_used, max_pinned_data) VALUES ($1, $2, $3, 1000000);`,
			sqlPublicKey(types.GeneratePrivateKey().PublicKey()),
			connectKeyID,
			lastUsed,
		); err != nil {
			t.Fatal(err)
		}
	}

	totalActive := func(infos []accounts.QuotaFundInfo) uint64 {
		var total uint64
		for _, info := range infos {
			total += info.ActiveAccounts
		}
		return total
	}

	// only 3 accounts
	const day = 24 * time.Hour
	insert(1 * day)
	insert(7 * day)

	assertActive := func(n uint64, threshold time.Time) {
		t.Helper()
		active, err := store.AccountFundingInfo(threshold)
		if err != nil {
			t.Fatal(err)
		}
		if totalActive(active) != n {
			t.Fatalf("expected %d active accounts, got %d", n, totalActive(active))
		}
	}

	// 0 days
	threshold := now
	assertActive(0, threshold.Add(-time.Second))
	assertActive(0, threshold.Add(time.Second))

	// 1 day
	threshold = now.Add(-1 * day)
	assertActive(0, threshold.Add(time.Second))
	assertActive(1, threshold.Add(-time.Second))

	// 7 days
	threshold = now.Add(-7 * day)
	assertActive(2, threshold.Add(-time.Second))
	assertActive(1, threshold.Add(time.Second))

	// create a second quota with a different fund target
	var premiumFundTarget = uint64(32 << 30) // 32 GiB
	if err := store.PutQuota("premium", accounts.PutQuotaRequest{
		Description:     "premium quota",
		MaxPinnedData:   1000,
		TotalUses:       10,
		FundTargetBytes: &premiumFundTarget,
	}); err != nil {
		t.Fatal(err)
	}

	// create a connect key for the premium quota
	premiumKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "premium-connect-key",
		Description: "premium connect key",
		Quota:       "premium",
	})
	if err != nil {
		t.Fatal(err)
	}

	var premiumKeyID int64
	if err := store.pool.QueryRow(t.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, premiumKey.Key).Scan(&premiumKeyID); err != nil {
		t.Fatal(err)
	}

	// insert accounts under the premium quota
	insertPremium := func(d time.Duration) {
		lastUsed := now.Add(-d)
		if _, err := store.pool.Exec(
			t.Context(),
			`INSERT INTO accounts (public_key, connect_key_id, last_used, max_pinned_data) VALUES ($1, $2, $3, 1000000);`,
			sqlPublicKey(types.GeneratePrivateKey().PublicKey()),
			premiumKeyID,
			lastUsed,
		); err != nil {
			t.Fatal(err)
		}
	}

	insertPremium(1 * day)
	insertPremium(3 * day)
	insertPremium(10 * day) // older than 7 days, won't be active at 7-day threshold

	// assert funding info includes both quotas
	threshold = now.Add(-7 * day)
	infos, err := store.AccountFundingInfo(threshold.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Fatalf("expected 2 quota infos, got %d", len(infos))
	}

	// find each quota's info
	var defaultInfo, premiumInfo accounts.QuotaFundInfo
	for _, info := range infos {
		switch info.QuotaName {
		case "default":
			defaultInfo = info
		case "premium":
			premiumInfo = info
		default:
			t.Fatalf("unexpected quota name: %s", info.QuotaName)
		}
	}

	const defaultFundTarget = uint64(16e9) // 16 GB, matches migration default
	if defaultInfo.ActiveAccounts != 2 {
		t.Fatalf("expected 2 default active accounts, got %d", defaultInfo.ActiveAccounts)
	} else if defaultInfo.FundTargetBytes != defaultFundTarget {
		t.Fatalf("expected default fund target %d, got %d", defaultFundTarget, defaultInfo.FundTargetBytes)
	}

	if premiumInfo.ActiveAccounts != 2 {
		t.Fatalf("expected 2 premium active accounts, got %d", premiumInfo.ActiveAccounts)
	} else if premiumInfo.FundTargetBytes != premiumFundTarget {
		t.Fatalf("expected premium fund target %d, got %d", premiumFundTarget, premiumInfo.FundTargetBytes)
	}

	// at a tighter threshold, only 1 premium account should be active
	infos, err = store.AccountFundingInfo(now.Add(-2 * day))
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, info := range infos {
		if info.QuotaName == "premium" {
			found = true
			if info.ActiveAccounts != 1 {
				t.Fatalf("expected 1 premium active account, got %d", info.ActiveAccounts)
			}
		}
	}
	if !found {
		t.Fatal("expected premium quota in funding info")
	}
}

// BenchmarkAccountFundingInfo benchmarks the AccountFundingInfo function on the store.
func BenchmarkAccountFundingInfo(b *testing.B) {
	// define parameters
	const numAccounts = 100000

	// prepare database
	store := initPostgres(b, zap.NewNop())

	// create a connect key for benchmark accounts
	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "benchmark-connect-key",
		Description: "benchmark connect key",
		Quota:       "default",
	})
	if err != nil {
		b.Fatal(err)
	}

	var connectKeyID int64
	if err := store.pool.QueryRow(b.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
		b.Fatal(err)
	}

	batch := &pgx.Batch{}
	for range numAccounts {
		lastUsed := time.Now().Add(-24 * 7 * time.Hour * time.Duration(frand.Intn(30)))
		batch.Queue(`INSERT INTO accounts (public_key, connect_key_id, last_used, max_pinned_data) VALUES ($1, $2, $3, 1000000);`, sqlPublicKey(types.GeneratePrivateKey().PublicKey()), connectKeyID, lastUsed)
	}
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	threshold := time.Now().Add(-24 * 7 * time.Hour)
	for b.Loop() {
		if _, err := store.AccountFundingInfo(threshold); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPruneAccounts(b *testing.B) {
	const (
		numAccounts       = 1000
		objectsPerAccount = 500
		slabsPerObject    = 5
	)

	store := initPostgres(b, zap.NewNop())

	// create a connect key for benchmark accounts
	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "benchmark-connect-key",
		Description: "benchmark connect key",
		Quota:       "default",
	})
	if err != nil {
		b.Fatal(err)
	}

	var connectKeyID int64
	if err := store.pool.QueryRow(b.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
		b.Fatal(err)
	}

	batch := &pgx.Batch{}
	accountID, objectID, slabID := 0, 0, 0
	pinnedDataPerAccount := int64(objectsPerAccount*slabsPerObject) * int64(proto.SectorSize)
	for range numAccounts {
		ak := types.GeneratePrivateKey().PublicKey()

		batch.Queue(`INSERT INTO accounts(public_key, connect_key_id, max_pinned_data, pinned_data) VALUES ($1, $2, 1000000, $3);`, sqlPublicKey(ak), connectKeyID, pinnedDataPerAccount)
		accountID++

		for range objectsPerAccount {
			var encryptionKey [32]byte
			frand.Read(encryptionKey[:])

			batch.Queue(`INSERT INTO objects(object_key, account_id, encrypted_data_key, encrypted_meta_key, data_signature, meta_signature) VALUES ($1, $2, $3, $4, $5, $6)`, sqlHash256(frand.Entropy256()), accountID, frand.Bytes(72), frand.Bytes(72), frand.Bytes(64), frand.Bytes(64))
			objectID++
			for k := range slabsPerObject {
				slabDigest := sqlHash256(frand.Entropy256())

				batch.Queue(`INSERT INTO slabs(digest, encryption_key, min_shards) VALUES ($1, $2, 1);`, slabDigest, sqlHash256(encryptionKey))
				slabID++

				batch.Queue(`INSERT INTO account_slabs(account_id, slab_id) VALUES ($1, $2)`, accountID, slabID)
				batch.Queue(`INSERT INTO object_slabs(object_id, slab_digest, slab_index, slab_offset, slab_length) VALUES ($1, $2, $3, 0, 0)`, objectID, slabDigest, k)
			}
		}

		// delete 1/10 accounts
		if accountID%10 == 0 {
			batch.Queue("UPDATE accounts SET deleted_at = NOW() WHERE public_key = $1", sqlPublicKey(ak))
		}
	}
	batch.Queue(`UPDATE app_connect_keys ack SET pinned_data = (
		SELECT COALESCE(SUM(a.pinned_data), 0)
		FROM accounts a
		WHERE a.connect_key_id = ack.id
	)`)
	batch.Queue(`UPDATE stats SET num_slabs = $1`, numAccounts*objectsPerAccount*slabsPerObject)
	batch.Queue(`UPDATE stats SET num_accounts_registered = $1`, numAccounts)
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	if _, err := store.pool.Exec(b.Context(), `VACUUM FULL ANALYZE;`); err != nil {
		b.Fatal(err)
	}

	for _, limit := range []int{100, 250, 500} {
		b.Run(fmt.Sprint(limit), func(b *testing.B) {
			for b.Loop() {
				if err := store.PruneAccounts(limit); errors.Is(err, accounts.ErrNotFound) {
					b.Logf("pruning error: %v", err)
				} else if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAccountReady(b *testing.B) {
	const (
		numAccounts     = 10_000
		hostsPerAccount = 100
	)

	store := initPostgres(b, zap.NewNop())

	// create a connect key
	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "benchmark-connect-key",
		Description: "benchmark connect key",
		Quota:       "default",
	})
	if err != nil {
		b.Fatal(err)
	}

	var connectKeyID int64
	if err := store.pool.QueryRow(b.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
		b.Fatal(err)
	}

	// insert accounts
	accountKeys := make([]types.PublicKey, numAccounts)
	batch := &pgx.Batch{}
	for i := range numAccounts {
		accountKeys[i] = types.GeneratePrivateKey().PublicKey()
		batch.Queue(`INSERT INTO accounts (public_key, connect_key_id, max_pinned_data) VALUES ($1, $2, 1000000);`, sqlPublicKey(accountKeys[i]), connectKeyID)
	}
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	// insert hosts
	batch = &pgx.Batch{}
	for range hostsPerAccount {
		hk := types.GeneratePrivateKey().PublicKey()
		batch.Queue(`INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW());`, sqlPublicKey(hk))
	}
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	// insert account_hosts rows
	batch = &pgx.Batch{}
	for i := range numAccounts {
		accountID := i + 1
		for j := range hostsPerAccount {
			hostID := j + 1
			batch.Queue(`INSERT INTO account_hosts (account_id, host_id, consecutive_failed_funds) VALUES ($1, $2, $3);`, accountID, hostID, 0)
		}
	}
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	if _, err := store.pool.Exec(b.Context(), `VACUUM ANALYZE;`); err != nil {
		b.Fatal(err)
	}

	b.Run("Account", func(b *testing.B) {
		for b.Loop() {
			ak := accountKeys[frand.Intn(numAccounts)]
			if _, err := store.Account(ak); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Accounts", func(b *testing.B) {
		for b.Loop() {
			if _, err := store.Accounts(frand.Intn(numAccounts), 100); err != nil {
				b.Fatal(err)
			}
		}
	})
}
