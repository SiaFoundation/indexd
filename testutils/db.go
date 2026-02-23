package testutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
)

const (
	// TestQuotaName is the name of the quota used in tests.
	TestQuotaName = "testing"
)

// TestQuotaFundTargetBytes is the fund target for the testing quota.
var TestQuotaFundTargetBytes uint64 = 1 << 30 // 1 GiB

// TestStore represents a postgres database for testing with some helpers for
// common actions in testing like adding accounts and running SQL queries.
type TestStore struct {
	*postgres.Store

	pool *pgxpool.Pool
}

// NewDB creates a new postgres database for testing.
func NewDB(t testing.TB, maintenanceSettings contracts.MaintenanceSettings, log *zap.Logger) TestStore {
	// parse connection info from env vars
	ci := postgres.ConnectionInfo{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		Database: os.Getenv("POSTGRES_DB"),
		SSLMode:  "disable",
	}

	// create test-specific database
	dbName := t.Name()
	pool, err := pgxpool.New(t.Context(), ci.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(t.Context(), fmt.Sprintf("DROP DATABASE IF EXISTS %q", dbName)); err != nil {
		t.Fatal(err)
	} else if _, err := pool.Exec(t.Context(), fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	ci.Database = dbName

	// create store
	store, err := postgres.NewStore(t.Context(), ci, maintenanceSettings, hosts.DefaultUsabilitySettings, log)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	pool, err = pgxpool.New(t.Context(), ci.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	return TestStore{
		Store: store,
		pool:  pool,
	}
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (ts TestStore) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return ts.pool.Query(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (ts TestStore) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return ts.pool.QueryRow(ctx, query, args...)
}

// Exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (ts TestStore) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return ts.pool.Exec(ctx, query, args...)
}

// AddTestHost adds a host to the database for testing.
func (ts TestStore) AddTestHost(t testing.TB, host hosts.Host) {
	t.Helper()

	if err := ts.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(host.PublicKey, host.Addresses, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	if err := ts.UpdateHostScan(host.PublicKey, host.Settings, geoip.Location{}, true, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// AddTestAccount adds an account to the database for testing.
func (ts TestStore) AddTestAccount(t testing.TB, ak types.PublicKey) {
	t.Helper()

	const connectKey = "test"
	if _, err := ts.ValidAppConnectKey(connectKey); errors.Is(err, accounts.ErrKeyNotFound) {
		// create testing quota if it doesn't exist
		if err := ts.PutQuota(TestQuotaName, accounts.PutQuotaRequest{
			Description:     "Testing quota",
			MaxPinnedData:   uint64(1e12),
			TotalUses:       10000,
			FundTargetBytes: &TestQuotaFundTargetBytes,
		}); err != nil {
			t.Fatal(err)
		}
		_, err := ts.AddAppConnectKey(accounts.UpdateAppConnectKey{
			Key:   connectKey,
			Quota: TestQuotaName,
		})
		if err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}

	if err := ts.RegisterAppKey(connectKey, ak, accounts.AppMeta{}); err != nil {
		t.Fatal(err)
	}
}
