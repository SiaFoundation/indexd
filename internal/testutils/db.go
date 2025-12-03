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
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
)

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
		_, err := ts.AddAppConnectKey(accounts.UpdateAppConnectKey{
			Key:           connectKey,
			MaxPinnedData: 1e10,
			RemainingUses: 10000,
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

// AddTestServiceAccount adds a service account to the database for testing.
func (s TestStore) AddTestServiceAccount(t testing.TB, hk types.PublicKey, ak proto.Account) {
	t.Helper()

	if err := s.Store.AddServiceAccount(types.PublicKey(ak), accounts.AppMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.UpdateServiceAccountBalance(hk, ak, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}
}
