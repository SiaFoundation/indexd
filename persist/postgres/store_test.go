package postgres

import (
	"context"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestEnsureDatabase(t *testing.T) {
	// generate random database name
	rBytes := frand.Entropy128()
	ci := connectionInfoFromEnv()
	ci.Database = hex.EncodeToString(rBytes[:])

	// ensure the database exists
	if err := ensureDatabase(t.Context(), ci); err != nil {
		t.Fatalf("failed to ensure database: %v", err)
	}

	// cleanup - drop the database
	store := initPostgres(t, zap.NewNop())
	if _, err := store.pool.Exec(t.Context(), `DROP DATABASE `+pgx.Identifier{ci.Database}.Sanitize()); err != nil {
		t.Fatalf("failed to drop database: %v", err)
	}
}

func connectionInfoFromEnv() ConnectionInfo {
	return ConnectionInfo{
		Host:     "localhost",
		Port:     5432,
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		Database: os.Getenv("POSTGRES_DB"),
		SSLMode:  "disable",
	}
}

// publishEvents publishes the events written so far, waiting out the current
// second first if a publish already claimed it and events are pending, since
// at most one batch is published per second.
func (s *Store) publishEvents(t testing.TB) {
	t.Helper()
	var wait bool
	if err := s.pool.QueryRow(t.Context(), `SELECT object_events_last_published >= date_trunc('second', NOW())
 AND (EXISTS (SELECT 1 FROM object_events WHERE updated_at IS NULL) OR EXISTS (SELECT 1 FROM space_object_events WHERE updated_at IS NULL))
 FROM global_settings`).Scan(&wait); err != nil {
		t.Fatal(err)
	}
	if wait {
		now := time.Now()
		time.Sleep(now.Truncate(time.Second).Add(time.Second).Sub(now) + 20*time.Millisecond)
	}
	if err := s.PublishObjectEvents(); err != nil {
		t.Fatal(err)
	}
}

// waitBlockedBy polls until whether a backend waits on a lock held by lockTx
// matches blocked, reporting whether it did within ten seconds.
func (s *Store) waitBlockedBy(t testing.TB, lockTx pgx.Tx, blocked bool) bool {
	t.Helper()
	for range 100 {
		var waiting bool
		if err := s.pool.QueryRow(t.Context(), `SELECT EXISTS (
 SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND $1 = ANY(pg_blocking_pids(pid))
)`, int32(lockTx.Conn().PgConn().PID())).Scan(&waiting); err != nil {
			t.Fatal(err)
		} else if waiting == blocked {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func initPostgres(t testing.TB, log *zap.Logger) *Store {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := NewStore(ctx, connectionInfoFromEnv(), contracts.DefaultMaintenanceSettings, hosts.DefaultUsabilitySettings, log.Named("postgres"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.pool.Exec(context.Background(), `DROP SCHEMA public CASCADE;CREATE SCHEMA public;`); err != nil {
			panic(err)
		} else if err := db.Close(); err != nil {
			panic(err)
		}
	})
	return db
}
