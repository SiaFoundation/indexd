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
	if err := ensureDatabase(context.Background(), ci); err != nil {
		t.Fatalf("failed to ensure database: %v", err)
	}

	// cleanup - drop the database
	store := initPostgres(t, zap.NewNop())
	if _, err := store.pool.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{ci.Database}.Sanitize()); err != nil {
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
