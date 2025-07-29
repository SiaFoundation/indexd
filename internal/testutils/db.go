package testutils

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.uber.org/zap"
)

// NewDB creates a new postgres database for testing.
func NewDB(t testing.TB, maintenanceSettings contracts.MaintenanceSettings, log *zap.Logger) *postgres.Store {
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
	pool, err := pgxpool.New(context.Background(), ci.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %q", dbName)); err != nil {
		t.Fatal(err)
	} else if _, err := pool.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	ci.Database = dbName

	// create store
	store, err := postgres.NewStore(context.Background(), ci, maintenanceSettings, hosts.DefaultUsabilitySettings, log)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	return store
}
