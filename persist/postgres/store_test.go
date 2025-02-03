package postgres

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func initPostgres(t *testing.T, log *zap.Logger) *Store {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ci := ConnectionInfo{
		Host:     "localhost",
		Port:     5432,
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		Database: os.Getenv("POSTGRES_DB"),
		SSLMode:  "disable",
	}
	db, err := Connect(ctx, ci, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.db.Exec(`DROP SCHEMA public CASCADE;CREATE SCHEMA public;`); err != nil {
			panic(err)
		} else if err := db.Close(); err != nil {
			panic(err)
		}
	})
	return db
}

func TestPostgresCurrency(t *testing.T) {
	amount := types.Siacoins(uint32(frand.Uint64n(math.MaxUint32)))
	log := zaptest.NewLogger(t)

	store := initPostgres(t, log.Named("postgres"))

	err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(`INSERT INTO hello_world VALUES ($1)`, sqlCurrency(amount))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var value types.Currency
	err = store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(`SELECT * FROM hello_world`).Scan((*sqlCurrency)(&value))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
