package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
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
