package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"

	_ "github.com/lib/pq" // postgres driver
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type (
	// ConnectionInfo contains the information needed to connect to a PostgreSQL
	// database.
	ConnectionInfo struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
		SSLMode  string `json:"sslmode"`
	}

	// A Store is a persistent store that uses a SQL database as its backend.
	Store struct {
		db  *sql.DB
		log *zap.Logger
	}
)

// String returns a connection string for the given ConnectionInfo.
func (ci ConnectionInfo) String() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", ci.Host, ci.Port, ci.User, ci.Password, ci.Database, ci.SSLMode)
}

func (s *Store) transaction(ctx context.Context, fn func(context.Context, *txn) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	log := s.log.Named("transaction").With(zap.String("id", hex.EncodeToString(frand.Bytes(4))))
	if err := fn(ctx, &txn{tx, log}); err != nil {
		return err
	} else if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Connect connects to a running PostgresSQL server. The passed in context
// determines the lifecycle of necessary migrations. If the context is cancelled,
// the running migration will be interupted and an error returned.
func Connect(ctx context.Context, ci ConnectionInfo, log *zap.Logger) (*Store, error) {
	db, err := sql.Open("postgres", ci.String())
	if err != nil {
		return nil, err
	}
	store := &Store{
		db:  db,
		log: log,
	}
	if err := store.init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize store: %w", err)
	}
	return store, nil
}
