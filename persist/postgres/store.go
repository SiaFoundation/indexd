package postgres

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type (
	// ConnectionInfo contains the information needed to connect to a PostgreSQL
	// database.
	ConnectionInfo struct {
		Host     string `json:"host" yaml:"host"`
		Port     int    `json:"port" yaml:"port"`
		User     string `json:"user" yaml:"user"`
		Password string `json:"password" yaml:"password"`
		Database string `json:"database" yaml:"database"`
		SSLMode  string `json:"sslmode" yaml:"sslmode"`
	}

	// A Store is a persistent store that uses a SQL database as its backend.
	Store struct {
		pool *pgxpool.Pool
		log  *zap.Logger
	}
)

// String returns a connection string for the given ConnectionInfo.
func (ci ConnectionInfo) String() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", ci.Host, ci.Port, ci.User, ci.Password, ci.Database, ci.SSLMode)
}

func (s *Store) transaction(ctx context.Context, fn func(context.Context, *txn) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	log := s.log.Named("transaction").With(zap.String("id", hex.EncodeToString(frand.Bytes(4))))
	if err := fn(ctx, &txn{tx, log}); err != nil {
		return err
	} else if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// NewStore creates a new Store instance, initializing the database if
// necessary.  The passed in context determines the lifecycle of necessary
// migrations. If the context is cancelled, the running migration will be
// interupted and an error returned.
func NewStore(ctx context.Context, ci ConnectionInfo, defaultMaintenanceSettings contracts.MaintenanceSettings, defaultUsabilitySettings hosts.UsabilitySettings, log *zap.Logger) (*Store, error) {
	if err := ensureDatabase(ctx, ci); err != nil {
		return nil, fmt.Errorf("failed to ensure database %q exists: %w", ci.Database, err)
	}

	pool, err := pgxpool.New(ctx, ci.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	} else if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	log.Info("connected", zap.String("database", ci.Database), zap.String("host", ci.Host), zap.Int("port", ci.Port))

	s := &Store{
		pool: pool,
		log:  log,
	}

	target := int64(len(migrations) + 1) // init.sql is the initial schema
	version := getDBVersion(ctx, pool)
	switch {
	case version == 0:
		if err := s.initNewDatabase(ctx, target, defaultMaintenanceSettings, defaultUsabilitySettings); err != nil {
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	case version < target:
		s.log.Info("database version is out of date;", zap.Int64("version", version), zap.Int64("target", target))
		if err := s.upgradeDatabase(ctx, version, target); err != nil {
			return nil, fmt.Errorf("failed to upgrade database: %w", err)
		}
	case version > target:
		return nil, fmt.Errorf("database version %v is newer than expected %v. database downgrades are not supported", version, target)
	}

	return s, nil
}

func ensureDatabase(ctx context.Context, ci ConnectionInfo) error {
	// return early if we're connecting to the default database
	if ci.Database == "postgres" {
		return nil
	}
	db := ci.Database
	ci.Database = "postgres"

	// connect to the postgres database
	pool, err := pgxpool.New(ctx, ci.String())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres database: %w", err)
	}
	defer pool.Close()

	// check if the database exists
	var exists bool
	if err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)", db).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	} else if exists {
		return nil
	}

	// create the database if it does not exist
	query := "CREATE DATABASE " + pgx.Identifier{db}.Sanitize()
	_, err = pool.Exec(ctx, query)
	return err
}
