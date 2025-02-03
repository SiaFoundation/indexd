package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "embed"

	"go.uber.org/zap"
)

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func initSettings(ctx context.Context, tx *txn) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO global_settings(id, db_version) VALUES (0, 1);`)
	return err
}

// getDBVersion returns the current version of the database.
func getDBVersion(db *sql.DB) (version int64) {
	// error is ignored -- the database may not have been initialized yet.
	db.QueryRow(`SELECT db_version FROM global_settings;`).Scan(&version)
	return
}

// setDBVersion sets the current version of the database.
func setDBVersion(tx *txn, version int64) error {
	const query = `UPDATE global_settings SET db_version=$1 RETURNING id;`
	var dbID int64
	return tx.QueryRow(query, version).Scan(&dbID)
}

func (s *Store) initNewDatabase(ctx context.Context, target int64) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(initDatabase); err != nil {
			return err
		} else if err := initSettings(ctx, tx); err != nil {
			return fmt.Errorf("failed to init settings: %w", err)
		} else if err := setDBVersion(tx, target); err != nil {
			return fmt.Errorf("failed to set initial database version: %w", err)
		}
		return nil
	})
}

func (s *Store) upgradeDatabase(ctx context.Context, current, target int64) error {
	log := s.log.Named("migrations").With(zap.Int64("target", target))
	for ; current < target; current++ {
		version := current + 1 // initial schema is version 1, migration 0 is version 2, etc.
		log := log.With(zap.Int64("version", version))
		start := time.Now()
		fn := migrations[current-1]
		err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
			if err := fn(tx, log); err != nil {
				return err
			}
			return setDBVersion(tx, version)
		})
		if err != nil {
			return fmt.Errorf("migration %d failed: %w", version, err)
		}
		log.Info("migration complete", zap.Duration("elapsed", time.Since(start)))
	}
	return nil
}

func (s *Store) init(ctx context.Context) error {
	target := int64(len(migrations) + 1) // init.sql is the initial schema
	version := getDBVersion(s.db)
	switch {
	case version == 0:
		if err := s.initNewDatabase(ctx, target); err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
	case version < target:
		s.log.Info("database version is out of date;", zap.Int64("version", version), zap.Int64("target", target))
		if err := s.upgradeDatabase(ctx, version, target); err != nil {
			return fmt.Errorf("failed to upgrade database: %w", err)
		}
	case version > target:
		return fmt.Errorf("database version %v is newer than expected %v. database downgrades are not supported", version, target)
	}
	// nothing to do
	return nil
}
