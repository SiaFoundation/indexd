package postgres

import (
	"context"
	"fmt"
	"time"

	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func initSettings(ctx context.Context, tx *txn, ms contracts.MaintenanceSettings, us hosts.UsabilitySettings) error {
	if _, err := tx.Exec(ctx, `INSERT INTO global_settings(id, db_version) VALUES (0, 1);`); err != nil {
		return fmt.Errorf("failed to insert initial global settings: %w", err)
	} else if err := setMaintenanceSettings(ctx, tx, ms); err != nil {
		return fmt.Errorf("failed to set initial maintenance settings %v: %w", ms, err)
	} else if err := setUsabilitySettings(ctx, tx, us); err != nil {
		return fmt.Errorf("failed to set initial usability settings %v: %w", us, err)
	}
	return nil
}

// getDBVersion returns the current version of the database.
func getDBVersion(ctx context.Context, pool *pgxpool.Pool) (version int64) {
	// error is ignored -- the database may not have been initialized yet.
	pool.QueryRow(ctx, `SELECT db_version FROM global_settings;`).Scan(&version)
	return
}

// setDBVersion sets the current version of the database.
func setDBVersion(ctx context.Context, tx *txn, version int64) error {
	const query = `UPDATE global_settings SET db_version=$1 RETURNING id;`
	var dbID int64
	return tx.QueryRow(ctx, query, version).Scan(&dbID)
}

func (s *Store) initNewDatabase(ctx context.Context, target int64, ms contracts.MaintenanceSettings, us hosts.UsabilitySettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(ctx, initDatabase); err != nil {
			return err
		} else if err := initSettings(ctx, tx, ms, us); err != nil {
			return fmt.Errorf("failed to init settings: %w", err)
		} else if err := s.initSectorStats(ctx, tx); err != nil {
			return fmt.Errorf("failed to init sector stats: %w", err)
		} else if err := setDBVersion(ctx, tx, target); err != nil {
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
			if err := fn(ctx, tx, log); err != nil {
				return err
			}
			return setDBVersion(ctx, tx, version)
		})
		if err != nil {
			return fmt.Errorf("migration %d failed: %w", version, err)
		}
		log.Info("migration complete", zap.Duration("elapsed", time.Since(start)))
	}
	return nil
}
