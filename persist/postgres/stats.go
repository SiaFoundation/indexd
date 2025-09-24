package postgres

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/indexd/api/admin"
)

func (s *Store) incrementNumSlabs(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_slabs = num_slabs + $1", delta)
	return err
}

func (s *Store) incrementNumMigratedSectors(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, `UPDATE stats SET num_migrated_sectors = num_migrated_sectors + 1`)
	return err
}

func (s *Store) incrementNumPinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, `UPDATE stats SET num_pinned_sectors = num_pinned_sectors + $1`, delta)
	return err
}

func (s *Store) incrementNumUnpinnableSlabs(ctx context.Context, tx *txn, incr uint64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_unpinnable_sectors = num_unpinnable_sectors + $1", incr)
	return err
}

func (s *Store) incrementUnpinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_unpinned_sectors = num_unpinned_sectors + $1", delta)
	return err
}

func (s *Store) initStats(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, "INSERT INTO stats (id) VALUES (0) ON CONFLICT(id) DO NOTHING")
	return err
}

// SectorStats reports statistics about the sectors and slabs stored in the
// database.
func (s *Store) SectorStats(ctx context.Context) (admin.SectorsStatsResponse, error) {
	var stats admin.SectorsStatsResponse
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		row := tx.QueryRow(ctx, "SELECT num_slabs, num_migrated_sectors, num_pinned_sectors, num_unpinnable_sectors, num_unpinned_sectors FROM stats")
		return row.Scan(&stats.NumSlabs, &stats.NumMigratedSectors, &stats.NumPinnedSectors, &stats.NumUnpinnableSectors, &stats.NumUnpinnedSectors)
	})
	return stats, err
}

func (s *Store) incrementNumAccounts(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_accounts_registered = num_accounts_registered + $1", delta)
	return err
}

// AccountStats reports statistics about the accounts stored in the database.
func (s *Store) AccountStats(ctx context.Context) (admin.AccountStatsResponse, error) {
	var stats admin.AccountStatsResponse
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, "SELECT num_accounts_registered FROM stats").Scan(&stats.Registered)
		if err != nil {
			return fmt.Errorf("failed to get number of registered accounts: %w", err)
		}

		const activeAccountThreshold = 7 * 24 * time.Hour
		stats.Active, err = activeAccounts(ctx, tx, time.Now().Add(-activeAccountThreshold))
		if err != nil {
			return fmt.Errorf("failed to get active accounts: %w", err)
		}
		return nil
	})
	return stats, err
}
