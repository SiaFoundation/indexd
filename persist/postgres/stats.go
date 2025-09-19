package postgres

import (
	"context"

	"go.sia.tech/indexd/api/admin"
)

func (s *Store) incrementNumSlabs(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE sectors_stats SET num_slabs = num_slabs + $1", delta)
	return err
}

func (s *Store) incrementNumMigratedSectors(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, `UPDATE sectors_stats SET num_migrated_sectors = num_migrated_sectors + 1`)
	return err
}

func (s *Store) incrementPinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, `UPDATE sectors_stats SET num_pinned_sectors = num_pinned_sectors + $1`, delta)
	return err
}

func (s *Store) incrementUnpinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE sectors_stats SET num_unpinned_sectors = num_unpinned_sectors + $1", delta)
	return err
}

func (s *Store) initSectorStats(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, "INSERT INTO sectors_stats (id) VALUES (0) ON CONFLICT(id) DO NOTHING")
	return err
}

// SectorStats reports statistics about the sectors and slabs stored in the
// database.
func (s *Store) SectorStats(ctx context.Context) (admin.SectorsStatsResponse, error) {
	var stats admin.SectorsStatsResponse
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		row := tx.QueryRow(ctx, "SELECT num_slabs, num_migrated_sectors, num_pinned_sectors, num_unpinned_sectors FROM sectors_stats")
		return row.Scan(&stats.NumSlabs, &stats.NumMigratedSectors, &stats.NumPinnedSectors, &stats.NumUnpinnedSectors)
	})
	return stats, err
}
