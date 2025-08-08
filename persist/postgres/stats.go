package postgres

import (
	"context"

	"go.sia.tech/indexd/api/admin"
)

// SectorStats reports statistics about the sectors stored in the database. The
// response is cached and requires a call to RefreshSectorStats to be updated.
func (s *Store) SectorStats(ctx context.Context) (admin.SectorsStatsResponse, error) {
	var stats admin.SectorsStatsResponse
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		row := tx.QueryRow(ctx, "SELECT updated_at, num_sectors, num_unpinned_sectors, num_sectors_bad_contracts, num_sectors_no_host FROM sectors_stats")
		return row.Scan(&stats.UpdatedAt, &stats.NumSectors, &stats.NumUnpinnedSectors, &stats.NumSectorsBadContracts, &stats.NumSectorsNoHosts)
	})
	return stats, err
}

// RefreshSectorStats refreshes the cached sector statistics in the database.
// This is an expensive operation and should be called sparingly from a
// background thread.
func (s *Store) RefreshSectorStats(ctx context.Context) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY sectors_stats")
		return err
	})
}
