package postgres

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/hosts"
)

func incrementNumAccounts(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_accounts_registered = num_accounts_registered + $1", delta)
	return err
}

func incrementNumSlabs(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_slabs = num_slabs + $1", delta)
	return err
}

func incrementNumMigratedSectors(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, `UPDATE stats SET num_migrated_sectors = num_migrated_sectors + 1`)
	return err
}

func incrementNumPinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, `UPDATE stats SET num_pinned_sectors = num_pinned_sectors + $1`, delta)
	return err
}

func incrementNumUnpinnableSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_unpinnable_sectors = num_unpinnable_sectors + $1", delta)
	return err
}

func incrementNumUnpinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_unpinned_sectors = num_unpinned_sectors + $1", delta)
	return err
}

func incrementNumSectorsLost(ctx context.Context, tx *txn, delta uint64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_sectors_lost = num_sectors_lost + $1", delta)
	return err
}

func incrementNumSectorsChecked(ctx context.Context, tx *txn, delta uint64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_sectors_checked = num_sectors_checked + $1", delta)
	return err
}

func incrementNumSectorsFailed(ctx context.Context, tx *txn, delta uint64) error {
	_, err := tx.Exec(ctx, "UPDATE stats SET num_sectors_check_failed = num_sectors_check_failed + $1", delta)
	return err
}

func initStats(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, "INSERT INTO stats (id) VALUES (0) ON CONFLICT(id) DO NOTHING")
	return err
}

// SectorStats reports statistics about the sectors and slabs stored in the
// database.
func (s *Store) SectorStats() (admin.SectorsStatsResponse, error) {
	var stats admin.SectorsStatsResponse
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		row := tx.QueryRow(ctx, "SELECT num_slabs, num_migrated_sectors, num_pinned_sectors, num_unpinnable_sectors, num_unpinned_sectors, num_sectors_lost, num_sectors_checked, num_sectors_check_failed FROM stats")
		return row.Scan(&stats.Slabs, &stats.Migrated, &stats.Pinned, &stats.Unpinnable, &stats.Unpinned, &stats.Lost, &stats.Checked, &stats.CheckFailed)
	})
	return stats, err
}

// AccountStats reports statistics about the accounts stored in the database.
func (s *Store) AccountStats() (admin.AccountStatsResponse, error) {
	var stats admin.AccountStatsResponse
	err := s.transaction(func(ctx context.Context, tx *txn) error {
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

// HostStats reports statistics about used hosts. We consider a host to be used
// as soon as we spent any funds on it.
func (s *Store) HostStats(offset, limit int) ([]hosts.HostStats, error) {
	var stats []hosts.HostStats
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			WITH globals AS (
				SELECT scanned_height FROM global_settings
			),
			selected_hosts AS (
				SELECT
					h.id,
					h.public_key,
					h.lost_sectors,
					h.usage_account_funding,
					h.usage_total_spent,
					hb.public_key IS NOT NULL AS blocked,
					COALESCE(hb.reasons, ARRAY[]::TEXT[]) AS blocked_reasons
				FROM hosts h
				LEFT JOIN hosts_blocklist hb ON hb.public_key = h.public_key
				WHERE h.usage_total_spent > 0
				ORDER BY h.usage_total_spent DESC
				OFFSET $1
				LIMIT $2
			)
			SELECT
				h.public_key,
				h.lost_sectors,
				COALESCE(cs.total_contracts_size, 0) AS total_contracts_size,
				h.usage_account_funding,
				h.usage_total_spent,
				h.blocked,
				h.blocked_reasons
			FROM selected_hosts h
			LEFT JOIN LATERAL (
			SELECT SUM(size) AS total_contracts_size
			FROM contracts
			WHERE host_id = h.id
				AND (state = 0 OR state = 1)
				AND renewed_to IS NULL
				AND proof_height > (SELECT scanned_height FROM globals)
			) cs ON TRUE
			ORDER BY h.usage_total_spent DESC;
		`, offset, limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var hs hosts.HostStats
			if err := rows.Scan(
				(*sqlPublicKey)(&hs.PublicKey),
				&hs.LostSectors,
				&hs.ActiveContractsSize,
				(*sqlCurrency)(&hs.AccountUsage),
				(*sqlCurrency)(&hs.TotalUsage),
				&hs.Blocked,
				&hs.BlockedReasons,
			); err != nil {
				return err
			}
			stats = append(stats, hs)
		}
		return rows.Err()
	})
	return stats, err
}
