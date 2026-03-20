package postgres

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/hosts"
)

const (
	statSlabs              = "num_slabs"
	statMigratedSectors    = "num_migrated_sectors"
	statPinnedSectors      = "num_pinned_sectors"
	statUnpinnableSectors  = "num_unpinnable_sectors"
	statUnpinnedSectors    = "num_unpinned_sectors"
	statSectorsChecked     = "num_sectors_checked"
	statSectorsLost        = "num_sectors_lost"
	statSectorsCheckFailed = "num_sectors_check_failed"
	statAccountsRegistered = "num_accounts_registered"
	statScans              = "num_scans"
	statScansFailed        = "num_scans_failed"
)

func incrementStat(ctx context.Context, tx *txn, name string, delta int64) error {
	result, err := tx.Exec(ctx, "UPDATE stats SET stat_value = stat_value + $1 WHERE stat_name = $2", delta, name)
	if err != nil {
		return err
	} else if result.RowsAffected() == 0 {
		return fmt.Errorf("stat %q does not exist", name)
	}
	return err
}

func incrementNumAccounts(ctx context.Context, tx *txn, delta int64) error {
	return incrementStat(ctx, tx, statAccountsRegistered, delta)
}

func incrementNumSlabs(ctx context.Context, tx *txn, delta int64) error {
	return incrementStat(ctx, tx, statSlabs, delta)
}

func incrementNumMigratedSectors(ctx context.Context, tx *txn) error {
	return incrementStat(ctx, tx, statMigratedSectors, 1)
}

func incrementNumPinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	return incrementStat(ctx, tx, statPinnedSectors, delta)
}

func incrementNumUnpinnableSectors(ctx context.Context, tx *txn, delta int64) error {
	return incrementStat(ctx, tx, statUnpinnableSectors, delta)
}

func incrementNumUnpinnedSectors(ctx context.Context, tx *txn, delta int64) error {
	return incrementStat(ctx, tx, statUnpinnedSectors, delta)
}

func incrementNumSectorsLost(ctx context.Context, tx *txn, delta uint64) error {
	return incrementStat(ctx, tx, statSectorsLost, int64(delta))
}

func incrementNumSectorsChecked(ctx context.Context, tx *txn, delta uint64) error {
	return incrementStat(ctx, tx, statSectorsChecked, int64(delta))
}

func incrementNumSectorsFailed(ctx context.Context, tx *txn, delta uint64) error {
	return incrementStat(ctx, tx, statSectorsCheckFailed, int64(delta))
}

func incrementNumScans(ctx context.Context, tx *txn, success bool) error {
	if err := incrementStat(ctx, tx, statScans, 1); err != nil {
		return err
	}
	if !success {
		return incrementStat(ctx, tx, statScansFailed, 1)
	}
	return nil
}

func incrementHostUnpinnedSectors(ctx context.Context, tx *txn, hostID int64, delta int64) error {
	_, err := tx.Exec(ctx, `UPDATE hosts SET unpinned_sectors = unpinned_sectors + $1 WHERE id = $2`, delta, hostID)
	return err
}

type unpinnedDelta struct {
	hostID int64
	delta  int64
}

func incrementHostsUnpinnedSectors(ctx context.Context, tx *txn, deltas []unpinnedDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	// sort by hostID to ensure consistent lock ordering and prevent deadlocks
	slices.SortFunc(deltas, func(a, b unpinnedDelta) int {
		return int(a.hostID - b.hostID)
	})
	batch := &pgx.Batch{}
	for _, delta := range deltas {
		batch.Queue(`UPDATE hosts SET unpinned_sectors = unpinned_sectors + $1 WHERE id = $2`, delta.delta, delta.hostID)
	}
	return tx.SendBatch(ctx, batch).Close()
}

func initStats(ctx context.Context, tx *txn) error {
	_, err := tx.Exec(ctx, `INSERT INTO stats (stat_name) VALUES
		($1), ($2), ($3), ($4), ($5), ($6), ($7), ($8), ($9), ($10), ($11)
	ON CONFLICT (stat_name) DO NOTHING`,
		statSlabs, statMigratedSectors, statPinnedSectors, statUnpinnableSectors, statUnpinnedSectors,
		statSectorsChecked, statSectorsLost, statSectorsCheckFailed,
		statAccountsRegistered,
		statScans, statScansFailed,
	)
	return err
}

// SectorStats reports statistics about the sectors and slabs stored in the
// database.
func (s *Store) SectorStats() (admin.SectorsStatsResponse, error) {
	var stats admin.SectorsStatsResponse
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT stat_value FROM stats WHERE stat_name = $1),
			(SELECT stat_value FROM stats WHERE stat_name = $2),
			(SELECT stat_value FROM stats WHERE stat_name = $3),
			(SELECT stat_value FROM stats WHERE stat_name = $4),
			(SELECT stat_value FROM stats WHERE stat_name = $5),
			(SELECT stat_value FROM stats WHERE stat_name = $6),
			(SELECT stat_value FROM stats WHERE stat_name = $7),
			(SELECT stat_value FROM stats WHERE stat_name = $8)`,
			statSlabs, statMigratedSectors, statPinnedSectors, statUnpinnableSectors,
			statUnpinnedSectors, statSectorsLost, statSectorsChecked, statSectorsCheckFailed).
			Scan(&stats.Slabs, &stats.Migrated, &stats.Pinned, &stats.Unpinnable, &stats.Unpinned, &stats.Lost, &stats.Checked, &stats.CheckFailed)
	})
	return stats, err
}

// AppStats reports per-app statistics including total accounts, active
// accounts, and total pinned data for all apps.
func (s *Store) AppStats(offset, limit int) ([]admin.AppStats, error) {
	var stats []admin.AppStats
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		stats = stats[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(ctx, `
SELECT
	app_id,
	ANY_VALUE(name),
	COUNT(*),
	COUNT(*) FILTER (WHERE last_used >= $1),
	COALESCE(SUM(pinned_data), 0),
	COALESCE(SUM(pinned_size), 0)
FROM accounts
WHERE deleted_at IS NULL
GROUP BY app_id
ORDER BY COUNT(*) DESC
OFFSET $2 LIMIT $3`,
			time.Now().Add(-accounts.AccountActivityThreshold), offset, limit,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var as admin.AppStats
			if err := rows.Scan((*sqlHash256)(&as.AppID), &as.Name, &as.Accounts, &as.Active, &as.PinnedData, &as.PinnedSize); err != nil {
				return err
			}
			stats = append(stats, as)
		}
		return rows.Err()
	})
	return stats, err
}

// AccountStats reports statistics about the accounts stored in the database.
func (s *Store) AccountStats() (admin.AccountStatsResponse, error) {
	var stats admin.AccountStatsResponse
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, "SELECT stat_value FROM stats WHERE stat_name = $1", statAccountsRegistered).Scan(&stats.Registered)
		if err != nil {
			return fmt.Errorf("failed to get number of registered accounts: %w", err)
		}

		err = tx.QueryRow(ctx,
			`SELECT COUNT(*) FILTER (WHERE last_used >= $1), COALESCE(SUM(pinned_data), 0), COALESCE(SUM(pinned_size), 0) FROM accounts WHERE deleted_at IS NULL;`,
			time.Now().Add(-accounts.AccountActivityThreshold),
		).Scan(&stats.Active, &stats.PinnedData, &stats.PinnedSize)
		if err != nil {
			return fmt.Errorf("failed to get account stats: %w", err)
		}
		return nil
	})
	return stats, err
}

// ConnectKeyStats reports statistics about connect keys, including the total
// number of keys and the breakdown by quota.
func (s *Store) ConnectKeyStats() (stats admin.ConnectKeyStatsResponse, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT quota_name, COUNT(*)
			FROM app_connect_keys
			GROUP BY quota_name
			ORDER BY quota_name`)
		if err != nil {
			return fmt.Errorf("failed to get connect key stats: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var qs admin.ConnectKeyQuotaStats
			if err := rows.Scan(&qs.Quota, &qs.Total); err != nil {
				return err
			}
			stats.Total += qs.Total
			stats.Quotas = append(stats.Quotas, qs)
		}
		return rows.Err()
	})
	return
}

// AggregatedHostStats reports aggregated statistics about all hosts, including the
// number of active hosts and scan counts.
func (s *Store) AggregatedHostStats() (stats admin.AggregatedHostStatsResponse, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		if err := tx.QueryRow(ctx, `SELECT
			(SELECT stat_value FROM stats WHERE stat_name = $1),
			(SELECT stat_value FROM stats WHERE stat_name = $2)`,
			statScans, statScansFailed).
			Scan(&stats.TotalScans, &stats.FailedScans); err != nil {
			return fmt.Errorf("failed to get scan stats: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			WITH globals AS (
				SELECT scanned_height FROM global_settings
			)
			SELECT
				COUNT(DISTINCT c.host_id),
				COUNT(DISTINCT c.host_id) FILTER (WHERE h.settings_remaining_storage > 0)
			FROM contracts c
			INNER JOIN hosts h ON c.host_id = h.id
			CROSS JOIN globals
			WHERE
				c.good = TRUE AND
				c.state IN (0,1) AND
				c.renewed_to IS NULL AND
				c.proof_height > globals.scanned_height AND
				h.stuck_since IS NULL
		`).Scan(&stats.Active, &stats.GoodForUpload); err != nil {
			return fmt.Errorf("failed to get active hosts: %w", err)
		}
		return nil
	})
	return
}

// HostStats reports statistics about used hosts. We consider a host to be used
// as soon as we spent any funds on it.
func (s *Store) HostStats(offset, limit int) ([]hosts.HostStats, error) {
	var stats []hosts.HostStats
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		stats = stats[:0] // reuse same slice if transaction retries

		rows, err := tx.Query(ctx, `
			WITH `+sqlGlobalsCTE+`,
			selected_hosts AS (
				SELECT
					host.*,
					COALESCE(NOT host.blocked AND (`+sqlUsabilityFilter+`
					) AND EXISTS (
						SELECT 1
						FROM contracts
						WHERE host_id = host.id
							AND state IN (0,1)
							AND renewed_to IS NULL
							AND good
							AND proof_height > globals.scanned_height
					), FALSE) AS usable
				FROM (
					SELECT
						h.id,
						h.public_key,
						h.lost_sectors,
						h.unpinned_sectors,
						h.usage_account_funding,
						h.usage_total_spent,
						h.settings_protocol_version,
						h.settings_release,
						h.scans,
						h.scans_failed,
						hb.public_key IS NOT NULL AS blocked,
						COALESCE(hb.reasons, ARRAY[]::TEXT[]) AS blocked_reasons,
						h.stuck_since,
						h.settings_remaining_storage,
						h.last_successful_scan IS NOT NULL AS has_settings,
						(get_byte(h.settings_protocol_version, 0) << 16) + (get_byte(h.settings_protocol_version, 1) << 8) + (get_byte(h.settings_protocol_version, 2)) AS settings_version,
						h.recent_uptime,
						h.settings_max_contract_duration,
						h.settings_max_collateral,
						h.settings_collateral,
						h.settings_valid_until,
						h.last_successful_scan,
						h.settings_accepting_contracts,
						h.settings_contract_price,
						h.settings_storage_price,
						h.settings_ingress_price,
						h.settings_egress_price,
						h.settings_free_sector_price
					FROM hosts h
					LEFT JOIN hosts_blocklist hb ON hb.public_key = h.public_key
					WHERE h.usage_total_spent > 0
					ORDER BY h.usage_total_spent DESC
					OFFSET $1
					LIMIT $2
				) host
				CROSS JOIN globals
			)
			SELECT
				h.public_key,
				h.lost_sectors,
				h.unpinned_sectors,
				COALESCE(cs.total_contracts_size, 0) AS total_contracts_size,
				h.usage_account_funding,
				h.usage_total_spent,
				h.settings_protocol_version,
				h.settings_release,
				h.scans,
				h.scans_failed,
				h.blocked,
				h.blocked_reasons,
				h.stuck_since,
				h.usable,
				h.usable AND h.stuck_since IS NULL AND h.settings_remaining_storage > 0 AS good_for_upload
			FROM selected_hosts h
			LEFT JOIN LATERAL (
			SELECT SUM(size) AS total_contracts_size
			FROM contracts
			WHERE host_id = h.id
				AND state IN (0,1)
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
			var stuckSince pgtype.Timestamp
			if err := rows.Scan(
				(*sqlPublicKey)(&hs.PublicKey),
				&hs.LostSectors,
				&hs.UnpinnedSectors,
				&hs.ActiveContractsSize,
				(*sqlCurrency)(&hs.AccountUsage),
				(*sqlCurrency)(&hs.TotalUsage),
				(*sqlProtocolVersion)(&hs.ProtocolVersion),
				&hs.Release,
				&hs.Scans,
				&hs.ScansFailed,
				&hs.Blocked,
				&hs.BlockedReasons,
				&stuckSince,
				&hs.Usable,
				&hs.GoodForUpload,
			); err != nil {
				return err
			}
			if stuckSince.Valid {
				hs.StuckSince = &stuckSince.Time
			}
			stats = append(stats, hs)
		}
		return rows.Err()
	})
	return stats, err
}
