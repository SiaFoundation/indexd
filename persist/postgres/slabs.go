package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// MarkSlabUnrecoverable flags a slab that can never be fully repaired as
// unrecoverable, excluding it from [Store.UnhealthySlabs] until it is
// re-pinned, and records the reason we gave up on it. The original reason is
// kept if the slab was already marked by a previous call.
func (s *Store) MarkSlabUnrecoverable(slabID slabs.SlabID, reason string) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		// the epoch marks a slab that can never be fully repaired, keep the
		// original reason if the slab is already marked
		res, err := tx.Exec(ctx, `
			UPDATE slabs
			SET unrecoverable_since = 'epoch',
				unrecoverable_reason = CASE WHEN unrecoverable_since = 'epoch' THEN unrecoverable_reason ELSE $2 END
			WHERE digest = $1`, sqlHash256(slabID), reason)
		if err != nil {
			return fmt.Errorf("failed to mark slab unrecoverable: %w", err)
		} else if res.RowsAffected() == 0 {
			return slabs.ErrSlabNotFound
		}
		return nil
	})
}

// RecordFailedSlabRecovery records a failed recovery of the slab's shards and
// reports whether it marked the slab unrecoverable. Failures only count while
// fewer than MinShards of the slab's sectors are stored on a host, and mark
// the slab unrecoverable once they have for [slabs.RecoveryWindow].
func (s *Store) RecordFailedSlabRecovery(slabID slabs.SlabID, reason string) (unrecoverable bool, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		unrecoverable = false // reset on retry
		err := tx.QueryRow(ctx, `
			WITH slab AS (
				SELECT s.id, (
					SELECT COUNT(*)
					FROM slab_sectors ss
					INNER JOIN sectors sec ON sec.id = ss.sector_id
					WHERE ss.slab_id = s.id AND sec.host_id IS NOT NULL
				) < s.min_shards AS below_min_shards
				FROM slabs s
				WHERE s.digest = $1
			), updated AS (
				UPDATE slabs s
				SET unrecoverable_since = CASE WHEN slab.below_min_shards THEN COALESCE(s.unrecoverable_since, NOW()) END,
					unrecoverable_reason = CASE WHEN slab.below_min_shards AND s.unrecoverable_since <= $2 THEN $3 END
				FROM slab
				WHERE s.id = slab.id AND s.unrecoverable_reason IS NULL AND (slab.below_min_shards OR s.unrecoverable_since IS NOT NULL)
				RETURNING s.unrecoverable_reason IS NOT NULL AS unrecoverable
			)
			SELECT COALESCE((SELECT unrecoverable FROM updated), FALSE) FROM slab`,
			sqlHash256(slabID), time.Now().Add(-slabs.RecoveryWindow), reason).Scan(&unrecoverable)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to record failed slab recovery: %w", err)
		}
		return nil
	})
	return
}

// MarkSlabRepaired marks the slab as repaired or increments the failed repair
// count. If the repair was successful, the consecutive_failed_repairs counter
// is reset to zero. If the repair failed, the counter is incremented and the
// next repair attempt time is set using exponential backoff. Either way, the
// slab's recovery window is reset.
func (s *Store) MarkSlabRepaired(slabID slabs.SlabID, success bool) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		if success {
			if res, err := tx.Exec(ctx, `
				UPDATE slabs
				SET consecutive_failed_repairs = 0,
					unrecoverable_since = CASE WHEN unrecoverable_since = 'epoch' THEN unrecoverable_since END,
					unrecoverable_reason = CASE WHEN unrecoverable_since = 'epoch' THEN unrecoverable_reason END
				WHERE digest = $1`, sqlHash256(slabID)); err != nil {
				return fmt.Errorf("failed to mark slab as repaired: %w", err)
			} else if res.RowsAffected() == 0 {
				return slabs.ErrSlabNotFound
			}
			return nil
		}

		var currentFailures int
		err := tx.QueryRow(ctx, `
			SELECT consecutive_failed_repairs
			FROM slabs
			WHERE digest = $1
			FOR UPDATE
		`, sqlHash256(slabID)).Scan(&currentFailures)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to fetch repair state: %w", err)
		}

		nextRepairBackoff := min(minRepairBackoff*time.Duration(1<<(currentFailures)), maxRepairBackoff)
		_, err = tx.Exec(ctx, `
			UPDATE slabs
			SET consecutive_failed_repairs = $2, next_repair_attempt = $3,
				unrecoverable_since = CASE WHEN unrecoverable_since = 'epoch' THEN unrecoverable_since END,
				unrecoverable_reason = CASE WHEN unrecoverable_since = 'epoch' THEN unrecoverable_reason END
			WHERE digest = $1`, sqlHash256(slabID), currentFailures+1, time.Now().Add(nextRepairBackoff))
		if err != nil {
			return fmt.Errorf("failed to update repair state: %w", err)
		}

		return nil
	})
}

// Slab retrieves a slab from the database by its ID.
func (s *Store) Slab(slabID slabs.SlabID) (slab slabs.Slab, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		slab.Sectors = slab.Sectors[:0] // reuse same slice if transaction retries

		var dbID int64
		err = tx.QueryRow(ctx, `SELECT s.id, s.encryption_key, s.min_shards, s.version, s.pinned_at FROM slabs s WHERE digest = $1`, sqlHash256(slabID)).Scan(
			&dbID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.Version, &slab.PinnedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get slab %q: %w", slabID, err)
		}
		slab.ID = slabID

		rows, err := tx.Query(ctx, `
			SELECT s.sector_root, h.public_key, csm.contract_id
			FROM sectors s
			INNER JOIN slab_sectors ss ON s.id = ss.sector_id
			LEFT JOIN hosts h ON h.id = s.host_id
			LEFT JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
			WHERE ss.slab_id = $1
			ORDER BY ss.slab_index ASC`, dbID)
		if err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var sector slabs.Sector
			var hostKey sql.Null[sqlPublicKey]
			var contractID sql.Null[sqlHash256]

			if err := rows.Scan((*sqlHash256)(&sector.Root), &hostKey, &contractID); err != nil {
				return fmt.Errorf("failed to scan sector: %w", err)
			}

			if hostKey.Valid {
				sector.HostKey = (*types.PublicKey)(&hostKey.V)
			}
			if contractID.Valid {
				sector.ContractID = (*types.FileContractID)(&contractID.V)
			}
			slab.Sectors = append(slab.Sectors, sector)
		}
		return rows.Err()
	})
	return
}

// PinnedSlab retrieves a slab currently pinned by the account by its ID. A slab
// the account has unpinned is not returned, even if its row still exists pending
// the background prune.
func (s *Store) PinnedSlab(account proto.Account, slabID slabs.SlabID) (slab slabs.PinnedSlab, err error) {
	slab.ID = slabID
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		slab.Sectors = slab.Sectors[:0] // reuse same slice if transaction retries

		// require an account_slabs association so an unpinned slab reads as not found
		var dbID int64
		err = tx.QueryRow(ctx, `SELECT s.id, s.encryption_key, s.min_shards, s.version
FROM slabs s
JOIN account_slabs a ON a.slab_id = s.id
JOIN accounts acc ON acc.id = a.account_id
WHERE s.digest = $1 AND acc.public_key = $2`, sqlHash256(slabID), sqlPublicKey(account)).Scan(
			&dbID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.Version)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get slab %q: %w", slabID, err)
		}

		rows, err := tx.Query(ctx, `SELECT s.sector_root, h.public_key
FROM slab_sectors ss
INNER JOIN sectors s ON (s.id = ss.sector_id)
LEFT JOIN hosts h ON (h.id = s.host_id)
WHERE ss.slab_id = $1 AND s.host_id IS NOT NULL
ORDER BY ss.slab_index ASC`, dbID)
		if err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var sector slabs.PinnedSector

			if err := rows.Scan((*sqlHash256)(&sector.Root), (*sqlPublicKey)(&sector.HostKey)); err != nil {
				return fmt.Errorf("failed to scan sector: %w", err)
			}
			slab.Sectors = append(slab.Sectors, sector)
		}

		if len(slab.Sectors) < int(slab.MinShards) {
			return fmt.Errorf("recovery requires at least %d sectors, slab has %d sectors: %w", slab.MinShards, len(slab.Sectors), slabs.ErrUnrecoverable)
		}
		return rows.Err()
	})
	return
}

// PruneSlabs unpins all of a user's slabs that are not currently connected to
// an object. Only slabs pinned before cutoff are eligible. The unpinned slabs
// and their sectors are queued for deletion by PruneDeletedSlabs rather than
// removed inline.
func (s *Store) PruneSlabs(account proto.Account, cutoff time.Time) error {
	var id int64
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		var err error
		id, _, err = accountID(ctx, tx, account)
		if err != nil {
			return fmt.Errorf("failed to get account ID: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	getSlabs := func(ctx context.Context, tx *txn, cursor, limit int64) ([]int64, error) {
		rows, err := tx.Query(ctx, `SELECT s.id
FROM slabs s
JOIN account_slabs a ON s.id = a.slab_id
WHERE a.account_id = $1
	AND a.slab_id > $2
	AND s.pinned_at < $3
	AND NOT EXISTS (
		SELECT 1
		FROM objects o
		JOIN object_slabs os ON o.id = os.object_id
		WHERE o.account_id = a.account_id
		AND os.slab_digest = s.digest
	)
ORDER BY a.slab_id
LIMIT $4
`, id, cursor, cutoff, limit)
		if err != nil {
			return nil, fmt.Errorf("failed to get unused slabs: %w", err)
		}
		return pgx.CollectRows(rows, pgx.RowTo[int64])
	}

	var cursor int64
	const batchSize = 100
	for {
		var nextCursor int64
		var exhausted bool
		err := s.transaction(func(ctx context.Context, tx *txn) error {
			nextCursor = cursor
			exhausted = false

			candidates, err := getSlabs(ctx, tx, cursor, batchSize)
			if err != nil {
				return fmt.Errorf("failed to get slabs to unpin: %w", err)
			}
			exhausted = len(candidates) < batchSize
			if len(candidates) == 0 {
				return nil
			}
			nextCursor = candidates[len(candidates)-1]

			return s.unpinUnreferencedSlabs(ctx, tx, id, candidates, &cutoff)
		})
		if err != nil {
			return err
		}
		cursor = nextCursor
		if exhausted {
			return nil
		}
	}
}
