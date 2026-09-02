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

// MarkSlabRepaired marks the slab as repaired or increments the failed repair
// count. If the repair was successful, the consecutive_failed_repairs counter
// is reset to zero. If the repair failed, the counter is incremented and the
// next repair attempt time is set using exponential backoff.
func (s *Store) MarkSlabRepaired(slabID slabs.SlabID, success bool) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		if success {
			if res, err := tx.Exec(ctx, `UPDATE slabs SET consecutive_failed_repairs = 0 WHERE digest = $1`, sqlHash256(slabID)); err != nil {
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
			SET consecutive_failed_repairs = $2, next_repair_attempt = $3
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
// the background prune. It returns ErrUnrecoverable if fewer sectors are still
// available than the slab's minimum shards.
func (s *Store) PinnedSlab(account proto.Account, slabID slabs.SlabID) (slab slabs.PinnedSlab, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		result, err := loadPinnedSlabs(ctx, tx, account, []slabs.SlabID{slabID})
		if err != nil {
			return err
		} else if len(result) == 0 {
			return slabs.ErrSlabNotFound
		}
		slab = result[0]

		available := 0
		for _, sector := range slab.Sectors {
			if sector.HostKey != (types.PublicKey{}) {
				available++
			}
		}
		if available < int(slab.MinShards) {
			return fmt.Errorf("recovery requires at least %d sectors, slab has %d available sectors: %w", slab.MinShards, available, slabs.ErrUnrecoverable)
		}
		return nil
	})
	return
}

// PinnedSlabs retrieves the slabs currently pinned by the account in request
// order, omitting slabs the account has not pinned. Unlike PinnedSlab, it does
// not check that the slabs are still recoverable.
func (s *Store) PinnedSlabs(account proto.Account, slabIDs []slabs.SlabID) (result []slabs.PinnedSlab, err error) {
	if len(slabIDs) == 0 {
		return nil, nil
	}
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		pinned, err := loadPinnedSlabs(ctx, tx, account, slabIDs)
		if err != nil {
			return err
		}
		result = pinned
		return nil
	})
	return
}

// loadPinnedSlabs returns the slabs the account has pinned, in request order.
func loadPinnedSlabs(ctx context.Context, tx *txn, account proto.Account, slabIDs []slabs.SlabID) ([]slabs.PinnedSlab, error) {
	sqlSlabIDs := make([]sqlHash256, len(slabIDs))
	for i, slabID := range slabIDs {
		sqlSlabIDs[i] = sqlHash256(slabID)
	}

	// require an account_slabs association so an unpinned slab is omitted
	slabsByID := make(map[slabs.SlabID]*slabs.PinnedSlab, len(slabIDs))
	slabsByDBID := make(map[int64]*slabs.PinnedSlab, len(slabIDs))
	dbIDs := make([]int64, 0, len(slabIDs))
	rows, err := tx.Query(ctx, `
		SELECT s.id, s.digest, s.encryption_key, s.min_shards, s.version
		FROM slabs s
		JOIN account_slabs a ON a.slab_id = s.id
		JOIN accounts acc ON acc.id = a.account_id
		WHERE acc.public_key = $1 AND s.digest = ANY($2::bytea[])
	`, sqlPublicKey(account), sqlSlabIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to query pinned slabs: %w", err)
	}
	err = forEachRow(rows, func(row pgx.CollectableRow) error {
		var dbID int64
		var slab slabs.PinnedSlab
		if err := row.Scan(&dbID, (*sqlHash256)(&slab.ID), (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.Version); err != nil {
			return err
		}
		slabsByID[slab.ID] = &slab
		slabsByDBID[dbID] = &slab
		dbIDs = append(dbIDs, dbID)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan pinned slabs: %w", err)
	} else if len(dbIDs) == 0 {
		return nil, nil
	}

	err = forEachSlabSector(ctx, tx, dbIDs, func(dbID int64, sector slabs.PinnedSector) error {
		slab, ok := slabsByDBID[dbID]
		if !ok {
			return fmt.Errorf("queried sector for unknown slab (developer error): %d", dbID)
		}
		slab.Sectors = append(slab.Sectors, sector)
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := make([]slabs.PinnedSlab, 0, len(slabIDs))
	for _, slabID := range slabIDs {
		if slab, ok := slabsByID[slabID]; ok {
			result = append(result, *slab)
		}
	}
	return result, nil
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
