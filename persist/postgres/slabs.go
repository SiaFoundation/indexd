package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
		var dbID int64
		err = tx.QueryRow(ctx, `SELECT s.id, s.encryption_key, s.min_shards, s.pinned_at FROM slabs s WHERE digest = $1`, sqlHash256(slabID)).Scan(
			&dbID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.PinnedAt)
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

// PinnedSlab retrieves a pinned slab from the database by its ID.
func (s *Store) PinnedSlab(account proto.Account, slabID slabs.SlabID) (slab slabs.PinnedSlab, err error) {
	slab.ID = slabID
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(ctx, `UPDATE accounts SET last_used = NOW() WHERE public_key = $1`, sqlPublicKey(account)); err != nil {
			return fmt.Errorf("failed to update last used: %w", err)
		}

		var dbID int64
		err = tx.QueryRow(ctx, `SELECT s.id, s.encryption_key, s.min_shards FROM slabs s WHERE digest = $1`, sqlHash256(slabID)).Scan(
			&dbID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards)
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

// PruneSlabs prunes all pinned slabs of a user not currently connected to an
// object.
func (s *Store) PruneSlabs(account proto.Account) error {
	var id int64
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		var err error
		id, err = accountID(ctx, tx, account)
		if err != nil {
			return fmt.Errorf("failed to get account ID: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	getSlabs := func(ctx context.Context, tx *txn, limit int64) ([]slabs.SlabID, error) {
		rows, err := tx.Query(ctx, `SELECT s.digest
FROM slabs s
JOIN account_slabs a ON s.id = a.slab_id
WHERE a.account_id = $1
	AND NOT EXISTS (
		SELECT 1
		FROM objects o
		JOIN object_slabs os ON o.id = os.object_id
		WHERE o.account_id = a.account_id
		AND os.slab_digest = s.digest
	)
LIMIT $2
`, id, limit)
		if err != nil {
			return nil, fmt.Errorf("failed to get unused slabs: %w", err)
		}
		defer rows.Close()

		var slabIDs []slabs.SlabID
		for rows.Next() {
			var slabID slabs.SlabID
			if err := rows.Scan((*sqlHash256)(&slabID)); err != nil {
				return nil, fmt.Errorf("failed to scan slab ID: %w", err)
			}
			slabIDs = append(slabIDs, slabID)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to get slab IDs: %w", err)
		} else if len(slabIDs) == 0 {
			return nil, nil
		}
		return slabIDs, nil
	}

	var exhausted bool
	const batchSize = 100
	for !exhausted {
		err := s.transaction(func(ctx context.Context, tx *txn) error {
			slabIDs, err := getSlabs(ctx, tx, batchSize)
			if err != nil {
				return fmt.Errorf("failed to get slabs to unpin: %w", err)
			} else if len(slabIDs) < batchSize {
				exhausted = true
			}
			if err := s.unpinSlabs(ctx, tx, id, slabIDs); err != nil {
				return fmt.Errorf("failed to unpin slabs: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
