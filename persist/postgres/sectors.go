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
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
)

// MarkSectorsLost marks the sectors as lost by setting both the contract ID and
// host ID to NULL. This is meant to be used in 2 cases:
// - The host reports that the sector is lost (e.g. when pinning it, during the integrity check or when fetching it for migration)
// - The host has failed the integrity check for that sector enough times
func (s *Store) MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error {
	if len(roots) == 0 {
		return nil
	}
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		sqlRoots := make([]sqlHash256, len(roots))
		for i, root := range roots {
			sqlRoots[i] = sqlHash256(root)
		}
		resp, err := tx.Exec(ctx, `
			UPDATE sectors
			SET contract_sectors_map_id = NULL, host_id = NULL
			WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)
			AND sector_root = ANY($2)
		`, sqlPublicKey(hostKey), sqlRoots)
		if err != nil {
			return err
		} else if resp.RowsAffected() == 0 {
			return nil
		}
		_, err = tx.Exec(ctx, `
			UPDATE hosts
			SET lost_sectors = lost_sectors + $1
			WHERE public_key = $2
		`, resp.RowsAffected(), sqlPublicKey(hostKey))
		if err != nil {
			return fmt.Errorf("failed to increment host's lost sectors: %w", err)
		}
		return nil
	})
	return err
}

// RecordIntegrityCheck records the result of integrity checks for the given
// sectors stored on the given host.
func (s *Store) RecordIntegrityCheck(ctx context.Context, success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		sqlRoots := make([]sqlHash256, len(roots))
		for i, root := range roots {
			sqlRoots[i] = sqlHash256(root)
		}
		var err error
		if success {
			_, err = tx.Exec(ctx, `
				UPDATE sectors
				SET next_integrity_check = $1, consecutive_failed_checks = 0
				WHERE host_id = (SELECT id FROM hosts WHERE public_key = $2) AND
					sector_root = ANY($3)
			`, nextCheck, sqlPublicKey(hostKey), sqlRoots)
		} else {
			_, err = tx.Exec(ctx, `
				UPDATE sectors
				SET next_integrity_check = $1, consecutive_failed_checks = consecutive_failed_checks + 1
				WHERE host_id = (SELECT id FROM hosts WHERE public_key = $2) AND
					sector_root = ANY($3)
			`, nextCheck, sqlPublicKey(hostKey), sqlRoots)
		}
		return err
	})
}

// SectorsForIntegrityCheck returns up to `limit` sectors that are due for an
// integrity check.
func (s *Store) SectorsForIntegrityCheck(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	var sectors []types.Hash256
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			WITH hid AS (
				SELECT id FROM hosts WHERE public_key = $1
			)
			SELECT sector_root
			FROM sectors
			WHERE
				host_id = (SELECT id FROM hid)
				AND next_integrity_check <= NOW()
			ORDER BY next_integrity_check ASC
			LIMIT $2
		`, sqlPublicKey(hostKey), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				return err
			}
			sectors = append(sectors, root)
		}
		return rows.Err()
	})
	return sectors, err
}

// MarkFailingSectorsLost marks sectors as lost if they have failed the
// integrity checks >= maxChecks times.
func (s *Store) MarkFailingSectorsLost(ctx context.Context, hostKey types.PublicKey, maxChecks uint) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		resp, err := tx.Exec(ctx, `
			UPDATE sectors
			SET contract_sectors_map_id = NULL, host_id = NULL
			WHERE
				host_id = (SELECT id FROM hosts WHERE public_key = $1)
				AND consecutive_failed_checks >= $2
		`, sqlPublicKey(hostKey), maxChecks)
		if err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE hosts
			SET lost_sectors = lost_sectors + $1
			WHERE public_key = $2
		`, resp.RowsAffected(), sqlPublicKey(hostKey))
		if err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		return nil
	})
}

// PinSlab adds a slab to the database for pinning. The slab is associated with
// the provided account.
func (s *Store) PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab slabs.SlabPinParams) (slabs.SlabID, error) {
	digest, err := slab.Digest()
	if err != nil {
		return slabs.SlabID{}, fmt.Errorf("failed to calculate slab digest: %w", err)
	}
	return digest, s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE public_key = $1", sqlPublicKey(account)).Scan(&accountID)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return err
		}

		// insert slab
		var slabID int64
		var existingSlab bool
		err = tx.QueryRow(ctx, `
			INSERT INTO slabs (digest, encryption_key, min_shards)
			VALUES ($1, $2, $3)
			ON CONFLICT (digest) DO UPDATE SET pinned_at = NOW()
			RETURNING id
			`, sqlHash256(digest), sqlHash256(slab.EncryptionKey), slab.MinShards).Scan(&slabID)
		if errors.Is(err, sql.ErrNoRows) {
			// slab already exists, fetch its slab id
			existingSlab = true
			err = tx.QueryRow(ctx, `SELECT id FROM slabs WHERE digest = $1`, sqlHash256(digest)).Scan(&slabID)
		}
		if err != nil {
			return err
		}

		// insert slab into join table
		_, err = tx.Exec(ctx, `
			INSERT INTO account_slabs (account_id, slab_id) VALUES ($1, $2)
			ON CONFLICT (account_id, slab_id) DO NOTHING
		`, accountID, slabID)
		if err != nil {
			return fmt.Errorf("failed to insert slab into account_slabs: %w", err)
		}

		// if the slab already existed, we don't need to insert the sectors
		if existingSlab {
			return nil
		}

		// update slab stats
		if err := s.incrementNumSlabs(ctx, tx, 1); err != nil {
			return fmt.Errorf("failed to increment number of slabs: %w", err)
		}

		// insert slab's sectors in a single batch
		batch := &pgx.Batch{}
		sectorIDs := make([]int64, len(slab.Sectors))
		for i, sector := range slab.Sectors {
			batch.Queue(`
				INSERT INTO sectors (sector_root, host_id, next_integrity_check)
				VALUES ($1, (SELECT id FROM hosts WHERE public_key = $2), $3)
				ON CONFLICT (sector_root) DO UPDATE SET sector_root=EXCLUDED.sector_root, uploaded_at=NOW()
				RETURNING id
			`, sqlHash256(sector.Root), sqlPublicKey(sector.HostKey), nextIntegrityCheck).QueryRow(func(row pgx.Row) error {
				return row.Scan(&sectorIDs[i])
			})
		}

		// fetch sector IDs
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("failed to insert sectors: %w", err)
		}

		// insert slab sectors into join table
		batch = &pgx.Batch{}
		for i, sectorID := range sectorIDs {
			batch.Queue(`
				INSERT INTO slab_sectors (slab_id, slab_index, sector_id)
				VALUES ($1, $2, $3)
				ON CONFLICT (slab_id, slab_index) DO NOTHING
			`, slabID, i, sectorID)
		}
		if err = tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("failed to insert slab sectors: %w", err)
		}
		return nil
	})
}

// UnpinSlab removes the association between the account and the given slab. If
// this slab was only referenced by the given account, it will also be deleted.
// The sectors are potentially orphaned and will be removed by a background
// process.
func (s *Store) UnpinSlab(ctx context.Context, accountID proto.Account, slabID slabs.SlabID) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		// delete the association between the account and the slab
		var sID int64
		err := tx.QueryRow(ctx, `
			DELETE FROM account_slabs
			WHERE
				account_id = (SELECT id FROM accounts WHERE public_key = $1) AND
				slab_id = (SELECT id FROM slabs WHERE digest = $2)
			RETURNING slab_id`, sqlPublicKey(accountID), sqlHash256(slabID)).Scan(&sID)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to unpin slab: %w", err)
		}

		// return early if the slab is pinned by another account
		var pinned bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM account_slabs WHERE slab_id = $1)`, sID).Scan(&pinned)
		if err != nil {
			return fmt.Errorf("failed to check if slab was pinned: %w", err)
		} else if pinned {
			return nil
		}

		// prune the slab and its sectors
		batch := &pgx.Batch{}
		batch.Queue(`
			WITH candidate_sectors AS (
				SELECT ss.sector_id
				FROM slab_sectors ss
				WHERE ss.slab_id = $1 AND NOT EXISTS (
					SELECT 1
					FROM slab_sectors ss2
					WHERE ss2.sector_id = ss.sector_id AND ss2.slab_id <> $1
				)
			)
			DELETE FROM sectors WHERE id IN (SELECT sector_id FROM candidate_sectors);`, sID)
		batch.Queue(`DELETE FROM slabs WHERE id = $1`, sID)
		if err := tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("failed to prune slab: %w", err)
		}

		// update slab stats
		if err := s.incrementNumSlabs(ctx, tx, -1); err != nil {
			return fmt.Errorf("failed to decrement number of slabs: %w", err)
		}

		return nil
	})
}

// SlabIDs returns the IDs of slabs associated with the given account. The IDs
// are returned in descending order of the `pinned_at` timestamp, which is the
// time when the slab was pinned to the indexer.
func (s *Store) SlabIDs(ctx context.Context, accountID proto.Account, offset, limit int) ([]slabs.SlabID, error) {
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var ids []slabs.SlabID
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, `SELECT digest
			FROM slabs s
			INNER JOIN account_slabs ac ON s.id = ac.slab_id
			INNER JOIN accounts a ON a.id = ac.account_id
			WHERE a.public_key = $1
			ORDER BY s.pinned_at DESC
			LIMIT $2 OFFSET $3`, sqlPublicKey(accountID), limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query slab digests: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id slabs.SlabID
			if err := rows.Scan((*sqlHash256)(&id)); err != nil {
				return fmt.Errorf("failed to scan slab digest: %w", err)
			}
			ids = append(ids, id)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return ids, nil
}

// Slabs returns the slabs with the given IDs from the database.
func (s *Store) Slabs(ctx context.Context, accountID proto.Account, slabIDs []slabs.SlabID) ([]slabs.Slab, error) {
	if len(slabIDs) == 0 {
		return nil, nil
	}

	results := make([]slabs.Slab, len(slabIDs))
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		dbIDMap := make(map[int64]int)
		var dbIDs []int64
		slabBatch := &pgx.Batch{}
		for i, slabID := range slabIDs {
			slabBatch.Queue(`SELECT s.id, s.encryption_key, s.min_shards, s.pinned_at
				FROM slabs s
				INNER JOIN account_slabs ac ON s.id = ac.slab_id
				INNER JOIN accounts a ON a.id = ac.account_id
				WHERE digest = $1 AND a.public_key = $2`, sqlHash256(slabID), sqlPublicKey(accountID)).QueryRow(func(row pgx.Row) error {
				results[i].ID = slabID
				var dbID int64
				if err := row.Scan(&dbID, (*sqlHash256)(&results[i].EncryptionKey), &results[i].MinShards, &results[i].PinnedAt); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						err = slabs.ErrSlabNotFound
					}
					return fmt.Errorf("failed to get slab %q: %w", slabID, err)
				}
				dbIDs = append(dbIDs, dbID)
				dbIDMap[dbID] = i
				return nil
			})
		}
		if err := tx.Tx.SendBatch(ctx, slabBatch).Close(); err != nil {
			return fmt.Errorf("failed to get slabs: %w", err)
		}

		sectorsBatch := &pgx.Batch{}
		for _, slabID := range dbIDs {
			sectorsBatch.Queue(`SELECT s.sector_root, h.public_key, csm.contract_id
FROM sectors s
INNER JOIN slab_sectors ss ON s.id = ss.sector_id
LEFT JOIN hosts h ON h.id = s.host_id
LEFT JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
WHERE ss.slab_id = $1
ORDER BY ss.slab_index ASC`, slabID).Query(func(rows pgx.Rows) error {
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
					results[dbIDMap[slabID]].Sectors = append(results[dbIDMap[slabID]].Sectors, sector)
				}
				return rows.Err()
			})
		}
		if err := tx.Tx.SendBatch(ctx, sectorsBatch).Close(); err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}

		return nil
	})
	return results, err
}

// PinSectors pins a batch of sector roots to a given contract. This also
// updates the host the sector is associated with to the host that we have the
// contract with. That way, we can avoid a race where the host changes in the
// meantime and the contract then no longer matches the host.
func (s *Store) PinSectors(ctx context.Context, contractID types.FileContractID, roots []types.Hash256) error {
	if len(roots) == 0 {
		return nil
	}

	sqlRoots := make([]sqlHash256, len(roots))
	for i, root := range roots {
		sqlRoots[i] = sqlHash256(root)
	}

	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		resp, err := tx.Exec(ctx, `
			UPDATE sectors
			SET (host_id, contract_sectors_map_id) = (result.host_id, result.contract_sectors_map_id)
			FROM (
				SELECT hosts.id AS host_id, contracts.id AS contract_sectors_map_id
				FROM contract_sectors_map
				INNER JOIN contracts ON contracts.contract_id = contract_sectors_map.contract_id
				INNER JOIN hosts ON contracts.host_id = hosts.id
				WHERE contract_sectors_map.contract_id = $1
			) AS result
			WHERE sector_root = ANY($2) AND result.contract_sectors_map_id IS NOT NULL
		`, sqlHash256(contractID), sqlRoots)
		if err != nil {
			return err
		} else if resp.RowsAffected() == 0 {
			// if no sectors were updated, check if the contract exists
			var exists bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM contracts WHERE contracts.contract_id = $1)", sqlHash256(contractID)).
				Scan(&exists); err != nil {
				return fmt.Errorf("failed to check if contract exists: %w", err)
			} else if !exists {
				return contracts.ErrNotFound
			}
		}
		return nil
	})
}

// PruneUnpinnableSectors sets the host ID for sectors that haven't been pinned
// by the threshold time to NULL.
func (s *Store) PruneUnpinnableSectors(ctx context.Context, threshold time.Time) error {
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
            UPDATE sectors
            SET host_id = NULL
            WHERE host_id IS NOT NULL
	            AND contract_sectors_map_id IS NULL
	            AND uploaded_at <= $1`, threshold)
		if err != nil {
			return fmt.Errorf("failed to prune unpinnable sectors: %w", err)
		}
		return nil
	})
	return err
}

// UnpinnedSectors returns up to 'limit' sectors which have been uploaded to a host but
// not pinned to a contract yet.
func (s *Store) UnpinnedSectors(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	roots := make([]types.Hash256, 0, limit)
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			WITH hid AS (
				SELECT id FROM hosts WHERE public_key = $1
			)
			SELECT sector_root
			FROM sectors
				WHERE host_id = (SELECT id FROM hid)
				AND contract_sectors_map_id IS NULL
			ORDER BY uploaded_at ASC
			LIMIT $2
		`, sqlPublicKey(hostKey), limit)
		if err != nil {
			return fmt.Errorf("failed to query sectors: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var root types.Hash256
			err = rows.Scan((*sqlHash256)(&root))
			if err != nil {
				return fmt.Errorf("failed to scan unpinned sector: %w", err)
			}
			roots = append(roots, root)
		}
		return rows.Err()
	})
	return roots, err
}

// UnhealthySlabs returns the ID of slabs which have at least one sector that
// needs to be migrated to a new host and hasn't had a repair attempted since
// 'maxRepairAttempt'. The condition for such a slab is that it either has:
// a). a sector that is not stored on a host (host_id == null)
// b). a sector that is stored in a bad contract (contract_id != null && contract.good = false)
// If a slab is found, it will have its last_repair_attempt updated to the time
// of the call. To prevent subsequent or parallel calls from returning the same slab.
//
// NOTE: For the sake of scalability, we don't prioritize any slabs and instead
// simply fetch the first batch we find.
func (s *Store) UnhealthySlabs(ctx context.Context, maxRepairAttempt time.Time, limit int) ([]slabs.SlabID, error) {
	now := time.Now()
	if maxRepairAttempt.After(now) {
		return nil, fmt.Errorf("maxRepairAttempt (%v) must be in the past (current time: %v)", maxRepairAttempt, now) // developer error
	}

	var results []slabs.SlabID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		for range limit {
			var slabID slabs.SlabID
			err := tx.QueryRow(ctx, `UPDATE slabs
				SET last_repair_attempt = $1
				WHERE id = (
					SELECT slabs.id
					FROM slabs
					INNER JOIN slab_sectors ON slabs.id = slab_sectors.slab_id
					INNER JOIN sectors ON slab_sectors.sector_id = sectors.id
					LEFT JOIN contract_sectors_map csm ON sectors.contract_sectors_map_id = csm.id
					LEFT JOIN contracts ON csm.contract_id = contracts.contract_id
					WHERE
						(
							-- stored on bad contract
							(sectors.contract_sectors_map_id IS NOT NULL AND contracts.good = FALSE) OR
							-- not stored on any host
							(sectors.host_id IS NULL)
						)
						AND (slabs.last_repair_attempt < $2)
					LIMIT 1
				)
				RETURNING digest
		`, now, maxRepairAttempt).Scan((*sqlHash256)(&slabID))
			if errors.Is(err, sql.ErrNoRows) {
				break
			} else if err != nil {
				return fmt.Errorf("failed to query unhealthy slabs: %w", err)
			}
			results = append(results, slabID)
		}
		return nil
	})
	return results, err
}

// MigrateSector updates a sector that was just migrated in the database to be
// linked to the new host identified by 'hostKey'. This will reset the contract
// ID since a freshly migrated sector isn't pinned yet. To pin a sector
// 'PinSectors' is used. If the host is not found, e.g. due to being deleted in
// the meantime, this operation is a no-op.
func (s *Store) MigrateSector(ctx context.Context, root types.Hash256, hostKey types.PublicKey) (bool, error) {
	var migrated bool
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		resp, err := tx.Exec(ctx, `
			UPDATE sectors
			SET host_id = hosts.id, contract_sectors_map_id = NULL, consecutive_failed_checks = 0, uploaded_at=NOW()
			FROM hosts
			WHERE sector_root = $1 AND hosts.public_key = $2
		`, sqlHash256(root), sqlPublicKey(hostKey))
		migrated = resp.RowsAffected() > 0
		return err
	})
	return migrated, err
}
