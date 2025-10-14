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

	sqlRoots := make([]sqlHash256, len(roots))
	for i, root := range roots {
		sqlRoots[i] = sqlHash256(root)
	}

	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, "SELECT id FROM hosts WHERE public_key = $1", sqlPublicKey(hostKey)).Scan(&hostID)
		if err != nil {
			return fmt.Errorf("failed to get host ID: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, contract_sectors_map_id
			FROM sectors
			WHERE host_id = $1 AND sector_root = ANY($2)`, hostID, sqlRoots)
		if err != nil {
			return fmt.Errorf("failed to query sectors: %w", err)
		}
		defer rows.Close()

		sectorIDs, pinned, unpinned, err := scanSectorIDs(rows)
		if err != nil {
			return fmt.Errorf("failed to scan sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}

		totalLost := pinned + unpinned
		if _, err := tx.Exec(ctx, `UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id = ANY($1)`, sectorIDs); err != nil {
			return fmt.Errorf("failed to mark sectors as lost: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE hosts SET lost_sectors = lost_sectors + $1 WHERE id = $2`, totalLost, hostID); err != nil {
			return fmt.Errorf("failed to increment host's lost sectors: %w", err)
		}
		if err := updateSectorStats(ctx, tx, -pinned, -unpinned, totalLost); err != nil {
			return fmt.Errorf("failed to update sector stats: %w", err)
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
	const batchSize = 1000
	for {
		updated, err := s.markFailingSectorsLostBatch(ctx, hostKey, maxChecks, batchSize)
		if err != nil {
			return err
		} else if updated < batchSize {
			break
		}
	}
	return nil
}

// markFailingSectorsLostBatch marks a batch of failing sectors as lost. We have
// to batch it because we first need to select all sectors to update in order to
// correctly updated the pinned sectors statistics.
func (s *Store) markFailingSectorsLostBatch(ctx context.Context, hostKey types.PublicKey, maxChecks, batchSize uint) (int64, error) {
	var totalLost int64
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, "SELECT id FROM hosts WHERE public_key = $1", sqlPublicKey(hostKey)).Scan(&hostID)
		if err != nil {
			return fmt.Errorf("failed to get host ID: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, contract_sectors_map_id
			FROM sectors
			WHERE host_id = $1 AND consecutive_failed_checks >= $2
			LIMIT $3
		`, hostID, maxChecks, batchSize)
		if err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		defer rows.Close()

		sectorIDs, pinned, unpinned, err := scanSectorIDs(rows)
		if err != nil {
			return fmt.Errorf("failed to scan sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}

		totalLost = pinned + unpinned
		if _, err := tx.Exec(ctx, `UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id = ANY($1)`, sectorIDs); err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE hosts SET lost_sectors = lost_sectors + $1 WHERE id = $2`, totalLost, hostID); err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		if err := updateSectorStats(ctx, tx, -pinned, -unpinned, totalLost); err != nil {
			return fmt.Errorf("failed to update sector stats: %w", err)
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return totalLost, nil
}

// PinSlabs adds slabs to the database for pinning. The slabs are associated
// with the provided account.
func (s *Store) PinSlabs(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) ([]slabs.SlabID, error) {
	var digests []slabs.SlabID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		var pinnedData, maxPinnedData uint64
		err := tx.QueryRow(ctx, "UPDATE accounts SET last_used = NOW() WHERE public_key = $1 RETURNING id, pinned_data, max_pinned_data", sqlPublicKey(account)).Scan(&accountID, &pinnedData, &maxPinnedData)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return err
		}

		for _, slab := range toPin {
			digest, err := slab.Digest()
			if err != nil {
				return fmt.Errorf("failed to calculate slab digest: %w", err)
			}
			digests = append(digests, digest)

			// insert slab
			var slabID int64
			var existingSlab bool
			err = tx.QueryRow(ctx, `
			INSERT INTO slabs (digest, encryption_key, min_shards)
			VALUES ($1, $2, $3)
			ON CONFLICT (digest) DO UPDATE SET pinned_at = NOW()
			RETURNING id
			`, sqlHash256(digest), sqlHash256(slab.EncryptionKey), slab.MinShards).Scan(&slabID)
			if err != nil {
				return err
			}

			// insert slab into join table
			res, err := tx.Exec(ctx, `
			INSERT INTO account_slabs (account_id, slab_id) VALUES ($1, $2)
			ON CONFLICT (account_id, slab_id) DO NOTHING
		`, accountID, slabID)
			if err != nil {
				return fmt.Errorf("failed to insert slab into account_slabs: %w", err)
			}

			// check if pinning the slab would exceed the account's storage limit
			// and if not, update the account's pinned data NOTE: we perform this
			// check here since we need to know if the slab is a new slab or whether
			// it was just repinned.
			if res.RowsAffected() > 0 {
				newPinnedData := pinnedData + slab.Size()
				if newPinnedData > maxPinnedData {
					return accounts.ErrStorageLimitExceeded
				}

				_, err := tx.Exec(ctx, `UPDATE accounts SET last_used=NOW(), pinned_data = $1 WHERE id = $2`, newPinnedData, accountID)
				if err != nil {
					return fmt.Errorf("failed to update account's pinned data: %w", err)
				}
			}

			// if the slab already existed, we don't need to insert the sectors
			if existingSlab {
				continue
			}

			// update slab stats
			if err := incrementNumSlabs(ctx, tx, 1); err != nil {
				return fmt.Errorf("failed to increment number of slabs: %w", err)
			}

			// insert slab's sectors in a single batch
			batch := &pgx.Batch{}
			for _, sector := range slab.Sectors {
				batch.Queue(`
				INSERT INTO sectors (sector_root, host_id, next_integrity_check)
				SELECT $1, h.id, $3
				FROM hosts h
				WHERE h.public_key = $2
				ON CONFLICT (sector_root) DO UPDATE SET uploaded_at=NOW()
				RETURNING id, (xmax = 0) AS inserted`,
					sqlHash256(sector.Root),
					sqlPublicKey(sector.HostKey),
					nextIntegrityCheck)
			}

			var unpinned int64
			br := tx.SendBatch(ctx, batch)
			sectorIDs := make([]int64, len(slab.Sectors))
			for i, sector := range slab.Sectors {
				var inserted bool
				if err := br.QueryRow().Scan(&sectorIDs[i], &inserted); err != nil {
					br.Close()
					if errors.Is(err, sql.ErrNoRows) {
						return fmt.Errorf("unknown host %q for sector", sector.HostKey)
					}
					return fmt.Errorf("failed to insert sector %q: %w", sector.Root, err)
				} else if inserted {
					unpinned++
				}
			}
			br.Close()

			// update number of unpinned sectors
			if unpinned > 0 {
				if err := incrementNumUnpinnedSectors(ctx, tx, unpinned); err != nil {
					return fmt.Errorf("failed to increment number of unpinned sectors: %w", err)
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
			}
		}
		return nil
	})
	return digests, err
}

func (s *Store) unpinSlabs(ctx context.Context, tx *txn, accountID int64, slabIDs []slabs.SlabID) error {
	var args []sqlHash256
	for _, slabID := range slabIDs {
		args = append(args, sqlHash256(slabID))
	}

	// delete the association between the account and the slab
	rows, err := tx.Query(ctx, `DELETE FROM account_slabs a
USING slabs s
WHERE a.account_id = $1
  AND a.slab_id = s.id
  AND s.digest = ANY($2)
RETURNING a.slab_id;`, accountID, args)
	if err != nil {
		return fmt.Errorf("failed to unpin slab: %w", err)
	}
	defer rows.Close()

	var sIDs []int64
	for rows.Next() {
		var sID int64
		if err := rows.Scan(&sID); err != nil {
			return fmt.Errorf("failed to scan slab ID: %w", err)
		}

		sIDs = append(sIDs, sID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get slab IDs: %w", err)
	}
	if len(sIDs) == 0 {
		return nil
	}

	// update the account's pinned data
	_, err = tx.Exec(ctx, `
			UPDATE accounts
			SET pinned_data = pinned_data - (
				SELECT COUNT(*) * $1
				FROM slab_sectors
				WHERE slab_id = ANY($2)
			)
			WHERE id = $3
		`, proto.SectorSize, sIDs, accountID)
	if err != nil {
		return fmt.Errorf("failed to update account's pinned data: %w", err)
	}

	// ignore the slabs that are pinned by another account
	rows, err = tx.Query(ctx, `SELECT slab_id FROM account_slabs WHERE slab_id = ANY($1)`, sIDs)
	if err != nil {
		return fmt.Errorf("failed to check if slab was pinned: %w", err)
	}
	defer rows.Close()

	seen := make(map[int64]struct{})
	for rows.Next() {
		var sID int64
		if err := rows.Scan(&sID); err != nil {
			return fmt.Errorf("failed to check pinned slab: %w", err)
		}
		seen[sID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get pinned slabs: %w", err)
	}

	// get all of the slabs that are not pinned by another account
	var toDelete []int64
	for _, sID := range sIDs {
		if _, ok := seen[sID]; ok {
			continue
		}
		toDelete = append(toDelete, sID)
	}

	// prune the slab and its sectors
	batch := &pgx.Batch{}
	batch.Queue(`
				WITH candidate_sectors AS (
					SELECT ss.sector_id
					FROM slab_sectors ss
					WHERE ss.slab_id = ANY($1) AND NOT EXISTS (
						SELECT 1
						FROM slab_sectors ss2
						WHERE ss2.sector_id = ss.sector_id AND ss2.slab_id <> ANY($1)
					)
				)
				DELETE FROM sectors WHERE id IN (SELECT sector_id FROM candidate_sectors);`, toDelete)
	batch.Queue(`DELETE FROM slabs WHERE id = ANY($1)`, toDelete)
	if err := tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("failed to prune slab: %w", err)
	}

	// update slab stats
	if err := incrementNumSlabs(ctx, tx, -int64(len(toDelete))); err != nil {
		return fmt.Errorf("failed to decrement number of slabs: %w", err)
	}

	return nil
}

// UnpinSlab removes the association between the account and the given slab. If
// this slab was only owned by the given account, it will also be deleted.  The
// sectors of the slab will also be removed in that case.
func (s *Store) UnpinSlab(ctx context.Context, account proto.Account, slabID slabs.SlabID) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		id, err := accountID(ctx, tx, account)
		if err != nil {
			return fmt.Errorf("failed to get account ID: %w", err)
		}

		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM account_slabs WHERE account_id = $1 and slab_id = (SELECT id FROM slabs WHERE digest = $2))`, id, sqlHash256(slabID)).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check if slab exists: %w", err)
		} else if !exists {
			return slabs.ErrSlabNotFound
		}

		if err := s.unpinSlabs(ctx, tx, id, []slabs.SlabID{slabID}); err != nil {
			return fmt.Errorf("failed to unpin slab: %w", err)
		}
		return nil
	})
}

// SlabIDs returns the IDs of slabs associated with the given account. The IDs
// are returned in descending order of the `pinned_at` timestamp, which is the
// time when the slab was pinned to the indexer.
func (s *Store) SlabIDs(ctx context.Context, account proto.Account, offset, limit int) ([]slabs.SlabID, error) {
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
			LIMIT $2 OFFSET $3`, sqlPublicKey(account), limit, offset)
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
func (s *Store) Slabs(ctx context.Context, account proto.Account, slabIDs []slabs.SlabID) ([]slabs.Slab, error) {
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
				WHERE digest = $1 AND a.public_key = $2`, sqlHash256(slabID), sqlPublicKey(account)).QueryRow(func(row pgx.Row) error {
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
		var hostID, contractMapID int64
		err := tx.QueryRow(ctx, `
			SELECT c.host_id, csm.id
			FROM contract_sectors_map csm
			INNER JOIN contracts c ON c.contract_id = csm.contract_id
			WHERE csm.contract_id = $1
		`, sqlHash256(contractID)).Scan(&hostID, &contractMapID)
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.ErrNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get contract's host: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, contract_sectors_map_id
			FROM sectors
			WHERE sector_root = ANY($1) AND (contract_sectors_map_id IS NULL OR contract_sectors_map_id != $2)`, sqlRoots, contractMapID)
		if err != nil {
			return fmt.Errorf("failed to query sectors: %w", err)
		}
		defer rows.Close()

		sectorIDs, _, unpinned, err := scanSectorIDs(rows)
		if err != nil {
			return fmt.Errorf("failed to scan sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx, `UPDATE sectors SET host_id = $1, contract_sectors_map_id = $2 WHERE id = ANY($3)`, hostID, contractMapID, sectorIDs); err != nil {
			return fmt.Errorf("failed to pin sectors: %w", err)
		} else if unpinned > 0 {
			if err := incrementNumPinnedSectors(ctx, tx, unpinned); err != nil {
				return fmt.Errorf("failed to update number of pinned sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, -unpinned); err != nil {
				return fmt.Errorf("failed to update number of unpinned sectors: %w", err)
			}
		}

		return nil
	})
}

// MarkSectorsUnpinnable sets the host ID for sectors that haven't been pinned
// by the threshold time to NULL.
func (s *Store) MarkSectorsUnpinnable(ctx context.Context, threshold time.Time) error {
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `
            UPDATE sectors
            SET host_id = NULL
            WHERE host_id IS NOT NULL
	            AND contract_sectors_map_id IS NULL
	            AND uploaded_at <= $1`, threshold)
		if err != nil {
			return fmt.Errorf("failed to prune unpinnable sectors: %w", err)
		} else if unpinnable := res.RowsAffected(); unpinnable > 0 {
			if err := incrementNumUnpinnableSectors(ctx, tx, unpinnable); err != nil {
				return fmt.Errorf("failed to increment unpinnable sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, -unpinnable); err != nil {
				return fmt.Errorf("failed to decrement unpinned sectors: %w", err)
			}
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
func (s *Store) UnhealthySlabs(ctx context.Context, maxRepairAttempt time.Time, limit int) (results []slabs.SlabID, err error) {
	now := time.Now()
	if maxRepairAttempt.After(now) {
		return nil, fmt.Errorf("maxRepairAttempt (%v) must be in the past (current time: %v)", maxRepairAttempt, now) // developer error
	}

	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		const query = `SELECT s.id, s.digest
FROM slabs s
WHERE s.last_repair_attempt < $1
AND EXISTS (
  SELECT 1
  FROM slab_sectors ss
  JOIN sectors sec ON sec.id = ss.sector_id
  LEFT JOIN contract_sectors_map csm ON csm.id = sec.contract_sectors_map_id
  LEFT JOIN contracts c ON c.contract_id = csm.contract_id
  WHERE ss.slab_id = s.id
    AND (
      sec.host_id IS NULL
      OR (sec.contract_sectors_map_id IS NOT NULL
          AND (c.good = FALSE OR c.state NOT IN ($2, $3)))
    )
)
ORDER BY s.last_repair_attempt ASC
LIMIT $4;`
		var selected []int64
		rows, err := tx.Query(ctx, query, maxRepairAttempt, sqlContractState(contracts.ContractStateActive), sqlContractState(contracts.ContractStatePending), limit)
		if err != nil {
			return fmt.Errorf("failed to query unhealthy slabs: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id int64
			var digest slabs.SlabID
			if err := rows.Scan(&id, (*sqlHash256)(&digest)); err != nil {
				return fmt.Errorf("failed to scan unhealthy slab: %w", err)
			}
			results = append(results, digest)
			selected = append(selected, id)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to read unhealthy slabs: %w", err)
		}
		rows.Close()
		if len(selected) == 0 {
			return nil
		}

		// update last_repair_attempt for the selected slabs
		if _, err := tx.Exec(ctx, `UPDATE slabs SET last_repair_attempt = $1 WHERE id = ANY($2)`, now, selected); err != nil {
			return fmt.Errorf("failed to update last_repair_attempt: %w", err)
		}
		return nil
	})
	return
}

// MigrateSector updates a sector that was just migrated in the database to be
// linked to the new host identified by 'hostKey'. This will reset the contract
// ID since a freshly migrated sector isn't pinned yet. To pin a sector
// 'PinSectors' is used. If the host is not found, e.g. due to being deleted in
// the meantime, this operation is a no-op.
func (s *Store) MigrateSector(ctx context.Context, root types.Hash256, hostKey types.PublicKey) (migrated bool, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID sql.NullInt64
		var contractMapID sql.NullInt64
		err := tx.QueryRow(ctx, `
			SELECT host_id, contract_sectors_map_id
			FROM sectors
			WHERE sector_root = $1`, sqlHash256(root)).Scan(&hostID, &contractMapID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // not migrated
		}

		resp, err := tx.Exec(ctx, `
			UPDATE sectors
			SET host_id = hosts.id, contract_sectors_map_id = NULL, consecutive_failed_checks = 0, num_migrated = num_migrated + 1, uploaded_at=NOW()
			FROM hosts
			WHERE sector_root = $1 AND hosts.public_key = $2
		`, sqlHash256(root), sqlPublicKey(hostKey))
		if err != nil {
			return err
		} else if resp.RowsAffected() == 0 {
			return nil
		}

		migrated = true
		if err := incrementNumMigratedSectors(ctx, tx); err != nil {
			return fmt.Errorf("failed to increment number of migrated sectors: %w", err)
		} else if contractMapID.Valid {
			// sector was pinned before, update stats
			if err := incrementNumPinnedSectors(ctx, tx, -1); err != nil {
				return fmt.Errorf("failed to decrement pinned sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, 1); err != nil {
				return fmt.Errorf("failed to increment unpinned sectors: %w", err)
			}
		} else if !hostID.Valid {
			// sector was unpinnable before, update stats
			if err := incrementNumUnpinnableSectors(ctx, tx, -1); err != nil {
				return fmt.Errorf("failed to decrement unpinnable sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, 1); err != nil {
				return fmt.Errorf("failed to increment unpinned sectors: %w", err)
			}
		}

		return nil
	})
	return
}

func updateSectorStats(ctx context.Context, tx *txn, pinnedDelta, unpinnedDelta, unpinnableDelta int64) error {
	if err := incrementNumPinnedSectors(ctx, tx, pinnedDelta); err != nil {
		return fmt.Errorf("failed to update pinned sectors: %w", err)
	}
	if err := incrementNumUnpinnedSectors(ctx, tx, unpinnedDelta); err != nil {
		return fmt.Errorf("failed to update unpinned sectors: %w", err)
	}
	if err := incrementNumUnpinnableSectors(ctx, tx, unpinnableDelta); err != nil {
		return fmt.Errorf("failed to update unpinnable sectors: %w", err)
	}
	return nil
}

// scanSectorIDs scans sector IDs from the given rows. It also counts how many
// of the sectors are currently pinned and unpinned.
func scanSectorIDs(rows *rows) (sectorIDs []int64, pinned, unpinned int64, err error) {
	for rows.Next() {
		var sectorID int64
		var contractMapID sql.NullInt64
		if err := rows.Scan(&sectorID, &contractMapID); err != nil {
			return nil, 0, 0, fmt.Errorf("failed to scan sector row: %w", err)
		} else if contractMapID.Valid {
			pinned++
		} else {
			unpinned++
		}
		sectorIDs = append(sectorIDs, sectorID)
	}
	err = rows.Err()
	return
}
