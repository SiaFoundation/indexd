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
	"go.sia.tech/indexd/slabs"
)

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

// PinSlabs adds slabs to the database for pinning. The slabs are associated
// with the provided account.
func (s *Store) PinSlabs(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, toPin []slabs.SlabPinParams) ([]slabs.SlabID, error) {
	if len(toPin) == 0 {
		return nil, nil
	}
	var ids []slabs.SlabID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE public_key = $1", sqlPublicKey(account)).Scan(&accountID)
		if err != nil {
			return fmt.Errorf("%w: %v", accounts.ErrNotFound, account)
		}
		for i, slab := range toPin {
			slabID, err := s.pinSlab(ctx, tx, accountID, nextIntegrityCheck, slab)
			if err != nil {
				return fmt.Errorf("failed to pin slab %d: %w", i+1, err)
			}
			ids = append(ids, slabID)
		}
		return nil
	})
	return ids, err
}

// Slabs returns the slabs with the given IDs from the database.
func (s *Store) Slabs(ctx context.Context, accountID proto.Account, slabIDs []slabs.SlabID) ([]slabs.Slab, error) {
	if len(slabIDs) == 0 {
		return nil, nil
	}

	result := make([]slabs.Slab, 0, len(slabIDs))
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		for _, slabID := range slabIDs {
			// query slab
			var sid int64
			slab := slabs.Slab{ID: slabID}
			err := tx.QueryRow(ctx, `
				SELECT slabs.id, encryption_key, min_shards
				FROM slabs
				INNER JOIN account_slabs ON slabs.id = account_slabs.slab_id
				INNER JOIN accounts a ON a.id = account_slabs.account_id
				WHERE digest = $1 AND a.public_key = $2
			`, sqlHash256(slabID), sqlPublicKey(accountID)).Scan(&sid, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: %v", slabs.ErrSlabNotFound, slabID)
			} else if err != nil {
				return fmt.Errorf("failed to query slab %s: %w", slabID, err)
			}

			// query sectors
			err = func() error {
				rows, err := tx.Query(ctx, `
					SELECT sector_root, h.public_key, c.contract_id
					FROM sectors
					LEFT JOIN hosts h ON h.id = sectors.host_id
					LEFT JOIN contracts c ON c.id = sectors.contract_id
					WHERE slab_id = $1
					ORDER BY slab_index ASC
				`, sid)
				if err != nil {
					return fmt.Errorf("failed to query sectors for slab %s: %w", slabID, err)
				}
				defer rows.Close()
				for rows.Next() {
					var sector slabs.Sector
					var hostKey sql.Null[sqlPublicKey]
					var contractID sql.Null[sqlHash256]
					err = rows.Scan((*sqlHash256)(&sector.Root), asNullable(&hostKey), &contractID)
					if err != nil {
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
			}()
			if err != nil {
				return err
			}
			result = append(result, slab)
		}
		return nil
	})
	return result, err
}

// UnhealthySlab returns a slab that has at least one sector that needs to be
// migrated to a new host and hasn't had a repair attempted since
// 'maxRepairAttempt'. The condition for such a slab is that it either has:
// a). a sector that is not stored on a host (host_id == null)
// b). a sector that is stored in a bad contract (contract_id != null && contract.good = false)
// When no slab is found, ErrSlabNotFound is returned. If a slab is found, it
// will have its last_repair_attempt updated to the time of the call. To prevent
// subsequent or parallel calls from returning the same slab.
//
// NOTE: For the sake of scalability, we don't prioritize any slabs and instead
// simply fetch the first one that we can get.
func (s *Store) UnhealthySlab(ctx context.Context, maxRepairAttempt time.Time) (slabs.SlabID, error) {
	var slabID slabs.SlabID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `
			UPDATE slabs
			SET last_repair_attempt = NOW()
			WHERE id = (
				SELECT slabs.id
				FROM slabs
				INNER JOIN sectors ON slabs.id = sectors.slab_id
				LEFT JOIN contracts ON sectors.contract_id = contracts.id
				WHERE
					(
						-- stored on bad contract
						(sectors.contract_id IS NOT NULL AND contracts.good = FALSE) OR
						-- not stored on any host
						(sectors.host_id IS NULL)
					)
					AND (slabs.last_repair_attempt <= $1)
				LIMIT 1
			)
			RETURNING digest
		`, maxRepairAttempt).Scan((*sqlHash256)(&slabID))
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		}
		return err
	})
	return slabID, err
}

func (s *Store) pinSlab(ctx context.Context, tx *txn, accountID int64, nextIntegrityCheck time.Time, slab slabs.SlabPinParams) (slabs.SlabID, error) {
	digest, err := slab.Digest()
	if err != nil {
		return slabs.SlabID{}, err
	}

	// insert slab
	var slabID int64
	var existingSlab bool
	err = tx.QueryRow(ctx, `
		INSERT INTO slabs (digest, encryption_key, min_shards)
		VALUES ($1, $2, $3)
		ON CONFLICT (digest) DO NOTHING
		RETURNING id
		`, sqlHash256(digest), sqlHash256(slab.EncryptionKey), slab.MinShards).Scan(&slabID)
	if errors.Is(err, sql.ErrNoRows) {
		// slab already exists, fetch its slab id
		existingSlab = true
		err = tx.QueryRow(ctx, `SELECT id FROM slabs WHERE digest = $1`, sqlHash256(digest)).Scan(&slabID)
	}
	if err != nil {
		return slabs.SlabID{}, err
	}

	// insert slab into join table
	_, err = tx.Exec(ctx, `
		INSERT INTO account_slabs (account_id, slab_id) VALUES ($1, $2)
		ON CONFLICT (account_id, slab_id) DO NOTHING
	`, accountID, slabID)
	if err != nil {
		return slabs.SlabID{}, fmt.Errorf("failed to insert slab into account_slabs: %w", err)
	}

	// if the slab already existed, we don't need to insert the sectors
	if existingSlab {
		return digest, nil
	}

	// insert slab's sectors in a single batch
	batch := &pgx.Batch{}
	for i, sector := range slab.Sectors {
		batch.Queue(`INSERT INTO sectors (sector_root, host_id, slab_id, slab_index, next_integrity_check) VALUES ($1, (SELECT id FROM hosts WHERE public_key = $2), $3, $4, $5)`, sqlHash256(sector.Root), sqlPublicKey(sector.HostKey), slabID, i, nextIntegrityCheck)
	}
	if err = tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
		return slabs.SlabID{}, fmt.Errorf("failed to insert sectors: %w", err)
	}
	return digest, nil
}
