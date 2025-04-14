package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

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
			SELECT sector_root
			FROM sectors
			INNER JOIN hosts ON hosts.id = sectors.host_id
			WHERE hosts.public_key = $1 AND next_integrity_check <= NOW()
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
				INNER JOIN accounts a ON a.id = slabs.account_id
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

func (s *Store) pinSlab(ctx context.Context, tx *txn, accountID int64, nextIntegrityCheck time.Time, slab slabs.SlabPinParams) (slabs.SlabID, error) {
	hasher := types.NewHasher()
	for _, sector := range slab.Sectors {
		// create slab id from sector roots
		if _, err := hasher.E.Write(sector.Root[:]); err != nil {
			return slabs.SlabID{}, fmt.Errorf("failed to write sector root to hasher: %w", err)
		}
	}
	digest := slabs.SlabID(hasher.Sum())

	// check if slab already exists
	var exists bool
	err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM slabs WHERE account_id = $1 AND digest = $2)", accountID, sqlHash256(digest)).
		Scan(&exists)
	if err != nil {
		return slabs.SlabID{}, fmt.Errorf("failed to check if slab exists: %w", err)
	} else if exists {
		return slabs.SlabID{}, slabs.ErrSlabExists
	}

	// insert slab
	var slabID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO slabs (account_id, digest, encryption_key, min_shards)
		VALUES ($1, $2, $3, $4)
		RETURNING id
		`, accountID, sqlHash256(digest), sqlHash256(slab.EncryptionKey), slab.MinShards).Scan(&slabID)
	if err != nil {
		return slabs.SlabID{}, err
	}

	// insert slab's sectors in a single batch
	var placeholders []string
	var args []any
	for i, sector := range slab.Sectors {
		placeholders = append(placeholders, fmt.Sprintf("($%d, (SELECT id FROM hosts WHERE public_key = $%d), $%d, $%d, $%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
		args = append(args, sqlHash256(sector.Root), sqlPublicKey(sector.HostKey), slabID, i, nextIntegrityCheck)
	}
	values := strings.Join(placeholders, ",")

	_, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO sectors (sector_root, host_id, slab_id, slab_index, next_integrity_check)
		VALUES %s
	`, values), args...)
	if err != nil {
		return slabs.SlabID{}, fmt.Errorf("failed to insert sectors: %w", err)
	}

	return digest, nil
}
