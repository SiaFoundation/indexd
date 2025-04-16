package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
)

// PinSlabs adds slabs to the database for pinning. The slabs are associated
// with the provided account.
func (s *Store) PinSlabs(ctx context.Context, account proto.Account, toPin []slabs.SlabPinParams) ([]slabs.SlabID, error) {
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
			slabID, err := s.pinSlab(ctx, tx, accountID, slab)
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

func (s *Store) pinSlab(ctx context.Context, tx *txn, accountID int64, slab slabs.SlabPinParams) (slabs.SlabID, error) {
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
		// slab already exists, fetch it's slab id
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
		batch.Queue(`INSERT INTO sectors (sector_root, host_id, slab_id, slab_index) VALUES ($1, (SELECT id FROM hosts WHERE public_key = $2), $3, $4)`, sqlHash256(sector.Root), sqlPublicKey(sector.HostKey), slabID, i)
	}
	if err = tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
		return slabs.SlabID{}, fmt.Errorf("failed to insert sectors: %w", err)
	}
	return digest, nil
}
