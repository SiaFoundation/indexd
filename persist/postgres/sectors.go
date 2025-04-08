package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
)

var (
	// ErrSlabExists is returned when a slab already exists in the database.
	ErrSlabExists = errors.New("slab already exists")

	// ErrSlabNotFound is returned when a slab is not found in the database.
	ErrSlabNotFound = errors.New("slab not found")
)

type (
	// SlabID is the ID of a slab derived from the slab's sectors' roots.
	SlabID types.Hash256

	// Sector is a 4MiB sector stored on a host.
	Sector struct {
		// Root is the sector root of a 4MiB sector
		Root types.Hash256 `json:"root"`

		// ContractID is the ID of the contract the sector is pinned to.
		// 'nil' if the sector isn't pinned.
		ContractID *types.FileContractID `json:"contractID,omitempty"`

		// HostKey is the public key of the host that stores the sector data.
		// 'nil' if a host lost the sector and the sector requires migration
		HostKey *types.PublicKey `json:"host,omitempty"`
	}

	// Slab is a group of sectors that is encrypted, erasure-coded and uploaded
	// to hosts.
	Slab struct {
		ID            SlabID   `json:"id"`
		EncryptionKey [32]byte `json:"encryptionKey"`
		MinShards     uint     `json:"minShards"`
		Sectors       []Sector `json:"sectors"`
	}

	// SectorPinParams describes an uploaded sector to be pinned.
	SectorPinParams struct {
		Root    types.Hash256   `json:"root"`
		HostKey types.PublicKey `json:"hostKey"`
	}

	// SlabPinParams is the input to PinSlabs
	SlabPinParams struct {
		EncryptionKey [32]byte          `json:"encryptionKey"`
		MinShards     uint              `json:"minShards"`
		Sectors       []SectorPinParams `json:"sectors"`
	}
)

// String implements the Stringer interface for SlabID.
func (s SlabID) String() string {
	return types.Hash256(s).String()
}

// PinSlabs adds slabs to the database for pinning. The slabs are associated
// with the provided account.
func (s *Store) PinSlabs(ctx context.Context, account proto.Account, slabs []SlabPinParams) ([]SlabID, error) {
	if len(slabs) == 0 {
		return nil, nil
	}
	var ids []SlabID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE public_key = $1", sqlPublicKey(account)).Scan(&accountID)
		if err != nil {
			return fmt.Errorf("%w: %v", accounts.ErrNotFound, account)
		}
		for i, slab := range slabs {
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
func (s *Store) Slabs(ctx context.Context, slabIDs []SlabID) ([]Slab, error) {
	if len(slabIDs) == 0 {
		return nil, nil
	}

	slabs := make([]Slab, 0, len(slabIDs))
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		for _, slabID := range slabIDs {
			// query slab
			var sid int64
			slab := Slab{ID: slabID}
			err := tx.QueryRow(ctx, `
				SELECT id, encryption_key, min_shards
				FROM slabs
				WHERE digest = $1
			`, sqlHash256(slabID)).Scan(&sid, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: %v", ErrSlabNotFound, slabID)
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
					var sector Sector
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
			slabs = append(slabs, slab)
		}
		return nil
	})
	return slabs, err
}

func (s *Store) pinSlab(ctx context.Context, tx *txn, accountID int64, slab SlabPinParams) (SlabID, error) {
	hasher := types.NewHasher()
	for _, sector := range slab.Sectors {
		// create slab id from sector roots
		if _, err := hasher.E.Write(sector.Root[:]); err != nil {
			return SlabID{}, fmt.Errorf("failed to write sector root to hasher: %w", err)
		}
	}
	digest := SlabID(hasher.Sum())

	// check if slab already exists
	var exists bool
	err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM slabs WHERE account_id = $1 AND digest = $2)", accountID, sqlHash256(digest)).
		Scan(&exists)
	if err != nil {
		return SlabID{}, fmt.Errorf("failed to check if slab exists: %w", err)
	} else if exists {
		return SlabID{}, ErrSlabExists
	}

	// insert slab, overwriting existing ones
	var slabID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO slabs (account_id, digest, encryption_key, min_shards)
		VALUES ($1, $2, $3, $4)
		RETURNING id
		`, accountID, sqlHash256(digest), sqlHash256(slab.EncryptionKey), slab.MinShards).Scan(&slabID)
	if err != nil {
		return SlabID{}, err
	}

	// insert sectors, overwriting existing ones
	for i, sector := range slab.Sectors {
		_, err := tx.Exec(ctx, `
			INSERT INTO sectors (sector_root, host_id, slab_id, slab_index)
			VALUES ($1, (SELECT id FROM hosts WHERE public_key = $2), $3, $4)
			RETURNING id
		`, sqlHash256(sector.Root), sqlPublicKey(sector.HostKey), slabID, i)
		if err != nil {
			return SlabID{}, fmt.Errorf("failed to insert sector %d: %w", i+1, err)
		}
	}

	return digest, nil
}
