package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
)

type (
	Object struct {
		Key       types.Hash256
		Slabs     []SlabSlice
		Meta      []byte
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	SlabSlice struct {
		SlabID types.Hash256
		Offset uint32
		Length uint32
	}
)

var (
	ErrObjectNotFound = errors.New("object not found")
)

func (s *Store) ListObjects(ctx context.Context, account proto.Account, after time.Time, limit int64) ([]Object, error) {
	var objects []Object
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE accounts.public_key = $1", sqlPublicKey(account)).Scan(&accountID)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			SELECT id, object_key, created_at, updated_at, meta
			FROM objects
			WHERE updated_at > $1 AND account_id = $2
			ORDER BY updated_at ASC, id ASC
			LIMIT $3
		`, after, accountID, limit)
		if err != nil {
			return fmt.Errorf("failed to query objects: %w", err)
		}

		// read objects
		var objectIDs []int64
		for rows.Next() {
			var obj Object
			var objID int64
			err := rows.Scan(&objID, (*sqlHash256)(&obj.Key), &obj.CreatedAt, &obj.UpdatedAt, &obj.Meta)
			if err != nil {
				return fmt.Errorf("failed to scan object: %w", err)
			}
			objects = append(objects, obj)
			objectIDs = append(objectIDs, objID)
		}
		if rows.Err() != nil {
			return err
		}

		// populate slabs
		for i := range objects {
			rows, err = tx.Query(ctx, `
				SELECT slab_digest, offset, length
				FROM object_slabs
				WHERE object_id = $1
				ORDER BY slab_index ASC
			`, objectIDs[i])
			if err != nil {
				return fmt.Errorf("failed to query slabs: %w", err)
			}
			for rows.Next() {
				var slab SlabSlice
				err := rows.Scan((*sqlHash256)(&slab.SlabID), &slab.Offset, &slab.Length)
				if err != nil {
					return fmt.Errorf("failed to scan slab: %w", err)
				}
				objects[i].Slabs = append(objects[i].Slabs, slab)
			}
			if err := rows.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	return nil, err
}

func (s *Store) DeleteObject(ctx context.Context, account proto.Account, objectKey types.Hash256) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var objectID int64
		err := tx.QueryRow(ctx, `SELECT objects.id FROM objects WHERE objects.object_key = $1)`, sqlHash256(objectKey)).
			Scan(&objectID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get object id: %w", err)
		}
		_, err = tx.Exec(ctx, `DELETE FROM object_slabs WHERE object_id = $1`, objectID)
		if err != nil {
			return fmt.Errorf("failed to delete object slabs: %w", err)
		}
		_, err = tx.Exec(ctx, `DELETE FROM objects WHERE id = $1`, objectID)
		if err != nil {
			return fmt.Errorf("failed to delete object: %w", err)
		}
		return nil
	})
}

func (s *Store) SaveObject(ctx context.Context, account proto.Account, obj Object) error {
	if len(obj.Slabs) == 0 {
		return errors.New("object must have at least one slab")
	}
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE accounts.public_key = $1", sqlPublicKey(account)).Scan(&accountID)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get account id: %w", err)
		}

		var objectID int64
		err = tx.QueryRow(ctx, `INSERT INTO objects (object_key, account_id, meta) VALUES ($1, $2, $3) RETURNING id`,
			sqlHash256(obj.Key), accountID, obj.Meta).Scan(&objectID)
		if err != nil {
			return fmt.Errorf("failed to insert object: %w", err)
		}

		// TODO: what about objects linking slabs that aren't pinned? Pin them here?

		for i, slab := range obj.Slabs {
			_, err := tx.Exec(ctx, `INSERT INTO object_slabs (object_id, slab_digest, slab_index, offset, length) VALUES ($1, $2, $3, $4, $5)`,
				objectID, sqlHash256(slab.SlabID), i, slab.Offset, slab.Length)
			if err != nil {
				return fmt.Errorf("failed to insert slab %d for object: %w", i, err)
			}
		}
		return nil
	})
}
