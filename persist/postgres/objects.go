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
	"go.sia.tech/indexd/objects"
)

// Object retrieves the object with the given key for the given account.
func (s *Store) Object(ctx context.Context, account proto.Account, key types.Hash256) (obj objects.Object, _ error) {
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		accountID, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		var objID int64
		err = tx.QueryRow(ctx, `SELECT id, object_key, meta, created_at, updated_at FROM objects WHERE account_id = $1 AND object_key = $2
		`, accountID, sqlHash256(key)).Scan(&objID, (*sqlHash256)(&obj.Key), &obj.Meta, &obj.CreatedAt, &obj.UpdatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return objects.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to query object: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT slab_digest, slab_offset, slab_length
			FROM object_slabs
			WHERE object_id = $1
			ORDER BY slab_index ASC
		`, objID)
		if err != nil {
			return fmt.Errorf("failed to query slabs: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var slab objects.SlabSlice
			err := rows.Scan((*sqlHash256)(&slab.SlabID), &slab.Offset, &slab.Length)
			if err != nil {
				return fmt.Errorf("failed to scan slab: %w", err)
			}
			obj.Slabs = append(obj.Slabs, slab)
		}
		return rows.Err()
	})
	return obj, err
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (s *Store) ListObjects(ctx context.Context, account proto.Account, cursor objects.Cursor, limit int) (objs []objects.Object, _ error) {
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		accountID, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			SELECT id, object_key, created_at, updated_at, meta
			FROM objects
			WHERE (updated_at > $1 OR (updated_at = $1 AND object_key > $2)) AND account_id = $3
			ORDER BY updated_at ASC, object_key ASC
			LIMIT $4
		`, cursor.After, sqlHash256(cursor.Key), accountID, limit)
		if err != nil {
			return fmt.Errorf("failed to query objects: %w", err)
		}

		// read objects
		var objectIDs []int64
		for rows.Next() {
			var obj objects.Object
			var objID int64
			err := rows.Scan(&objID, (*sqlHash256)(&obj.Key), &obj.CreatedAt, &obj.UpdatedAt, &obj.Meta)
			if err != nil {
				return fmt.Errorf("failed to scan object: %w", err)
			}
			objs = append(objs, obj)
			objectIDs = append(objectIDs, objID)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// populate slabs
		for i := range objs {
			rows, err = tx.Query(ctx, `
				SELECT slab_digest, slab_offset, slab_length
				FROM object_slabs
				WHERE object_id = $1
				ORDER BY slab_index ASC
			`, objectIDs[i])
			if err != nil {
				return fmt.Errorf("failed to query slabs: %w", err)
			}
			for rows.Next() {
				var slab objects.SlabSlice
				err := rows.Scan((*sqlHash256)(&slab.SlabID), &slab.Offset, &slab.Length)
				if err != nil {
					return fmt.Errorf("failed to scan slab: %w", err)
				}
				objs[i].Slabs = append(objs[i].Slabs, slab)
			}
			if err := rows.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	return objs, err
}

// DeleteObject deletes the object with the given key for the given account.
func (s *Store) DeleteObject(ctx context.Context, account proto.Account, objectKey types.Hash256) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		accountID, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}
		var objectID int64
		err = tx.QueryRow(ctx, `SELECT id FROM objects WHERE object_key = $1 AND account_id = $2`, sqlHash256(objectKey), accountID).
			Scan(&objectID)
		if errors.Is(err, sql.ErrNoRows) {
			return objects.ErrObjectNotFound
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

// SaveObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (s *Store) SaveObject(ctx context.Context, account proto.Account, obj objects.Object) error {
	if len(obj.Slabs) == 0 {
		return errors.New("object must have at least one slab")
	}
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		accountID, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		var objectID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO objects (object_key, account_id, meta) VALUES ($1, $2, $3)
			ON CONFLICT (account_id, object_key) DO UPDATE SET meta = EXCLUDED.meta, updated_at = NOW()
			RETURNING id`,
			sqlHash256(obj.Key), accountID, obj.Meta).Scan(&objectID)
		if err != nil {
			return fmt.Errorf("failed to insert object: %w", err)
		}

		// TODO: what about objects linking slabs that aren't pinned? Pin them here?

		// delete existing slabs
		batch := &pgx.Batch{}
		batch.Queue(`DELETE FROM object_slabs WHERE object_id = $1`, objectID)

		// insert new slabs
		for i, slab := range obj.Slabs {
			batch.Queue(`
				INSERT INTO object_slabs (object_id, slab_digest, slab_index, slab_offset, slab_length) VALUES ($1, $2, $3, $4, $5)
			`,
				objectID, sqlHash256(slab.SlabID), i, slab.Offset, slab.Length)
		}
		res := tx.SendBatch(ctx, batch)
		if err := res.Close(); err != nil {
			return fmt.Errorf("failed to insert slabs for object: %w", err)
		}
		return nil
	})
}

func accountID(ctx context.Context, tx *txn, account proto.Account) (int64, error) {
	var accountID int64
	err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE accounts.public_key = $1", sqlPublicKey(account)).Scan(&accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, accounts.ErrNotFound
	} else if err != nil {
		return 0, fmt.Errorf("failed to get account id: %w", err)
	}
	return accountID, nil
}
