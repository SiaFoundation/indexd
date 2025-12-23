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

// SharedObject retrieves the shared object with the given key for the given account.
func (s *Store) SharedObject(key types.Hash256) (obj slabs.SharedObject, _ error) {
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		var objID int64
		err := tx.QueryRow(ctx, `SELECT id FROM objects WHERE object_key = $1
		`, sqlHash256(key)).Scan(&objID)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to query shared object: %w", err)
		}

		rows, err := tx.Query(ctx, `SELECT s.id, s.encryption_key, s.min_shards, os.slab_offset, os.slab_length
		FROM object_slabs os
		INNER JOIN slabs s ON (os.slab_digest = s.digest)
		WHERE os.object_id = $1
		ORDER BY slab_index ASC
		`, objID)
		if err != nil {
			return fmt.Errorf("failed to query slabs: %w", err)
		}
		batch := &pgx.Batch{}
		var objectSlabs []slabs.SlabSlice
		for rows.Next() {
			var slab slabs.SlabSlice
			var slabDBID int64
			err := rows.Scan(&slabDBID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.Offset, &slab.Length)
			if err != nil {
				return fmt.Errorf("failed to scan slab: %w", err)
			}
			i := len(objectSlabs)
			objectSlabs = append(objectSlabs, slab)

			batch.Queue(`SELECT s.sector_root, h.public_key FROM sectors s
INNER JOIN slab_sectors ss ON (ss.sector_id = s.id)
LEFT JOIN hosts h ON (h.id = s.host_id)
WHERE ss.slab_id = $1
ORDER BY ss.slab_index ASC`, slabDBID).Query(func(rows pgx.Rows) error {
				defer rows.Close()
				for rows.Next() {
					var sector slabs.PinnedSector
					var hostKey sql.Null[sqlPublicKey]
					err := rows.Scan((*sqlHash256)(&sector.Root), &hostKey)
					if err != nil {
						return fmt.Errorf("failed to scan sector: %w", err)
					}
					if hostKey.Valid {
						sector.HostKey = (types.PublicKey)(hostKey.V)
					}
					objectSlabs[i].Sectors = append(objectSlabs[i].Sectors, sector)
				}
				return rows.Err()
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("failed to query slab sectors: %w", err)
		}

		obj.Slabs = objectSlabs
		return nil
	})
	return obj, err
}

// Object retrieves the object with the given key for the given account.
func (s *Store) Object(account proto.Account, key types.Hash256) (obj slabs.SealedObject, _ error) {
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		var objID int64
		var metaKey sql.Null[[]byte]
		err = tx.QueryRow(ctx, `SELECT id, encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, created_at, updated_at FROM objects WHERE account_id = $1 AND object_key = $2
		`, accountID, sqlHash256(key)).Scan(&objID, &obj.EncryptedDataKey, &metaKey, &obj.EncryptedMetadata, (*sqlSignature)(&obj.DataSignature), (*sqlSignature)(&obj.MetadataSignature), &obj.CreatedAt, &obj.UpdatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to query object: %w", err)
		}
		if metaKey.Valid {
			obj.EncryptedMetadataKey = metaKey.V
		}

		rows, err := tx.Query(ctx, `
			SELECT slabs.id, slab_offset, slab_length, slabs.encryption_key, slabs.min_shards
			FROM object_slabs
			JOIN slabs ON slabs.digest = object_slabs.slab_digest
			WHERE object_id = $1
			ORDER BY slab_index ASC
		`, objID)
		if err != nil {
			return fmt.Errorf("failed to query slabs: %w", err)
		}
		defer rows.Close()

		var slabIDs []int64
		for rows.Next() {
			var slab slabs.SlabSlice
			var slabID int64
			err := rows.Scan(&slabID, &slab.Offset, &slab.Length, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards)
			if err != nil {
				return fmt.Errorf("failed to scan slab: %w", err)
			}
			obj.Slabs = append(obj.Slabs, slab)
			slabIDs = append(slabIDs, slabID)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if err := decorateSectors(ctx, tx, &obj, slabIDs); err != nil {
			return fmt.Errorf("failed to decorate sectors of slab: %w", err)
		}
		return nil
	})
	return obj, err
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (s *Store) ListObjects(account proto.Account, cursor slabs.Cursor, limit int) (events []slabs.ObjectEvent, _ error) {
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			SELECT object_key, was_deleted, updated_at
			FROM object_events
			WHERE (updated_at > $1 OR (updated_at = $1 AND object_key > $2)) AND account_id = $3
			ORDER BY updated_at ASC, object_key ASC
			LIMIT $4
		`, cursor.After, sqlHash256(cursor.Key), accountID, limit)
		if err != nil {
			return fmt.Errorf("failed to query objects: %w", err)
		}

		for rows.Next() {
			var event slabs.ObjectEvent
			err := rows.Scan((*sqlHash256)(&event.Key), &event.Deleted, &event.UpdatedAt)
			if err != nil {
				return fmt.Errorf("failed to scan object event: %w", err)
			}
			events = append(events, event)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		objectIDs := make(map[types.Hash256]int64)
		for i := range events {
			if events[i].Deleted {
				continue
			}

			var obj slabs.SealedObject
			var objID int64
			var metaKey sql.Null[[]byte]
			err := tx.QueryRow(ctx, `SELECT id, encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, created_at, updated_at
				FROM objects
				WHERE account_id = $1 AND object_key = $2`,
				accountID,
				sqlHash256(events[i].Key),
			).Scan(&objID, &obj.EncryptedDataKey, &metaKey, &obj.EncryptedMetadata, (*sqlSignature)(&obj.DataSignature), (*sqlSignature)(&obj.MetadataSignature), &obj.CreatedAt, &obj.UpdatedAt)
			if err != nil {
				return fmt.Errorf("failed to query objects: %w", err)
			}
			if metaKey.Valid {
				obj.EncryptedMetadataKey = metaKey.V
			}
			events[i].Object = &obj
			objectIDs[events[i].Key] = objID
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// populate slabs
		for i := range events {
			if events[i].Deleted {
				continue
			}

			rows, err = tx.Query(ctx, `
				SELECT slabs.id, slab_offset, slab_length, slabs.encryption_key, slabs.min_shards
				FROM object_slabs
				JOIN slabs ON slabs.digest = object_slabs.slab_digest
				WHERE object_id = $1
				ORDER BY slab_index ASC
			`, objectIDs[events[i].Key])
			if err != nil {
				return fmt.Errorf("failed to query slabs: %w", err)
			}

			var slabIDs []int64
			for rows.Next() {
				var slab slabs.SlabSlice
				var slabID int64
				err := rows.Scan(&slabID, &slab.Offset, &slab.Length, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards)
				if err != nil {
					rows.Close()
					return fmt.Errorf("failed to scan slab: %w", err)
				}
				events[i].Object.Slabs = append(events[i].Object.Slabs, slab)
				slabIDs = append(slabIDs, slabID)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			rows.Close()

			if err := decorateSectors(ctx, tx, events[i].Object, slabIDs); err != nil {
				return fmt.Errorf("failed to decorate sectors of slab: %w", err)
			}
		}
		return nil
	})
	return events, err
}

// DeleteObject deletes the object with the given key for the given account.
func (s *Store) DeleteObject(account proto.Account, objectKey types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		var objectID int64
		err = tx.QueryRow(ctx, `SELECT id FROM objects WHERE object_key = $1 AND account_id = $2`, sqlHash256(objectKey), accountID).Scan(&objectID)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
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
		_, err = tx.Exec(ctx, `
                       UPDATE object_events SET was_deleted = TRUE, updated_at = NOW()
                       WHERE account_id = $1 AND object_key = $2`,
			accountID, sqlHash256(objectKey))
		if err != nil {
			return fmt.Errorf("failed to update object events: %w", err)
		}

		return nil
	})
}

// SaveObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (s *Store) SaveObject(account proto.Account, obj slabs.SealedObject) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}

		// ensure empty slices are passed as nil
		var encryptedMetaKey []byte
		if len(obj.EncryptedMetadataKey) > 0 {
			encryptedMetaKey = obj.EncryptedMetadataKey
		}
		var encryptedMeta []byte
		if len(obj.EncryptedMetadata) > 0 {
			encryptedMeta = obj.EncryptedMetadata
		}

		var objectID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO objects (object_key, account_id, encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (account_id, object_key) DO UPDATE SET (encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, updated_at) = (EXCLUDED.encrypted_data_key, EXCLUDED.encrypted_meta_key, EXCLUDED.encrypted_metadata, EXCLUDED.data_signature, EXCLUDED.meta_signature, NOW())
			RETURNING id`,
			sqlHash256(obj.ID()), accountID, obj.EncryptedDataKey, encryptedMetaKey, encryptedMeta, sqlSignature(obj.DataSignature), sqlSignature(obj.MetadataSignature)).Scan(&objectID)
		if err != nil {
			return fmt.Errorf("failed to insert object: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO object_events (object_key, account_id, was_deleted) VALUES ($1, $2, FALSE)
			ON CONFLICT (account_id, object_key) DO UPDATE SET (was_deleted, updated_at) = (FALSE, NOW())`,
			sqlHash256(obj.ID()), accountID)
		if err != nil {
			return fmt.Errorf("failed to insert object event: %w", err)
		}

		slabIDs := make([]slabs.SlabID, 0, len(obj.Slabs))
		for _, slab := range obj.Slabs {
			slabIDs = append(slabIDs, slab.Digest())
		}

		// check that this account has pinned these slabs
		args := make([]any, 0, len(obj.Slabs))
		seen := make(map[slabs.SlabID]struct{})
		for i := range obj.Slabs {
			seen[slabIDs[i]] = struct{}{}
			args = append(args, sqlHash256(slabIDs[i]))
		}

		var count int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM slabs
JOIN account_slabs ON account_slabs.slab_id = slabs.id
WHERE account_slabs.account_id = $1
AND slabs.digest = ANY($2)`, accountID, args).Scan(&count); err != nil {
			return fmt.Errorf("failed to check how many slab IDs exist: %w", err)
		} else if len(seen) != count {
			return slabs.ErrObjectUnpinnedSlab
		}

		// delete existing slabs
		batch := &pgx.Batch{}
		batch.Queue(`DELETE FROM object_slabs WHERE object_id = $1`, objectID)

		// insert new slabs
		for i, slab := range obj.Slabs {
			batch.Queue(`
				INSERT INTO object_slabs (object_id, slab_digest, slab_index, slab_offset, slab_length) VALUES ($1, $2, $3, $4, $5)
			`,
				objectID, sqlHash256(slabIDs[i]), i, slab.Offset, slab.Length)
		}
		res := tx.SendBatch(ctx, batch)
		if err := res.Close(); err != nil {
			return fmt.Errorf("failed to insert slabs for object: %w", err)
		}
		return nil
	})
}

func accountID(ctx context.Context, tx *txn, account proto.Account) (int64, bool, error) {
	var accountID int64
	var deleted bool
	err := tx.QueryRow(ctx, "SELECT id, deleted_at IS NOT NULL FROM accounts WHERE accounts.public_key = $1", sqlPublicKey(account)).Scan(&accountID, &deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, accounts.ErrNotFound
	} else if err != nil {
		return 0, false, fmt.Errorf("failed to get account id: %w", err)
	}
	return accountID, deleted, nil
}

func decorateSectors(ctx context.Context, tx *txn, so *slabs.SealedObject, slabIDs []int64) error {
	if len(so.Slabs) != len(slabIDs) {
		return fmt.Errorf("mismatched slab count (developer error): have %d, want %d", len(so.Slabs), len(slabIDs))
	}
	batch := &pgx.Batch{}
	for i := range so.Slabs {
		batch.Queue(`
			SELECT s.sector_root, h.public_key
			FROM slabs
			JOIN slab_sectors ss ON ss.slab_id = slabs.id
			JOIN sectors s ON s.id = ss.sector_id
			LEFT JOIN hosts h ON h.id = s.host_id
			WHERE slabs.id = $1
			ORDER BY ss.slab_index ASC
		`, slabIDs[i]).Query(func(rows pgx.Rows) error {
			defer rows.Close()

			for rows.Next() {
				var sector slabs.PinnedSector
				var hostKey sql.Null[sqlPublicKey]
				err := rows.Scan((*sqlHash256)(&sector.Root), &hostKey)
				if err != nil {
					rows.Close()
					return fmt.Errorf("failed to scan slab sector: %w", err)
				}
				if hostKey.Valid {
					sector.HostKey = (types.PublicKey)(hostKey.V)
				}
				so.Slabs[i].Sectors = append(so.Slabs[i].Sectors, sector)
			}
			return rows.Err()
		})
	}
	if err := tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("failed to query slab sectors: %w", err)
	}
	return nil
}
