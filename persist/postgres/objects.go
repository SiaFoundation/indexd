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
	"go.uber.org/zap"
)

const sqlObjectsByKey = `
	SELECT id, object_key, encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, created_at, updated_at
	FROM objects
	WHERE account_id = $1 AND object_key = ANY($2::bytea[])`

// SharedObject retrieves the shared object with the given key for the given account.
func (s *Store) SharedObject(key types.Hash256) (obj slabs.SharedObject, _ error) {
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		if err := assertObjectNotBlocked(ctx, tx, key); err != nil {
			return err
		}

		var objID int64
		err := tx.QueryRow(ctx, `SELECT id FROM objects WHERE object_key = $1
		`, sqlHash256(key)).Scan(&objID)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to query shared object: %w", err)
		}

		rows, err := tx.Query(ctx, `SELECT s.id, s.encryption_key, s.min_shards, os.slab_offset, os.slab_length, s.version
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
			err := rows.Scan(&slabDBID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.Offset, &slab.Length, &slab.Version)
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
		obj = slabs.SealedObject{} // reset if transaction retries

		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		if err := assertObjectNotBlocked(ctx, tx, key); err != nil {
			return err
		}

		rows, err := tx.Query(ctx, sqlObjectsByKey, accountID, []sqlHash256{sqlHash256(key)})
		if err != nil {
			return fmt.Errorf("failed to query object: %w", err)
		}
		objID, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int64, error) {
			id, _, o, err := scanObject(row)
			obj = o
			return id, err
		})
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to query object: %w", err)
		}

		return loadObjectSlabs(ctx, tx, map[int64]*slabs.SealedObject{objID: &obj})
	})
	return obj, err
}

// ListObjects lists object events for the given account that were published
// after the given cursor.
func (s *Store) ListObjects(account proto.Account, cursor slabs.Cursor, limit int) (events []slabs.ObjectEvent, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			SELECT object_key, was_deleted, published_at
			FROM object_events oe
			WHERE oe.account_id = $1 AND (oe.published_at, oe.object_key) > ($2, $3)
			  AND NOT EXISTS (SELECT 1 FROM blocked_objects b WHERE b.object_key = oe.object_key)
			ORDER BY oe.published_at ASC, oe.object_key ASC
			LIMIT $4
		`, accountID, cursor.After, sqlHash256(cursor.Key), limit)
		if err != nil {
			return fmt.Errorf("failed to query object events: %w", err)
		}
		events, err = pgx.AppendRows(events[:0], rows, scanObjectEvent)
		if err != nil {
			return fmt.Errorf("failed to scan object events: %w", err)
		}

		objectKeys := make([]sqlHash256, 0, len(events))
		eventByKey := make(map[types.Hash256]int, len(events))
		for i := range events {
			if events[i].Deleted {
				continue
			}
			objectKeys = append(objectKeys, sqlHash256(events[i].Key))
			eventByKey[events[i].Key] = i
		}
		if len(objectKeys) == 0 {
			return nil
		}

		rows, err = tx.Query(ctx, sqlObjectsByKey, accountID, objectKeys)
		if err != nil {
			return fmt.Errorf("failed to query objects: %w", err)
		}
		objectsByID := make(map[int64]*slabs.SealedObject, len(objectKeys))
		err = forEachRow(rows, func(row pgx.CollectableRow) error {
			id, key, obj, err := scanObject(row)
			if err != nil {
				return err
			}
			eventIndex, ok := eventByKey[key]
			if !ok {
				return fmt.Errorf("queried object not present in event page (developer error): %v", key)
			}
			events[eventIndex].Object = &obj
			objectsByID[id] = &obj
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to scan objects: %w", err)
		} else if len(objectsByID) != len(objectKeys) {
			return fmt.Errorf("failed to query objects: expected %d objects, got %d", len(objectKeys), len(objectsByID))
		}
		return loadObjectSlabs(ctx, tx, objectsByID)
	})
	return
}

// DeleteObject deletes the object with the given key for the given account.
// Slabs that were referenced by the object and are no longer referenced by any
// of the account's objects are unpinned and queued for deletion by
// PruneDeletedSlabs.
func (s *Store) DeleteObject(account proto.Account, objectKey types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		var objectID int64
		err = tx.QueryRow(ctx, `SELECT id FROM objects WHERE object_key = $1 AND account_id = $2 FOR UPDATE`, sqlHash256(objectKey), accountID).Scan(&objectID)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get object id: %w", err)
		}

		// delete the object's slab references, remembering the slabs so the
		// now unreferenced ones can be unpinned after the object is deleted
		rows, err := tx.Query(ctx, `WITH deleted AS (
	DELETE FROM object_slabs WHERE object_id = $1 RETURNING slab_digest
)
SELECT DISTINCT s.id FROM slabs s
INNER JOIN deleted d ON (d.slab_digest = s.digest)`, objectID)
		if err != nil {
			return fmt.Errorf("failed to delete object slabs: %w", err)
		}
		objectSlabIDs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("failed to collect object slab ids: %w", err)
		}

		_, err = tx.Exec(ctx, `DELETE FROM objects WHERE id = $1`, objectID)
		if err != nil {
			return fmt.Errorf("failed to delete object: %w", err)
		}
		_, err = tx.Exec(ctx, `
                       UPDATE object_events SET was_deleted = TRUE, published_at = NULL
                       WHERE account_id = $1 AND object_key = $2`,
			accountID, sqlHash256(objectKey))
		if err != nil {
			return fmt.Errorf("failed to update object events: %w", err)
		}

		// unpin the object's slabs that are no longer referenced by any of
		// the account's objects; the locking inside serializes concurrent
		// deletions of objects sharing a slab, which could otherwise each
		// still see the other's object and leave the shared slab pinned
		// forever
		return s.unpinUnreferencedSlabs(ctx, tx, accountID, objectSlabIDs, nil)
	})
}

// ObjectsForSlab returns all (account, object key) pairs for objects that
// reference the given slab.
func (s *Store) ObjectsForSlab(slabID slabs.SlabID) (objects []slabs.SlabObject, _ error) {
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		objects = objects[:0] // reuse same slice if transaction retries

		rows, err := tx.Query(ctx, `
			SELECT DISTINCT a.public_key, o.object_key
			FROM objects o
			JOIN object_slabs os ON o.id = os.object_id
			JOIN accounts a ON o.account_id = a.id
			WHERE os.slab_digest = $1`, sqlHash256(slabID))
		if err != nil {
			return fmt.Errorf("failed to query objects for slab: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var obj slabs.SlabObject
			if err := rows.Scan((*sqlPublicKey)(&obj.Account), (*sqlHash256)(&obj.ObjectID)); err != nil {
				return fmt.Errorf("failed to scan object for slab: %w", err)
			}
			objects = append(objects, obj)
		}
		return rows.Err()
	})
	return objects, err
}

// PinObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (s *Store) PinObject(account proto.Account, obj slabs.PinObjectRequest) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}

		if err := assertObjectNotBlocked(ctx, tx, obj.ID); err != nil {
			return err
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
			sqlHash256(obj.ID), accountID, obj.EncryptedDataKey, encryptedMetaKey, encryptedMeta, sqlSignature(obj.DataSignature), sqlSignature(obj.MetadataSignature)).Scan(&objectID)
		if err != nil {
			return fmt.Errorf("failed to insert object: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO object_events (object_key, account_id, was_deleted) VALUES ($1, $2, FALSE)
			ON CONFLICT (account_id, object_key) DO UPDATE SET (was_deleted, published_at) = (FALSE, NULL)`,
			sqlHash256(obj.ID), accountID)
		if err != nil {
			return fmt.Errorf("failed to insert object event: %w", err)
		}

		slabIDs := make([]slabs.SlabID, 0, len(obj.Slabs))
		for _, slab := range obj.Slabs {
			slabIDs = append(slabIDs, slab.ID)
		}

		// check that this account has pinned these slabs
		args := make([]any, 0, len(obj.Slabs))
		seen := make(map[slabs.SlabID]struct{})
		for i := range obj.Slabs {
			seen[slabIDs[i]] = struct{}{}
			args = append(args, sqlHash256(slabIDs[i]))
		}

		// lock the slabs so a concurrent prune of the deletion queue can't
		// delete them between the pin check and the object_slabs insert
		// below. KEY SHARE is what the FK insert takes anyway; it conflicts
		// with the FOR UPDATE held by deleters but not with pinned_at
		// refreshes. Locks are acquired in digest order like everywhere else.
		if _, err := tx.Exec(ctx, `SELECT digest FROM slabs WHERE digest = ANY($1) ORDER BY digest FOR KEY SHARE`, args); err != nil {
			return fmt.Errorf("failed to lock slabs: %w", err)
		}

		// check that this account has pinned these slabs. The check runs as
		// a separate statement after the lock so its snapshot includes pins
		// removed by a concurrent unpin we may have waited on; a single
		// combined statement would count rows from before the wait.
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

// objectEventPublishBatchSize bounds how many events take a position in one
// publish so a backlog cannot hold the settings row for a full table scan.
const objectEventPublishBatchSize = 5000

// PublishObjectEvents assigns stream positions to object events that do not
// have one yet. At most one batch of events is published per wall clock second.
func (s *Store) PublishObjectEvents() error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		// the row lock serializes publishers, so a batch's second is unique
		// and every event in it sorts after all existing cursors
		var published time.Time
		err := tx.QueryRow(ctx, `
			UPDATE global_settings
			SET object_events_last_published = date_trunc('second', NOW())
			WHERE date_trunc('second', NOW()) > object_events_last_published
			RETURNING object_events_last_published`).Scan(&published)
		if errors.Is(err, sql.ErrNoRows) {
			// the guard also fails when the last published second is ahead of
			// the database clock, which stalls publishing until it catches up
			var lastPublished string
			var stalled bool
			if err := tx.QueryRow(ctx, `
				SELECT object_events_last_published::TEXT, object_events_last_published > date_trunc('second', NOW())
				FROM global_settings`).Scan(&lastPublished, &stalled); err != nil {
				return fmt.Errorf("failed to get publish state: %w", err)
			} else if stalled {
				s.log.Warn("object events are not being published, the last published second is ahead of the database clock", zap.String("lastPublished", lastPublished))
			}
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to advance publish state: %w", err)
		}

		// events locked by an in-flight writer are skipped here and take a
		// position in a later batch
		_, err = tx.Exec(ctx, `
			UPDATE object_events SET published_at = $1
			WHERE (account_id, object_key) IN (
				SELECT account_id, object_key FROM object_events
				WHERE published_at IS NULL
				ORDER BY account_id, object_key
				FOR UPDATE SKIP LOCKED
				LIMIT $2)`, published, objectEventPublishBatchSize)
		if err != nil {
			return fmt.Errorf("failed to publish object events: %w", err)
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

func scanObject(row pgx.CollectableRow) (id int64, key types.Hash256, obj slabs.SealedObject, err error) {
	var metaKey sql.Null[[]byte]
	if err = row.Scan(&id, (*sqlHash256)(&key), &obj.EncryptedDataKey, &metaKey, &obj.EncryptedMetadata, (*sqlSignature)(&obj.DataSignature), (*sqlSignature)(&obj.MetadataSignature), &obj.CreatedAt, &obj.UpdatedAt); err != nil {
		return
	}
	if metaKey.Valid {
		obj.EncryptedMetadataKey = metaKey.V
	}
	return
}

func scanObjectEvent(row pgx.CollectableRow) (event slabs.ObjectEvent, _ error) {
	err := row.Scan((*sqlHash256)(&event.Key), &event.Deleted, &event.UpdatedAt)
	return event, err
}

func scanObjectSlab(row pgx.CollectableRow) (objectID, slabID int64, slab slabs.SlabSlice, err error) {
	err = row.Scan(&objectID, &slabID, &slab.Offset, &slab.Length, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.Version)
	return
}

func scanSlabSector(row pgx.CollectableRow) (slabID int64, sector slabs.PinnedSector, err error) {
	var hostKey sql.Null[sqlPublicKey]
	if err = row.Scan(&slabID, (*sqlHash256)(&sector.Root), &hostKey); err != nil {
		return
	}
	if hostKey.Valid {
		sector.HostKey = types.PublicKey(hostKey.V)
	}
	return
}

// forEachRow calls `scan` for every row
//
// rows is automatically closed before returning
func forEachRow(rows pgx.Rows, scan func(pgx.CollectableRow) error) error {
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// loadObjectSlabs loads each object's slab slices in order and decorates them
// with their sectors.
func loadObjectSlabs(ctx context.Context, tx *txn, objects map[int64]*slabs.SealedObject) error {
	if len(objects) == 0 {
		return nil
	}

	objectIDs := make([]int64, 0, len(objects))
	for objectID := range objects {
		objectIDs = append(objectIDs, objectID)
	}

	rows, err := tx.Query(ctx, `
		SELECT object_slabs.object_id, slabs.id, slab_offset, slab_length, slabs.encryption_key, slabs.min_shards, slabs.version
		FROM object_slabs
		JOIN slabs ON slabs.digest = object_slabs.slab_digest
		WHERE object_slabs.object_id = ANY($1)
		ORDER BY object_slabs.object_id, slab_index ASC
	`, objectIDs)
	if err != nil {
		return fmt.Errorf("failed to query slabs: %w", err)
	}

	type slabLocation struct {
		objectID int64
		slab     int
	}
	slabLocations := make(map[int64][]slabLocation)
	var slabIDs []int64
	err = forEachRow(rows, func(row pgx.CollectableRow) error {
		objectID, slabID, slab, err := scanObjectSlab(row)
		if err != nil {
			return err
		}
		obj, ok := objects[objectID]
		if !ok {
			return fmt.Errorf("queried slab for unknown object (developer error): %d", objectID)
		}
		if len(slabLocations[slabID]) == 0 {
			slabIDs = append(slabIDs, slabID)
		}
		slabLocations[slabID] = append(slabLocations[slabID], slabLocation{objectID, len(obj.Slabs)})
		obj.Slabs = append(obj.Slabs, slab)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to scan slabs: %w", err)
	} else if len(slabIDs) == 0 {
		return nil
	}

	rows, err = tx.Query(ctx, `
		SELECT ss.slab_id, s.sector_root, h.public_key
		FROM slab_sectors ss
		JOIN sectors s ON s.id = ss.sector_id
		LEFT JOIN hosts h ON h.id = s.host_id
		WHERE ss.slab_id = ANY($1)
		ORDER BY ss.slab_id, ss.slab_index ASC
	`, slabIDs)
	if err != nil {
		return fmt.Errorf("failed to query slab sectors: %w", err)
	}
	err = forEachRow(rows, func(row pgx.CollectableRow) error {
		slabID, sector, err := scanSlabSector(row)
		if err != nil {
			return err
		}
		for _, loc := range slabLocations[slabID] {
			objects[loc.objectID].Slabs[loc.slab].Sectors = append(objects[loc.objectID].Slabs[loc.slab].Sectors, sector)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to scan slab sectors: %w", err)
	}
	return nil
}
