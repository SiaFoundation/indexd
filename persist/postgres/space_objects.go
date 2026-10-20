package postgres

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
)

// PinSpaceObject adds or replaces an object in a space, charging storage to the
// owner. Account must have write permission, and the object's slabs must already
// be pinned by account or to this space. An existing object's encrypted data key cannot change.
func (s *Store) PinSpaceObject(account proto.Account, key types.Hash256, obj slabs.PinObjectRequest) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Write, false)
		if err != nil {
			return err
		} else if err := assertObjectNotBlocked(ctx, tx, obj.ID); err != nil {
			return err
		}
		digests := make([]sqlHash256, 0, len(obj.Slabs))
		unique := make(map[slabs.SlabID]struct{})
		for _, slab := range obj.Slabs {
			digests = append(digests, sqlHash256(slab.ID))
			unique[slab.ID] = struct{}{}
		}
		if _, err := tx.Exec(ctx, `SELECT id FROM slabs WHERE digest = ANY($1) ORDER BY digest FOR UPDATE`, digests); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM slabs s WHERE digest = ANY($1)
   AND (EXISTS (SELECT 1 FROM space_slabs p WHERE p.slab_id = s.id AND p.space_id = $2)
   OR EXISTS (SELECT 1 FROM account_slabs a WHERE a.slab_id = s.id AND a.account_id = $3))`, digests, id, callerID).Scan(&count); err != nil {
			return err
		} else if count != len(unique) {
			return slabs.ErrObjectUnpinnedSlab
		} else if err := pinSpaceObjectSlabs(ctx, tx, id, ownerID, digests); err != nil {
			return err
		}
		var metaKey []byte
		if len(obj.EncryptedMetadataKey) > 0 {
			metaKey = obj.EncryptedMetadataKey
		}
		var objectID int64
		err = tx.QueryRow(ctx, `INSERT INTO space_objects (space_id, object_key, account_key, encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, size)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
   ON CONFLICT (space_id, object_key) DO UPDATE SET account_key = EXCLUDED.account_key,
   encrypted_meta_key = EXCLUDED.encrypted_meta_key,
   encrypted_metadata = EXCLUDED.encrypted_metadata, data_signature = EXCLUDED.data_signature,
   meta_signature = EXCLUDED.meta_signature, size = EXCLUDED.size, updated_at = NOW()
   WHERE space_objects.encrypted_data_key = EXCLUDED.encrypted_data_key RETURNING id`,
			id, sqlHash256(obj.ID), sqlPublicKey(account), obj.EncryptedDataKey, metaKey, obj.EncryptedMetadata, sqlSignature(obj.DataSignature), sqlSignature(obj.MetadataSignature), obj.Size()).Scan(&objectID)
		if errors.Is(err, pgx.ErrNoRows) {
			return spaces.ErrObjectKeyMismatch
		} else if err != nil {
			return err
		}
		oldSlabs, err := spaceSlabIDs(ctx, tx, id, &objectID)
		if err != nil {
			return err
		}
		batch := &pgx.Batch{}
		batch.Queue(`DELETE FROM space_object_slabs WHERE object_id = $1`, objectID)
		for i, slab := range obj.Slabs {
			batch.Queue(`INSERT INTO space_object_slabs (object_id, slab_digest, slab_index, slab_offset, slab_length) VALUES ($1, $2, $3, $4, $5)`, objectID, sqlHash256(slab.ID), i, slab.Offset, slab.Length)
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO space_object_events (space_id, object_key, was_deleted)
 VALUES ($1, $2, FALSE) ON CONFLICT (space_id, object_key) DO UPDATE SET was_deleted = FALSE, updated_at = NULL`, id, sqlHash256(obj.ID)); err != nil {
			return err
		}
		return s.unpinSpaceSlabs(ctx, tx, id, ownerID, oldSlabs)
	})
}

// DeleteSpaceObject removes an object from a space and releases storage no longer
// used by the owner's objects. Account must have delete permission.
func (s *Store) DeleteSpaceObject(account proto.Account, key, objectKey types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, _, err := spaceAccess(ctx, tx, account, key, spaces.Delete, false)
		if err != nil {
			return err
		}
		var objectID int64
		err = tx.QueryRow(ctx, `SELECT id FROM space_objects WHERE space_id = $1 AND object_key = $2`, id, sqlHash256(objectKey)).Scan(&objectID)
		if errors.Is(err, pgx.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return err
		}
		slabIDs, err := spaceSlabIDs(ctx, tx, id, &objectID)
		if err != nil {
			return err
		} else if _, err := tx.Exec(ctx, `DELETE FROM space_objects WHERE id = $1`, objectID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE space_object_events SET was_deleted = TRUE, updated_at = NULL
 WHERE space_id = $1 AND object_key = $2`, id, sqlHash256(objectKey)); err != nil {
			return err
		}
		return s.unpinSpaceSlabs(ctx, tx, id, ownerID, slabIDs)
	})
}

// DeleteSpace removes a space, its objects, memberships, and invitations.
// Only the owner may delete it.
func (s *Store) DeleteSpace(account proto.Account, key types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, _, err := spaceAccess(ctx, tx, account, key, spaces.Owner, false)
		if err != nil {
			return err
		}
		slabIDs, err := spaceSlabIDs(ctx, tx, id, nil)
		if err != nil {
			return err
		} else if _, err := tx.Exec(ctx, `DELETE FROM spaces WHERE id = $1`, id); err != nil {
			return err
		}
		return s.unpinUnreferencedSlabs(ctx, tx, ownerID, slabIDs, nil)
	})
}

// ListSpaceObjects returns published object changes after cursor. Read access is required.
func (s *Store) ListSpaceObjects(account proto.Account, key types.Hash256, cursor slabs.Cursor, limit int) (events []spaces.ObjectEvent, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		}
		page, ids, err := listSpaceObjectEvents(ctx, tx, id, cursor, limit)
		if err != nil {
			return err
		}
		events = make([]spaces.ObjectEvent, len(page))
		objectSlabs := make(map[int64]*[]slabs.SlabSlice, len(ids))
		for i, event := range page {
			events[i] = spaces.ObjectEvent{Key: event.Key, Deleted: event.Deleted, UpdatedAt: event.UpdatedAt}
			if event.Object != nil {
				events[i].Object = &spaces.Object{Key: event.Key, Account: event.Object.Account, SealedObject: *event.Object.WithSlabs(nil)}
			}
		}
		for id, i := range ids {
			objectSlabs[id] = &events[i].Object.Slabs
		}
		if err := loadSpaceObjectSlabs(ctx, tx, objectSlabs); err != nil {
			return err
		}
		for i := range events {
			if events[i].Object != nil && len(events[i].Object.Slabs) == 0 {
				events[i].Deleted, events[i].Object = true, nil
			}
		}
		return nil
	})
	return
}

// ListSpaceObjectsWithoutSlabs returns published object changes without slab slices. Read access is required.
func (s *Store) ListSpaceObjectsWithoutSlabs(account proto.Account, key types.Hash256, cursor slabs.Cursor, limit int) (events []spaces.ObjectEventWithoutSlabs, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		}
		events, _, err = listSpaceObjectEvents(ctx, tx, id, cursor, limit)
		return err
	})
	return
}

// SpaceObjectSlabs returns slab slices starting at cursor. Read access is required.
func (s *Store) SpaceObjectSlabs(account proto.Account, key, objectKey types.Hash256, cursor int64, limit int) (result []slabs.SlabSlice, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		}
		if err := assertObjectNotBlocked(ctx, tx, objectKey); err != nil {
			return err
		}
		var objectID int64
		err = tx.QueryRow(ctx, `SELECT id FROM space_objects WHERE space_id = $1 AND object_key = $2`, id, sqlHash256(objectKey)).Scan(&objectID)
		if errors.Is(err, pgx.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT os.object_id, s.id, os.slab_offset, os.slab_length, s.encryption_key, s.min_shards, s.version
 FROM space_object_slabs os JOIN slabs s ON s.digest = os.slab_digest
 WHERE os.object_id = $1 AND os.slab_index >= $2 ORDER BY os.slab_index LIMIT $3`, objectID, cursor, limit)
		if err != nil {
			return err
		}
		result = []slabs.SlabSlice{}
		return collectObjectSlabs(ctx, tx, rows, map[int64]*[]slabs.SlabSlice{objectID: &result})
	})
	return
}

// SpaceObject returns an object in a space. Account must have read permission,
// and the object must not be blocked.
func (s *Store) SpaceObject(account proto.Account, key, objectKey types.Hash256) (object spaces.Object, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		} else if err := assertObjectNotBlocked(ctx, tx, objectKey); err != nil {
			return err
		}
		objects, err := spaceObjects(ctx, tx, id, &objectKey, 0, 1)
		if err != nil {
			return err
		} else if len(objects) == 0 {
			return slabs.ErrObjectNotFound
		}
		object = objects[0]
		return nil
	})
	return
}

// PinSpaceSlabs pins slabs against the space owner's quota and returns their IDs.
// Account must have write permission.
func (s *Store) PinSpaceSlabs(account proto.Account, key types.Hash256, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) (digests []slabs.SlabID, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, _, err := spaceAccess(ctx, tx, account, key, spaces.Write, false)
		if err != nil {
			return err
		}
		var owner proto.Account
		if err := tx.QueryRow(ctx, `SELECT public_key FROM accounts WHERE id = $1`, ownerID).Scan((*sqlPublicKey)(&owner)); err != nil {
			return err
		}
		digests, err = s.pinSlabs(ctx, tx, owner, nextIntegrityCheck, toPin...)
		if err != nil {
			return err
		}
		keys := make([]sqlHash256, len(digests))
		for i, digest := range digests {
			keys[i] = sqlHash256(digest)
		}
		_, err = tx.Exec(ctx, `INSERT INTO space_slabs (space_id, account_id, slab_id)
 SELECT $1, $2, id FROM slabs WHERE digest = ANY($3) ON CONFLICT DO NOTHING`, id, ownerID, keys)
		return err
	})
	return
}

func (s *Store) unpinSpaceSlabs(ctx context.Context, tx *txn, spaceID, ownerID int64, slabIDs []int64) error {
	if _, err := tx.Exec(ctx, `DELETE FROM space_slabs p USING slabs s
 WHERE p.space_id = $1 AND p.slab_id = s.id AND p.slab_id = ANY($2)
 AND NOT EXISTS (SELECT 1 FROM space_objects o JOIN space_object_slabs os ON os.object_id = o.id
 WHERE o.space_id = p.space_id AND os.slab_digest = s.digest)`, spaceID, slabIDs); err != nil {
		return err
	}
	return s.unpinUnreferencedSlabs(ctx, tx, ownerID, slabIDs, nil)
}

func pinSpaceObjectSlabs(ctx context.Context, tx *txn, spaceID, ownerID int64, digests []sqlHash256) error {
	var data, size uint64
	err := tx.QueryRow(ctx, `WITH pinned AS (
  INSERT INTO account_slabs (account_id, slab_id) SELECT $1, id FROM slabs WHERE digest = ANY($2)
  ON CONFLICT DO NOTHING RETURNING slab_id
 ) SELECT
  (SELECT COALESCE(SUM(min_shards::bigint), 0) * $3 FROM slabs WHERE id IN (SELECT slab_id FROM pinned)),
  (SELECT COUNT(*) * $3 FROM slab_sectors WHERE slab_id IN (SELECT slab_id FROM pinned))`, ownerID, digests, proto.SectorSize).Scan(&data, &size)
	if err != nil {
		return err
	}
	var connectID int64
	var pinned, limit uint64
	err = tx.QueryRow(ctx, `UPDATE accounts SET pinned_data = pinned_data + $2, pinned_size = pinned_size + $3
  WHERE id = $1 AND deleted_at IS NULL RETURNING connect_key_id, pinned_data, max_pinned_data`, ownerID, data, size).Scan(&connectID, &pinned, &limit)
	if errors.Is(err, pgx.ErrNoRows) {
		return accounts.ErrNotFound
	} else if err != nil {
		return err
	} else if pinned > limit {
		return accounts.ErrAccountStorageLimitExceeded
	}
	err = tx.QueryRow(ctx, `UPDATE app_connect_keys a SET pinned_data = pinned_data + $2, pinned_size = pinned_size + $3
  FROM quotas q WHERE a.id = $1 AND q.name = a.quota_name RETURNING a.pinned_data, q.max_pinned_data`, connectID, data, size).Scan(&pinned, &limit)
	if err != nil {
		return err
	} else if pinned > limit {
		return accounts.ErrAppKeyStorageLimitExceeded
	}
	_, err = tx.Exec(ctx, `INSERT INTO space_slabs (space_id, account_id, slab_id)
 SELECT $1, $2, id FROM slabs WHERE digest = ANY($3) ON CONFLICT DO NOTHING`, spaceID, ownerID, digests)
	return err
}

func spaceSlabIDs(ctx context.Context, tx *txn, spaceID int64, objectID *int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `SELECT p.slab_id FROM space_slabs p JOIN slabs s ON s.id = p.slab_id
 WHERE p.space_id = $1 AND ($2::bigint IS NULL OR EXISTS (
 SELECT 1 FROM space_object_slabs os WHERE os.object_id = $2 AND os.slab_digest = s.digest))`, spaceID, objectID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func spaceObjects(ctx context.Context, tx *txn, spaceID int64, key *types.Hash256, offset, limit int) ([]spaces.Object, error) {
	var objectKey any
	if key != nil {
		objectKey = sqlHash256(*key)
	}
	rows, err := tx.Query(ctx, `SELECT o.id, o.object_key, o.account_key, o.encrypted_data_key, o.encrypted_meta_key,
  o.encrypted_metadata, o.data_signature, o.meta_signature, o.created_at, o.updated_at
  FROM space_objects o
  WHERE o.space_id = $1 AND ($2::bytea IS NULL OR o.object_key = $2)
  AND NOT EXISTS (SELECT 1 FROM blocked_objects b WHERE b.object_key = o.object_key)
  ORDER BY o.created_at DESC, o.id DESC LIMIT $3 OFFSET $4`, spaceID, objectKey, limit, offset)
	if err != nil {
		return nil, err
	}
	var objects []spaces.Object
	var ids []int64
	err = forEachRow(rows, func(row pgx.CollectableRow) error {
		var o spaces.Object
		var id int64
		if err := row.Scan(&id, (*sqlHash256)(&o.Key), (*sqlPublicKey)(&o.Account), &o.EncryptedDataKey, &o.EncryptedMetadataKey,
			&o.EncryptedMetadata, (*sqlSignature)(&o.DataSignature), (*sqlSignature)(&o.MetadataSignature), &o.CreatedAt, &o.UpdatedAt); err != nil {
			return err
		}
		objects = append(objects, o)
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, err
	} else if len(ids) == 0 {
		return objects, nil
	}
	objectSlabs := make(map[int64]*[]slabs.SlabSlice, len(ids))
	for i, id := range ids {
		objectSlabs[id] = &objects[i].Slabs
	}
	return objects, loadSpaceObjectSlabs(ctx, tx, objectSlabs)
}

func loadSpaceObjectSlabs(ctx context.Context, tx *txn, objects map[int64]*[]slabs.SlabSlice) error {
	if len(objects) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT os.object_id, s.id, os.slab_offset, os.slab_length, s.encryption_key, s.min_shards, s.version
 FROM space_object_slabs os JOIN slabs s ON s.digest = os.slab_digest
 WHERE os.object_id = ANY($1) ORDER BY os.object_id, os.slab_index`, slices.Collect(maps.Keys(objects)))
	if err != nil {
		return err
	}
	return collectObjectSlabs(ctx, tx, rows, objects)
}

func listSpaceObjectEvents(ctx context.Context, tx *txn, spaceID int64, cursor slabs.Cursor, limit int) (events []spaces.ObjectEventWithoutSlabs, ids map[int64]int, err error) {
	rows, err := tx.Query(ctx, `SELECT object_key, was_deleted, updated_at FROM space_object_events e
 WHERE space_id = $1 AND (updated_at, object_key) > ($2, $3)
 AND NOT EXISTS (SELECT 1 FROM blocked_objects b WHERE b.object_key = e.object_key)
 ORDER BY updated_at, object_key LIMIT $4`, spaceID, cursor.After, sqlHash256(cursor.Key), limit)
	if err != nil {
		return nil, nil, err
	}
	events, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (event spaces.ObjectEventWithoutSlabs, err error) {
		err = row.Scan((*sqlHash256)(&event.Key), &event.Deleted, &event.UpdatedAt)
		return
	})
	if err != nil {
		return nil, nil, err
	}
	keys := make([]sqlHash256, 0, len(events))
	byKey := make(map[types.Hash256]int, len(events))
	for i, event := range events {
		if !event.Deleted {
			keys = append(keys, sqlHash256(event.Key))
			byKey[event.Key] = i
		}
	}
	ids = make(map[int64]int, len(keys))
	if len(keys) == 0 {
		return events, ids, nil
	}
	rows, err = tx.Query(ctx, `SELECT id, object_key, account_key, encrypted_data_key, encrypted_meta_key,
 encrypted_metadata, data_signature, meta_signature, created_at, updated_at, size
 FROM space_objects WHERE space_id = $1 AND object_key = ANY($2)`, spaceID, keys)
	if err != nil {
		return nil, nil, err
	}
	err = forEachRow(rows, func(row pgx.CollectableRow) error {
		var id int64
		var obj spaces.ObjectWithoutSlabs
		if err := row.Scan(&id, (*sqlHash256)(&obj.Key), (*sqlPublicKey)(&obj.Account), &obj.EncryptedDataKey, &obj.EncryptedMetadataKey,
			&obj.EncryptedMetadata, (*sqlSignature)(&obj.DataSignature), (*sqlSignature)(&obj.MetadataSignature), &obj.CreatedAt, &obj.UpdatedAt, &obj.Size); err != nil {
			return err
		}
		i := byKey[obj.Key]
		events[i].Object = &obj
		ids[id] = i
		return nil
	})
	for i := range events {
		if events[i].Object == nil {
			events[i].Deleted = true
		}
	}
	return events, ids, err
}
