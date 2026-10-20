package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
)

// SpaceObject is an encrypted object in a space and the account that signed it.
type SpaceObject struct {
	Key     types.Hash256
	Account proto.Account
	slabs.SealedObject
}

// PinSpaceObject adds or replaces an object in a space, charging storage to the
// owner. Account must have write permission, and the object's slabs must already
// be pinned by account or the owner.
func (s *Store) PinSpaceObject(account proto.Account, key types.Hash256, obj slabs.PinObjectRequest) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, SpaceWrite, false)
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
   AND EXISTS (SELECT 1 FROM account_slabs a WHERE a.slab_id = s.id AND a.account_id IN ($2, $3))`, digests, callerID, ownerID).Scan(&count); err != nil {
			return err
		} else if count != len(unique) {
			return slabs.ErrObjectUnpinnedSlab
		} else if err := pinSpaceObjectSlabs(ctx, tx, ownerID, digests); err != nil {
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
   encrypted_data_key = EXCLUDED.encrypted_data_key, encrypted_meta_key = EXCLUDED.encrypted_meta_key,
   encrypted_metadata = EXCLUDED.encrypted_metadata, data_signature = EXCLUDED.data_signature,
   meta_signature = EXCLUDED.meta_signature, size = EXCLUDED.size, updated_at = NOW() RETURNING id`,
			id, sqlHash256(obj.ID), sqlPublicKey(account), obj.EncryptedDataKey, metaKey, obj.EncryptedMetadata, sqlSignature(obj.DataSignature), sqlSignature(obj.MetadataSignature), obj.Size()).Scan(&objectID)
		if err != nil {
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
		return s.unpinUnreferencedSlabs(ctx, tx, ownerID, oldSlabs, nil)
	})
}

// DeleteSpaceObject removes an object from a space and releases storage no longer
// used by the owner's objects. Account must have delete permission.
func (s *Store) DeleteSpaceObject(account proto.Account, key, objectKey types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, _, err := spaceAccess(ctx, tx, account, key, SpaceDelete, false)
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
		return s.unpinUnreferencedSlabs(ctx, tx, ownerID, slabIDs, nil)
	})
}

// DeleteSpace removes a space, its objects, memberships, and invitations.
// Only the owner may delete it.
func (s *Store) DeleteSpace(account proto.Account, key types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, _, err := spaceAccess(ctx, tx, account, key, spaceOwner, false)
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

// SpaceObjects returns a page of objects in a space, newest first, excluding
// blocked objects. Account must have read permission.
func (s *Store) SpaceObjects(account proto.Account, key types.Hash256, offset, limit int) (objects []SpaceObject, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, SpaceRead, true)
		if err != nil {
			return err
		}
		objects, err = spaceObjects(ctx, tx, id, nil, offset, limit)
		return err
	})
	return
}

// SpaceObject returns an object in a space. Account must have read permission,
// and the object must not be blocked.
func (s *Store) SpaceObject(account proto.Account, key, objectKey types.Hash256) (object SpaceObject, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, SpaceRead, true)
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
		_, ownerID, _, err := spaceAccess(ctx, tx, account, key, SpaceWrite, false)
		if err != nil {
			return err
		}
		var owner proto.Account
		if err := tx.QueryRow(ctx, `SELECT public_key FROM accounts WHERE id = $1`, ownerID).Scan((*sqlPublicKey)(&owner)); err != nil {
			return err
		}
		digests, err = s.pinSlabs(ctx, tx, owner, nextIntegrityCheck, toPin...)
		return err
	})
	return
}

func pinSpaceObjectSlabs(ctx context.Context, tx *txn, ownerID int64, digests []sqlHash256) error {
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
	return nil
}

func spaceSlabIDs(ctx context.Context, tx *txn, spaceID int64, objectID *int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT s.id FROM slabs s
  JOIN space_object_slabs os ON os.slab_digest = s.digest JOIN space_objects o ON o.id = os.object_id
  WHERE o.space_id = $1 AND ($2::bigint IS NULL OR o.id = $2)`, spaceID, objectID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func spaceObjects(ctx context.Context, tx *txn, spaceID int64, key *types.Hash256, offset, limit int) ([]SpaceObject, error) {
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
	var objects []SpaceObject
	var ids []int64
	err = forEachRow(rows, func(row pgx.CollectableRow) error {
		var o SpaceObject
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
	rows, err = tx.Query(ctx, `SELECT os.object_id, s.id, os.slab_offset, os.slab_length, s.encryption_key, s.min_shards, s.version
  FROM space_object_slabs os JOIN slabs s ON s.digest = os.slab_digest
  WHERE os.object_id = ANY($1) ORDER BY os.object_id, os.slab_index`, ids)
	if err != nil {
		return nil, err
	} else if err := collectObjectSlabs(ctx, tx, rows, objectSlabs); err != nil {
		return nil, err
	}
	return objects, nil
}
