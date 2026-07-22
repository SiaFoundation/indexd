package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
)

func scanSharingKey(s scanner) (key sharing.Key, err error) {
	var nonce []byte
	err = s.Scan(
		(*sqlPublicKey)(&key.Account),
		(*sqlPublicKey)(&key.PublicKey),
		&nonce,
		&key.Description,
		&key.ObjectCount,
		&key.ObjectSize,
		&key.PinnedData,
		&key.PinnedSize,
		&key.ExpiresAt,
		&key.CreatedAt,
		&key.UpdatedAt,
	)
	copy(key.Nonce[:], nonce)
	return
}

// AddSharingKey creates a sharing key owned by the given account.
func (s *Store) AddSharingKey(account proto.Account, req sharing.KeyRequest) (key sharing.Key, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}

		var expiresAt any
		if req.ExpiresAt != nil {
			expiresAt = *req.ExpiresAt
		}
		key, err = scanSharingKey(tx.QueryRow(ctx, `
			WITH ins AS (
				INSERT INTO sharing_keys (account_id, public_key, nonce, use_description, object_count, size, pinned_data, pinned_size, expires_at)
				VALUES ($1, $2, $3, $4, 0, 0, 0, 0, $5)
				ON CONFLICT (public_key) DO NOTHING
				RETURNING account_id, public_key, nonce, use_description, object_count, size, pinned_data, pinned_size, expires_at, created_at, updated_at
			)
			SELECT a.public_key, ins.public_key, ins.nonce, ins.use_description, ins.object_count, ins.size, ins.pinned_data, ins.pinned_size, ins.expires_at, ins.created_at, ins.updated_at
			FROM ins INNER JOIN accounts a ON a.id = ins.account_id
		`, accountID, sqlPublicKey(req.PublicKey), req.Nonce[:], req.Description, expiresAt))
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharingKeyExists
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return sharing.ErrSharingKeyExists
		}
		return err
	})
	return
}

// SharingKey returns the sharing key with the given public key.
func (s *Store) SharingKey(publicKey types.PublicKey) (key sharing.Key, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		key, err = scanSharingKey(tx.QueryRow(ctx, `
			SELECT a.public_key, sk.public_key, sk.nonce, sk.use_description, sk.object_count, sk.size, sk.pinned_data, sk.pinned_size, sk.expires_at, sk.created_at, sk.updated_at
			FROM sharing_keys sk
			INNER JOIN accounts a ON a.id = sk.account_id
			WHERE sk.public_key = $1 AND a.deleted_at IS NULL AND (sk.expires_at IS NULL OR sk.expires_at > NOW())
		`, sqlPublicKey(publicKey)))
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharingKeyNotFound
		}
		return err
	})
	return
}

// SharingKeys returns a paginated list of the given account's sharing keys,
// most recently created first.
func (s *Store) SharingKeys(account proto.Account, offset, limit int) (keys []sharing.Key, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			SELECT a.public_key, sk.public_key, sk.nonce, sk.use_description, sk.object_count, sk.size, sk.pinned_data, sk.pinned_size, sk.expires_at, sk.created_at, sk.updated_at
			FROM sharing_keys sk
			INNER JOIN accounts a ON a.id = sk.account_id
			WHERE sk.account_id = $1 AND (sk.expires_at IS NULL OR sk.expires_at > NOW())
			ORDER BY sk.created_at DESC
			LIMIT $2 OFFSET $3
		`, accountID, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		keys = keys[:0]
		for rows.Next() {
			key, err := scanSharingKey(rows)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
		return rows.Err()
	})
	return
}

// DeleteSharingKey deletes the given account's sharing key along with its
// attached objects.
func (s *Store) DeleteSharingKey(account proto.Account, publicKey types.PublicKey) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		res, err := tx.Exec(ctx, `DELETE FROM sharing_keys WHERE public_key = $1 AND account_id = $2`, sqlPublicKey(publicKey), accountID)
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return sharing.ErrSharingKeyNotFound
		}
		return nil
	})
}

// AddSharedObject attaches an object the account owns to one of its sharing
// keys. If the object is already attached, its re-sealed keys and signatures
// are overwritten.
func (s *Store) AddSharedObject(account proto.Account, sharingKey types.PublicKey, req sharing.SharedObjectRequest) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}

		var sharingKeyID int64
		err = tx.QueryRow(ctx, `SELECT id FROM sharing_keys WHERE public_key = $1 AND account_id = $2 AND (expires_at IS NULL OR expires_at > NOW())`, sqlPublicKey(sharingKey), accountID).Scan(&sharingKeyID)
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharingKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get sharing key: %w", err)
		}

		var objectID int64
		err = tx.QueryRow(ctx, `SELECT id FROM objects WHERE object_key = $1 AND account_id = $2`, sqlHash256(req.ObjectID), accountID).Scan(&objectID)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get object: %w", err)
		}

		// capture the object's sizes at attach time so the trigger can
		// maintain the sharing key's totals: size is the object's logical size
		// (sum of slab slice lengths), pinned_data/pinned_size are its storage
		// footprint before and after redundancy.
		var objectSize, minShards, sectorCount int64
		err = tx.QueryRow(ctx, `
			SELECT
				COALESCE(SUM(os.slab_length), 0)::bigint,
				COALESCE(SUM(s.min_shards), 0)::bigint,
				COALESCE(SUM((SELECT COUNT(*) FROM slab_sectors ss WHERE ss.slab_id = s.id)), 0)::bigint
			FROM object_slabs os
			INNER JOIN slabs s ON s.digest = os.slab_digest
			WHERE os.object_id = $1
		`, objectID).Scan(&objectSize, &minShards, &sectorCount)
		if err != nil {
			return fmt.Errorf("failed to compute object size: %w", err)
		}
		size := uint64(objectSize)
		pinnedData := uint64(minShards) * proto.SectorSize
		pinnedSize := uint64(sectorCount) * proto.SectorSize

		// ensure empty slices are passed as nil
		var encryptedMetaKey []byte
		if len(req.EncryptedMetadataKey) > 0 {
			encryptedMetaKey = req.EncryptedMetadataKey
		}
		var encryptedMeta []byte
		if len(req.EncryptedMetadata) > 0 {
			encryptedMeta = req.EncryptedMetadata
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO shared_objects (object_id, sharing_key_id, encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, size, pinned_data, pinned_size)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (object_id, sharing_key_id) DO UPDATE SET (encrypted_data_key, encrypted_meta_key, encrypted_metadata, data_signature, meta_signature, size, pinned_data, pinned_size, updated_at) = (EXCLUDED.encrypted_data_key, EXCLUDED.encrypted_meta_key, EXCLUDED.encrypted_metadata, EXCLUDED.data_signature, EXCLUDED.meta_signature, EXCLUDED.size, EXCLUDED.pinned_data, EXCLUDED.pinned_size, NOW())`,
			objectID, sharingKeyID, req.EncryptedDataKey, encryptedMetaKey, encryptedMeta, sqlSignature(req.DataSignature), sqlSignature(req.MetadataSignature), size, pinnedData, pinnedSize)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
				return sharing.ErrSharedObjectConflict
			}
			return fmt.Errorf("failed to insert shared object: %w", err)
		}
		return nil
	})
}

// PruneExpiredSharingKeys deletes all sharing keys that expired on or before
// the cutoff.
func (s *Store) PruneExpiredSharingKeys(cutoff time.Time) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `DELETE FROM sharing_keys WHERE expires_at IS NOT NULL AND expires_at <= $1`, cutoff)
		return err
	})
}

// SharedObjects returns a paginated list of the objects attached to the sharing
// key, most recently attached first. Each object's encryption keys and
// signatures are the ones re-sealed under the sharing key.
func (s *Store) SharedObjects(sharingKey types.PublicKey, offset, limit int) (objects []slabs.SealedObject, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		var sharingKeyID int64
		err = tx.QueryRow(ctx, `
			SELECT sk.id FROM sharing_keys sk
			INNER JOIN accounts a ON a.id = sk.account_id
			WHERE sk.public_key = $1 AND a.deleted_at IS NULL AND (sk.expires_at IS NULL OR sk.expires_at > NOW())
		`, sqlPublicKey(sharingKey)).Scan(&sharingKeyID)
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharingKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get sharing key: %w", err)
		}

		// fetch the page of shared objects along with their re-sealed keys
		rows, err := tx.Query(ctx, `
			SELECT so.object_id, so.encrypted_data_key, so.encrypted_meta_key, so.encrypted_metadata, so.data_signature, so.meta_signature, so.created_at, so.updated_at
			FROM shared_objects so
			WHERE so.sharing_key_id = $1
			ORDER BY so.created_at DESC
			LIMIT $2 OFFSET $3
		`, sharingKeyID, limit, offset)
		if err != nil {
			return err
		}

		var objectIDs []int64
		objects = objects[:0]
		for rows.Next() {
			var objectID int64
			var obj slabs.SealedObject
			var metaKey sql.Null[[]byte]
			if err := rows.Scan(&objectID, &obj.EncryptedDataKey, &metaKey, &obj.EncryptedMetadata, (*sqlSignature)(&obj.DataSignature), (*sqlSignature)(&obj.MetadataSignature), &obj.CreatedAt, &obj.UpdatedAt); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan shared object: %w", err)
			}
			if metaKey.Valid {
				obj.EncryptedMetadataKey = metaKey.V
			}
			objects = append(objects, obj)
			objectIDs = append(objectIDs, objectID)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// load each object's slabs (with decorated sectors)
		for i := range objects {
			if err := loadObjectSlabs(ctx, tx, objectIDs[i], &objects[i]); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// SharingKeyObject returns a single object attached to the sharing key with its
// encryption keys re-sealed under the sharing key and its slabs decorated with
// sectors.
func (s *Store) SharingKeyObject(sharingKey types.PublicKey, objectKey types.Hash256) (obj slabs.SealedObject, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		obj = slabs.SealedObject{} // reset if the transaction retries

		var sharingKeyID int64
		err := tx.QueryRow(ctx, `
			SELECT sk.id FROM sharing_keys sk
			INNER JOIN accounts a ON a.id = sk.account_id
			WHERE sk.public_key = $1 AND a.deleted_at IS NULL AND (sk.expires_at IS NULL OR sk.expires_at > NOW())
		`, sqlPublicKey(sharingKey)).Scan(&sharingKeyID)
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharingKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get sharing key: %w", err)
		}

		var objectID int64
		var metaKey sql.Null[[]byte]
		err = tx.QueryRow(ctx, `
			SELECT so.object_id, so.encrypted_data_key, so.encrypted_meta_key, so.encrypted_metadata, so.data_signature, so.meta_signature, so.created_at, so.updated_at
			FROM shared_objects so
			INNER JOIN objects o ON o.id = so.object_id
			WHERE so.sharing_key_id = $1 AND o.object_key = $2
		`, sharingKeyID, sqlHash256(objectKey)).Scan(&objectID, &obj.EncryptedDataKey, &metaKey, &obj.EncryptedMetadata, (*sqlSignature)(&obj.DataSignature), (*sqlSignature)(&obj.MetadataSignature), &obj.CreatedAt, &obj.UpdatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharedObjectNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get shared object: %w", err)
		}
		if metaKey.Valid {
			obj.EncryptedMetadataKey = metaKey.V
		}

		return loadObjectSlabs(ctx, tx, objectID, &obj)
	})
	return
}

// SharingAccountKey returns the sharing account key derived from the owner of
// the sharing key. It is used to sign account tokens for downloading the
// shared objects.
func (s *Store) SharingAccountKey(sharingKey types.PublicKey) (key types.PrivateKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		var userSecret types.Hash256
		err := tx.QueryRow(ctx, `
			SELECT ack.user_secret
			FROM sharing_keys sk
			INNER JOIN accounts a ON a.id = sk.account_id
			INNER JOIN app_connect_keys ack ON ack.id = a.connect_key_id
			WHERE sk.public_key = $1 AND a.deleted_at IS NULL AND (sk.expires_at IS NULL OR sk.expires_at > NOW())
		`, sqlPublicKey(sharingKey)).Scan((*sqlHash256)(&userSecret))
		if errors.Is(err, sql.ErrNoRows) {
			return sharing.ErrSharingKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get user secret: %w", err)
		}
		key = accounts.DeriveSharingAccountKey(userSecret)
		return nil
	})
	return
}

// DeleteSharedObject detaches an object from one of the account's sharing keys.
func (s *Store) DeleteSharedObject(account proto.Account, sharingKey types.PublicKey, objectKey types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		accountID, _, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		}

		res, err := tx.Exec(ctx, `
			DELETE FROM shared_objects so
			USING sharing_keys sk, objects o
			WHERE so.sharing_key_id = sk.id
				AND so.object_id = o.id
				AND sk.public_key = $1
				AND sk.account_id = $2
				AND o.object_key = $3
				AND o.account_id = $2`,
			sqlPublicKey(sharingKey), accountID, sqlHash256(objectKey))
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return sharing.ErrSharedObjectNotFound
		}
		return nil
	})
}
