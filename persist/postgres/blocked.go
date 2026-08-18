package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// assertObjectNotBlocked returns slabs.ErrObjectBlocked if the given object key
// is on the blocklist.
func assertObjectNotBlocked(ctx context.Context, tx *txn, objectKey types.Hash256) error {
	var blocked bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM blocked_objects WHERE object_key = $1)`, sqlHash256(objectKey)).Scan(&blocked); err != nil {
		return fmt.Errorf("failed to check blocklist: %w", err)
	} else if blocked {
		return slabs.ErrObjectBlocked
	}
	return nil
}

// BlockObject adds the given object key to the blocklist. If the key is already
// blocked its reason is updated.
func (s *Store) BlockObject(objectKey types.Hash256, reason string) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO blocked_objects (object_key, reason)
			VALUES ($1, $2)
			ON CONFLICT (object_key) DO UPDATE SET reason = EXCLUDED.reason
		`, sqlHash256(objectKey), reason)
		if err != nil {
			return fmt.Errorf("failed to block object: %w", err)
		}
		return nil
	})
}

// UnblockObject removes an object key from the blocklist. Unblocking a key that
// is not blocked is a no-op.
func (s *Store) UnblockObject(objectKey types.Hash256) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `DELETE FROM blocked_objects WHERE object_key = $1`, sqlHash256(objectKey))
		if err != nil {
			return fmt.Errorf("failed to unblock object: %w", err)
		} else if res.RowsAffected() == 0 {
			return nil // nothing was blocked, so there is no event to bump
		}

		// bump the event so clients that already paged past the object pick it
		// up again. ListObjects withholds the in-progress second, so a cursor
		// can never rest on the one we stamp here.
		if _, err := tx.Exec(ctx, `
			UPDATE object_events
			SET updated_at = date_trunc('second', NOW())
			WHERE object_key = $1
		`, sqlHash256(objectKey)); err != nil {
			return fmt.Errorf("failed to bump object events: %w", err)
		}
		return nil
	})
}

// BlockedObject returns the blocklist entry for the given object key.
func (s *Store) BlockedObject(objectKey types.Hash256) (obj slabs.BlockedObject, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `
			SELECT object_key, reason, created_at
			FROM blocked_objects
			WHERE object_key = $1
		`, sqlHash256(objectKey)).Scan((*sqlHash256)(&obj.Key), &obj.Reason, &obj.CreatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrObjectNotBlocked
		} else if err != nil {
			return fmt.Errorf("failed to get blocked object: %w", err)
		}
		return nil
	})
	return
}

// BlockedObjects returns a paginated list of the blocked object keys, most
// recently blocked first.
func (s *Store) BlockedObjects(offset, limit int) (blocked []slabs.BlockedObject, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		blocked = blocked[:0] // reuse same slice if transaction retries

		rows, err := tx.Query(ctx, `
			SELECT object_key, reason, created_at
			FROM blocked_objects
			ORDER BY created_at DESC, object_key ASC
			LIMIT $1 OFFSET $2
		`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query blocklist: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var obj slabs.BlockedObject
			if err := rows.Scan((*sqlHash256)(&obj.Key), &obj.Reason, &obj.CreatedAt); err != nil {
				return fmt.Errorf("failed to scan blocked object: %w", err)
			}
			blocked = append(blocked, obj)
		}
		return rows.Err()
	})
	return
}
