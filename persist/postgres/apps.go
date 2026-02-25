package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"lukechampine.com/frand"
)

func scanConnectKey(s scanner) (key accounts.ConnectKey, err error) {
	var lastUsed sql.NullTime
	err = s.Scan(
		&key.Key,
		&key.Description,
		&key.DateCreated,
		&key.LastUpdated,
		&lastUsed,
		&key.PinnedData,
		&key.Quota,
		&key.RemainingUses,
	)
	if lastUsed.Valid {
		key.LastUsed = lastUsed.Time
	}
	return
}

// AddAppConnectKey adds or updates an application connection key in the database.
func (s *Store) AddAppConnectKey(meta accounts.AppConnectKeyRequest) (key accounts.ConnectKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		userSecret := frand.Bytes(32)
		key, err = scanConnectKey(tx.QueryRow(ctx, `
			INSERT INTO app_connect_keys (app_key, user_secret, use_description, quota_name)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (app_key) DO NOTHING
			RETURNING app_key, use_description, created_at, updated_at, last_used, pinned_data,
				quota_name,
				(SELECT total_uses FROM quotas WHERE name = quota_name)
		`, meta.Key, userSecret, meta.Description, meta.Quota))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyAlreadyExists
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case pgerrcode.ForeignKeyViolation:
				return accounts.ErrQuotaNotFound
			case pgerrcode.UniqueViolation:
				return accounts.ErrKeyAlreadyExists
			}
		}
		return err
	})
	return
}

// UpdateAppConnectKey updates an existing application connection key in the database.
// If the key does not exist, it returns [app.ErrKeyNotFound].
func (s *Store) UpdateAppConnectKey(meta accounts.AppConnectKeyRequest) (key accounts.ConnectKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		// verify quota exists
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM quotas WHERE name = $1)`, meta.Quota).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check quota: %w", err)
		} else if !exists {
			return accounts.ErrQuotaNotFound
		}

		key, err = scanConnectKey(tx.QueryRow(ctx, `
			UPDATE app_connect_keys ack SET (use_description, quota_name) = ($2, $3) WHERE app_key = $1
			RETURNING app_key, use_description, created_at, updated_at, last_used, pinned_data,
				quota_name,
				GREATEST(0, (SELECT total_uses FROM quotas WHERE name = quota_name) - (SELECT COUNT(*) FROM accounts WHERE connect_key_id = ack.id AND deleted_at IS NULL))
		`, meta.Key, meta.Description, meta.Quota))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation {
			return accounts.ErrQuotaNotFound
		}
		return err
	})
	return
}

// ValidAppConnectKey checks if an application connection key exists.
func (s *Store) ValidAppConnectKey(key string) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var id int64
		err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, key).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		}
		return err
	})
}

// AppConnectKey retrieves an application connection key from the database.
func (s *Store) AppConnectKey(key string) (connectKey accounts.ConnectKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		connectKey, err = scanConnectKey(tx.QueryRow(ctx, `
			SELECT ack.app_key, ack.use_description, ack.created_at, ack.updated_at, ack.last_used, ack.pinned_data,
				ack.quota_name,
				GREATEST(0, q.total_uses - (SELECT COUNT(*) FROM accounts WHERE connect_key_id = ack.id AND deleted_at IS NULL))
			FROM app_connect_keys ack
			INNER JOIN quotas q ON q.name = ack.quota_name
			WHERE ack.app_key = $1`, key))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		}
		return err
	})
	return
}

// AppConnectKeys retrieves a list of application connection keys from the database.
func (s *Store) AppConnectKeys(offset, limit int) (keys []accounts.ConnectKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		keys = keys[:0] // reuse same slice if transaction retries

		rows, err := tx.Query(ctx, `
			SELECT ack.app_key, ack.use_description, ack.created_at, ack.updated_at, ack.last_used, ack.pinned_data,
				ack.quota_name,
				GREATEST(0, q.total_uses - COALESCE(ac.cnt, 0))
			FROM app_connect_keys ack
			INNER JOIN quotas q ON q.name = ack.quota_name
			LEFT JOIN (
				SELECT connect_key_id, COUNT(*) AS cnt
				FROM accounts
				WHERE deleted_at IS NULL
				GROUP BY connect_key_id
			) ac ON ac.connect_key_id = ack.id
			ORDER BY ack.created_at DESC
			LIMIT $1 OFFSET $2
		`, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			key, err := scanConnectKey(rows)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
		return rows.Err()
	})
	return
}

// DeleteAppConnectKey deletes an application connection key from the database.
func (s *Store) DeleteAppConnectKey(connectKey string) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var connectKeyID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey).Scan(&connectKeyID); errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get connect key ID: %w", err)
		}

		var inUse bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE connect_key_id = $1)`, connectKeyID).Scan(&inUse); err != nil {
			return fmt.Errorf("failed to check if connect key in use: %w", err)
		} else if inUse {
			// it is only safe to delete if there are no accounts linked to this connect key
			return accounts.ErrKeyInUse
		}

		_, err := tx.Exec(ctx, `
			DELETE FROM app_connect_keys WHERE app_key = $1
		`, connectKey)
		return err
	})
}

// AppConnectKeyUserSecret retrieves the user secret associated with a connect key.
func (s *Store) AppConnectKeyUserSecret(connectKey string) (secret types.Hash256, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `
			SELECT user_secret FROM app_connect_keys WHERE app_key = $1
		`, connectKey).Scan((*sqlHash256)(&secret))
	})
	if errors.Is(err, sql.ErrNoRows) {
		return types.Hash256{}, accounts.ErrKeyNotFound
	}
	return
}

// RegisterAppKey uses a connect key to register a new app account.
// This secret must never be exposed to the user.
func (s *Store) RegisterAppKey(connectKey string, appKey types.PublicKey, meta accounts.AppMeta) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var remainingUses int
		var storageLimit uint64
		err := tx.QueryRow(ctx, `
			UPDATE app_connect_keys ack SET last_used = NOW()
			FROM quotas q
			WHERE ack.app_key = $1 AND q.name = ack.quota_name
			RETURNING GREATEST(0, q.total_uses - (SELECT COUNT(*) FROM accounts a WHERE a.connect_key_id = ack.id AND a.deleted_at IS NULL)), q.max_pinned_data
		`, connectKey).Scan(&remainingUses, &storageLimit)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to update app connect key %q: %w", connectKey, err)
		}

		err = addAccount(ctx, tx, connectKey, appKey, meta, accounts.WithMaxPinnedData(storageLimit))
		if errors.Is(err, accounts.ErrExists) {
			// account already registered — re-auth is always allowed regardless of remaining uses
			return err
		} else if err != nil {
			return fmt.Errorf("failed to add app account: %w", err)
		} else if remainingUses <= 0 {
			// new account created — enforce quota
			return accounts.ErrKeyExhausted
		}
		return nil
	})
}
