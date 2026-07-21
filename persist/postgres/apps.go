package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
		&key.PinnedSize,
		&key.Quota,
		&key.RemainingUses,
	)
	if lastUsed.Valid {
		key.LastUsed = lastUsed.Time
	}
	return
}

func scanPreAuthorizedKey(s scanner) (key accounts.PreAuthorizedKey, err error) {
	var lastUsed sql.NullTime
	var allowedAppID sql.Null[sqlHash256]
	err = s.Scan(
		(*sqlPublicKey)(&key.PublicKey),
		&key.ConnectKey,
		&key.Expiration,
		&key.TotalUses,
		&key.RemainingUses,
		&allowedAppID,
		&key.DateCreated,
		&lastUsed,
	)
	if allowedAppID.Valid {
		appID := types.Hash256(allowedAppID.V)
		key.AllowedAppID = &appID
	}
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
			RETURNING app_key, use_description, created_at, updated_at, last_used, pinned_data, pinned_size,
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
			default:
				return err
			}
		} else if err != nil {
			return err
		}

		// create a pool for the new connect key, all accounts under a connect
		// key share a single pool for host funding on pool capable hosts
		poolKey := types.GeneratePrivateKey()
		_, err = tx.Exec(ctx, `INSERT INTO pools (connect_key_id, pool_key) VALUES ((SELECT id FROM app_connect_keys WHERE app_key = $1), $2)`, meta.Key, []byte(poolKey))
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
			RETURNING app_key, use_description, created_at, updated_at, last_used, pinned_data, pinned_size,
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
			SELECT ack.app_key, ack.use_description, ack.created_at, ack.updated_at, ack.last_used, ack.pinned_data, ack.pinned_size,
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
			SELECT ack.app_key, ack.use_description, ack.created_at, ack.updated_at, ack.last_used, ack.pinned_data, ack.pinned_size,
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

// AddPreAuthorizedKey creates a limited-use application authorization key.
func (s *Store) AddPreAuthorizedKey(req accounts.PreAuthorizedKeyRequest) (key accounts.PreAuthorizedKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		var connectKeyID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, req.ConnectKey).Scan(&connectKeyID); errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get connect key ID: %w", err)
		}

		var allowedAppID any
		if req.AllowedAppID != nil {
			allowedAppID = sqlHash256(*req.AllowedAppID)
		}
		key, err = scanPreAuthorizedKey(tx.QueryRow(ctx, `
			INSERT INTO preauthorized_keys (public_key, connect_key_id, expires_at, total_uses, remaining_uses, allowed_app_id)
			VALUES ($1, $2, $3, $4, $4, $5)
			ON CONFLICT (public_key) DO NOTHING
			RETURNING public_key, $6::text, expires_at, total_uses, remaining_uses, allowed_app_id, created_at, last_used
		`, sqlPublicKey(req.PublicKey), connectKeyID, req.Expiration, req.TotalUses, allowedAppID, req.ConnectKey))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyAlreadyExists
		}
		return err
	})
	return
}

// PreAuthorizedKey returns a pre-authorized key by its public key.
func (s *Store) PreAuthorizedKey(publicKey types.PublicKey) (preAuthorizedKey accounts.PreAuthorizedKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		preAuthorizedKey, err = scanPreAuthorizedKey(tx.QueryRow(ctx, `
			SELECT pak.public_key, ack.app_key, pak.expires_at, pak.total_uses, pak.remaining_uses,
				pak.allowed_app_id, pak.created_at, pak.last_used
			FROM preauthorized_keys pak
			INNER JOIN app_connect_keys ack ON ack.id = pak.connect_key_id
			WHERE pak.public_key = $1
		`, sqlPublicKey(publicKey)))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		}
		return err
	})
	return
}

// PreAuthorizedKeys returns a paginated list of pre-authorized keys.
func (s *Store) PreAuthorizedKeys(offset, limit int) (keys []accounts.PreAuthorizedKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		keys = keys[:0]
		rows, err := tx.Query(ctx, `
			SELECT pak.public_key, ack.app_key, pak.expires_at, pak.total_uses, pak.remaining_uses,
				pak.allowed_app_id, pak.created_at, pak.last_used
			FROM preauthorized_keys pak
			INNER JOIN app_connect_keys ack ON ack.id = pak.connect_key_id
			ORDER BY pak.created_at DESC
			LIMIT $1 OFFSET $2
		`, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			key, err := scanPreAuthorizedKey(rows)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
		return rows.Err()
	})
	return
}

// DeletePreAuthorizedKey deletes a pre-authorized key.
func (s *Store) DeletePreAuthorizedKey(publicKey types.PublicKey) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `DELETE FROM preauthorized_keys WHERE public_key = $1`, sqlPublicKey(publicKey))
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return accounts.ErrKeyNotFound
		}
		return nil
	})
}

// PruneExpiredPreAuthorizedKeys deletes all pre-authorized keys that expired
// on or before the cutoff.
func (s *Store) PruneExpiredPreAuthorizedKeys(cutoff time.Time) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `DELETE FROM preauthorized_keys WHERE expires_at <= $1`, cutoff)
		return err
	})
}

// ConsumePreAuthorizedKey validates and consumes one use of a pre-authorized
// key. The update and authorization lookup are atomic, preventing concurrent
// callers from exceeding the key's use limit.
func (s *Store) ConsumePreAuthorizedKey(publicKey types.PublicKey, appID types.Hash256) (connectKey string, userSecret types.Hash256, reconnecting bool, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `
			UPDATE preauthorized_keys pak
			SET remaining_uses = pak.remaining_uses - 1, last_used = NOW()
			FROM app_connect_keys ack
			WHERE pak.public_key = $1
				AND ack.id = pak.connect_key_id
				AND pak.expires_at > NOW()
				AND pak.remaining_uses > 0
				AND (pak.allowed_app_id IS NULL OR pak.allowed_app_id = $2)
			RETURNING ack.app_key
		`, sqlPublicKey(publicKey), sqlHash256(appID)).Scan(&connectKey)
		if errors.Is(err, sql.ErrNoRows) {
			var expired bool
			var remainingUses int
			var allowedAppID sql.Null[sqlHash256]
			err = tx.QueryRow(ctx, `
				SELECT expires_at <= NOW(), remaining_uses, allowed_app_id
				FROM preauthorized_keys
				WHERE public_key = $1
			`, sqlPublicKey(publicKey)).Scan(&expired, &remainingUses, &allowedAppID)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return accounts.ErrKeyNotFound
			case err != nil:
				return err
			case expired:
				return accounts.ErrPreAuthorizedKeyExpired
			case remainingUses <= 0:
				return accounts.ErrPreAuthorizedKeyExhausted
			case allowedAppID.Valid && types.Hash256(allowedAppID.V) != appID:
				return accounts.ErrPreAuthorizedKeyAppMismatch
			default:
				return accounts.ErrKeyNotFound
			}
		} else if err != nil {
			return err
		}

		userSecret, reconnecting, err = appAuthorization(ctx, tx, connectKey, appID)
		return err
	})
	return
}

func appAuthorization(ctx context.Context, tx *txn, connectKey string, appID types.Hash256) (userSecret types.Hash256, reconnecting bool, err error) {
	err = tx.QueryRow(ctx, `
		SELECT ack.user_secret, EXISTS (
			SELECT 1 FROM accounts a
			WHERE a.connect_key_id = ack.id AND a.app_id = $2 AND a.deleted_at IS NULL
		)
		FROM app_connect_keys ack
		WHERE ack.app_key = $1
	`, connectKey, sqlHash256(appID)).Scan((*sqlHash256)(&userSecret), &reconnecting)
	if errors.Is(err, sql.ErrNoRows) {
		err = accounts.ErrKeyNotFound
	}
	return
}

// AppAuthorization returns the raw authorization material for a connect key
// and reports whether the application was previously connected.
func (s *Store) AppAuthorization(connectKey string, appID types.Hash256) (userSecret types.Hash256, reconnecting bool, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		userSecret, reconnecting, err = appAuthorization(ctx, tx, connectKey, appID)
		return err
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

// RegisterAppKey uses a connect key to register a new app account.
// This secret must never be exposed to the user.
func (s *Store) RegisterAppKey(connectKey string, appKey types.PublicKey, meta accounts.AppMeta) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var remainingUses int
		err := tx.QueryRow(ctx, `
			UPDATE app_connect_keys ack SET last_used = NOW()
			FROM quotas q
			WHERE ack.app_key = $1 AND q.name = ack.quota_name
			RETURNING GREATEST(0, q.total_uses - (SELECT COUNT(*) FROM accounts a WHERE a.connect_key_id = ack.id AND a.deleted_at IS NULL))
		`, connectKey).Scan(&remainingUses)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to update app connect key %q: %w", connectKey, err)
		}

		err = addAccount(ctx, tx, connectKey, appKey, meta)
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
