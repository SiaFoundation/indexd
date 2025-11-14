package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
)

func scanConnectKey(s scanner) (key accounts.ConnectKey, err error) {
	var lastUsed sql.NullTime
	err = s.Scan(
		&key.Key,
		&key.Description,
		&key.RemainingUses,
		&key.TotalUses,
		&key.DateCreated,
		&key.LastUpdated,
		&lastUsed,
		&key.MaxPinnedData,
	)
	if lastUsed.Valid {
		key.LastUsed = lastUsed.Time
	}
	return
}

// AddAppConnectKey adds or updates an application connection key in the database.
func (s *Store) AddAppConnectKey(meta accounts.UpdateAppConnectKey) (key accounts.ConnectKey, err error) {
	if meta.RemainingUses <= 0 {
		return accounts.ConnectKey{}, accounts.ErrKeyExhausted
	} else if meta.MaxPinnedData == 0 {
		return accounts.ConnectKey{}, fmt.Errorf("max pinned data must be greater than 0")
	}
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		key, err = scanConnectKey(tx.QueryRow(ctx, `
			INSERT INTO app_connect_keys (app_key, use_description, remaining_uses, max_pinned_data) VALUES ($1, $2, $3, $4)
			RETURNING app_key, use_description, remaining_uses, total_uses, created_at, updated_at, last_used, max_pinned_data;
		`, meta.Key, meta.Description, meta.RemainingUses, meta.MaxPinnedData))
		return err
	})
	return
}

// UpdateAppConnectKey updates an existing application connection key in the database.
// If the key does not exist, it returns [app.ErrKeyNotFound].
func (s *Store) UpdateAppConnectKey(meta accounts.UpdateAppConnectKey) (key accounts.ConnectKey, err error) {
	if meta.RemainingUses <= 0 {
		return accounts.ConnectKey{}, accounts.ErrKeyExhausted
	} else if meta.MaxPinnedData == 0 {
		return accounts.ConnectKey{}, fmt.Errorf("max pinned data must be greater than 0")
	}
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		key, err = scanConnectKey(tx.QueryRow(ctx, `
			UPDATE app_connect_keys SET (use_description, remaining_uses, max_pinned_data) = ($2, $3, $4) WHERE app_key = $1
			RETURNING app_key, use_description, remaining_uses, total_uses, created_at, updated_at, last_used, max_pinned_data;
		`, meta.Key, meta.Description, meta.RemainingUses, meta.MaxPinnedData))
		return err
	})
	return
}

// ValidAppConnectKey checks if an application connection key is valid.
func (s *Store) ValidAppConnectKey(key string) (bool, error) {
	var uses int
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `
			SELECT remaining_uses FROM app_connect_keys WHERE app_key = $1
		`, key).Scan(&uses)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, accounts.ErrKeyNotFound
	} else if err != nil {
		return false, err
	}
	return uses > 0, nil
}

// AppConnectKey retrieves an application connection key from the database.
func (s *Store) AppConnectKey(key string) (connectKey accounts.ConnectKey, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		connectKey, err = scanConnectKey(tx.QueryRow(ctx, `
			SELECT app_key, use_description, remaining_uses, total_uses, created_at, updated_at, last_used, max_pinned_data
			FROM app_connect_keys
			WHERE app_key = $1`, key))
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
		rows, err := tx.Query(ctx, `
			SELECT app_key, use_description, remaining_uses, total_uses, created_at, updated_at, last_used, max_pinned_data
			FROM app_connect_keys
			ORDER BY created_at DESC
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

// UseAppConnectKey decrements the remaining uses of a connect key
// and adds the app account.
func (s *Store) UseAppConnectKey(connectKey string, appKey types.PublicKey, meta accounts.AccountMeta) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var uses int
		var storageLimit uint64
		err := tx.QueryRow(ctx, `
			UPDATE app_connect_keys SET (remaining_uses, total_uses, last_used) = (remaining_uses - 1, total_uses + 1, NOW())
			WHERE app_key = $1 RETURNING remaining_uses, max_pinned_data
		`, connectKey).Scan(&uses, &storageLimit)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to update app connect key %q: %w", connectKey, err)
		} else if uses < 0 {
			// uses is returned after updating, -1 would mean the key is exhausted
			return accounts.ErrKeyExhausted
		}

		if err := addAccount(ctx, tx, &connectKey, appKey, false, accounts.AccountMeta{
			Description: meta.Description,
			LogoURL:     meta.LogoURL,
			ServiceURL:  meta.ServiceURL,
		},
			accounts.WithMaxPinnedData(storageLimit),
		); err != nil {
			return fmt.Errorf("failed to add app account: %w", err)
		}
		return nil
	})
}
