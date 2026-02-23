package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/indexd/accounts"
)

func scanQuota(s scanner) (quota accounts.Quota, err error) {
	err = s.Scan(&quota.Key, &quota.Description, &quota.MaxPinnedData, &quota.TotalUses, &quota.FundTargetBytes)
	return
}

// PutQuota creates or updates a quota in the database.
func (s *Store) PutQuota(key string, req accounts.PutQuotaRequest) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO quotas (name, description, max_pinned_data, total_uses, fund_target_bytes)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (name) DO UPDATE SET
				description = EXCLUDED.description,
				max_pinned_data = EXCLUDED.max_pinned_data,
				total_uses = EXCLUDED.total_uses,
				fund_target_bytes = EXCLUDED.fund_target_bytes
		`, key, req.Description, req.MaxPinnedData, req.TotalUses, req.FundTargetBytes)
		return err
	})
}

// DeleteQuota deletes a quota from the database.
// If the quota does not exist, it returns [accounts.ErrQuotaNotFound].
// If the quota is in use by connect keys, it returns [accounts.ErrQuotaInUse].
func (s *Store) DeleteQuota(key string) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		// check if quota exists
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM quotas WHERE name = $1)`, key).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check quota: %w", err)
		} else if !exists {
			return accounts.ErrQuotaNotFound
		}

		// check if quota is in use
		var inUse bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_connect_keys WHERE quota_name = $1)`, key).Scan(&inUse); err != nil {
			return fmt.Errorf("failed to check if quota in use: %w", err)
		} else if inUse {
			return accounts.ErrQuotaInUse
		}

		_, err := tx.Exec(ctx, `DELETE FROM quotas WHERE name = $1`, key)
		return err
	})
}

// Quota retrieves a quota from the database.
// If the quota does not exist, it returns [accounts.ErrQuotaNotFound].
func (s *Store) Quota(key string) (quota accounts.Quota, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		quota, err = scanQuota(tx.QueryRow(ctx, `
			SELECT name, description, max_pinned_data, total_uses, fund_target_bytes
			FROM quotas
			WHERE name = $1
		`, key))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrQuotaNotFound
		}
		return err
	})
	return
}

// Quotas retrieves a list of quotas from the database.
func (s *Store) Quotas(offset, limit int) (quotas []accounts.Quota, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		quotas = quotas[:0] // reset in case of retry
		rows, err := tx.Query(ctx, `
			SELECT name, description, max_pinned_data, total_uses, fund_target_bytes
			FROM quotas
			ORDER BY name
			LIMIT $1 OFFSET $2
		`, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			quota, err := scanQuota(rows)
			if err != nil {
				return err
			}
			quotas = append(quotas, quota)
		}
		return rows.Err()
	})
	return
}
