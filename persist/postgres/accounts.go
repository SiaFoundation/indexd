package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
)

// Accounts returns a list of account keys.
func (s *Store) Accounts(offset, limit int, opts ...accounts.QueryAccountsOpt) (accs []accounts.Account, err error) {
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var queryOpts accounts.QueryAccountsOptions
	for _, opt := range opts {
		opt(&queryOpts)
	}

	if err := s.transaction(func(ctx context.Context, tx *txn) (err error) {
		accs = accs[:0] // reuse same slice if transaction retries

		var connectKeyID sql.NullInt64
		if queryOpts.ConnectKey != nil {
			if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, *queryOpts.ConnectKey).Scan(&connectKeyID); errors.Is(err, sql.ErrNoRows) {
				return accounts.ErrKeyNotFound
			} else if err != nil {
				return fmt.Errorf("failed to get connect key ID: %w", err)
			}
		}

		rows, err := tx.Query(ctx, `
			SELECT a.public_key, ak.app_key, a.max_pinned_data, q.max_pinned_data, ak.pinned_data, a.pinned_data, a.pinned_size, COALESCE(ahr.ready_hosts, 0) >= $4, a.app_id, a.name, a.description, a.logo_url, a.service_url, a.last_used
			FROM accounts a
			INNER JOIN app_connect_keys ak ON ak.id = a.connect_key_id
			INNER JOIN quotas q ON q.name = ak.quota_name
			LEFT JOIN LATERAL (
				SELECT COUNT(*) AS ready_hosts
				FROM (
					SELECT 1
					FROM account_hosts ah
					WHERE ah.account_id = a.id
					  AND ah.consecutive_failed_funds = 0
					LIMIT $4
				) ready
			) ahr ON TRUE
			WHERE a.deleted_at IS NULL AND
			($1::integer IS NULL OR connect_key_id = $1::integer)
			ORDER BY a.id
			LIMIT $2 OFFSET $3
			`, connectKeyID, limit, offset, accounts.ReadyHostThreshold)
		if err != nil {
			return fmt.Errorf("failed to query accounts: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			account, err := scanAccount(rows)
			if err != nil {
				return fmt.Errorf("failed to scan account key: %w", err)
			}
			accs = append(accs, account)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return accs, nil
}

// Account returns information about the account with the given public key.
func (s *Store) Account(ak types.PublicKey) (accounts.Account, error) {
	var account accounts.Account
	account.AccountKey = proto.Account(ak) // no need to fetch key
	err := s.transaction(func(ctx context.Context, tx *txn) (err error) {
		account, err = scanAccount(tx.QueryRow(ctx, `SELECT a.public_key, ak.app_key, a.max_pinned_data, q.max_pinned_data, ak.pinned_data, a.pinned_data, a.pinned_size, COALESCE(ahr.ready_hosts, 0) >= $2, a.app_id, a.name, a.description, a.logo_url, a.service_url, a.last_used
FROM accounts a
INNER JOIN app_connect_keys ak ON ak.id = a.connect_key_id
INNER JOIN quotas q ON q.name = ak.quota_name
LEFT JOIN LATERAL (
	SELECT COUNT(*) AS ready_hosts
	FROM (
		SELECT 1
		FROM account_hosts ah
		WHERE ah.account_id = a.id
		  AND ah.consecutive_failed_funds = 0
		LIMIT $2
	) ready
) ahr ON TRUE
WHERE public_key = $1`, sqlPublicKey(ak), accounts.ReadyHostThreshold))
		return err
	})
	return account, err
}

// HasAccount reports whether an account with the given public key exists and
// has not been soft-deleted. As a side effect, when the account exists its
// last_used timestamp is bumped to NOW() so we can track active accounts.
func (s *Store) HasAccount(ak types.PublicKey) (bool, error) {
	var exists bool
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `UPDATE accounts SET last_used = NOW() WHERE public_key = $1 AND deleted_at IS NULL RETURNING TRUE`, sqlPublicKey(ak)).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}); err != nil {
		return false, fmt.Errorf("failed to check if account exists: %w", err)
	}
	return exists, nil
}

// DeleteAccount deletes the account in the database with given account key.
func (s *Store) DeleteAccount(acc proto.Account) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `UPDATE accounts SET deleted_at = NOW() WHERE public_key = $1 AND deleted_at IS NULL`, sqlPublicKey(acc))
		if err != nil {
			return fmt.Errorf("failed to delete account: %w", err)
		} else if res.RowsAffected() == 0 {
			return accounts.ErrNotFound
		}
		return nil
	})
}

// UpdateAccount updates the given account with any non-null fields provided.
func (s *Store) UpdateAccount(ak types.PublicKey, updates accounts.UpdateAccountRequest) error {
	if updates.MaxPinnedData == nil {
		return nil // no changes
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `UPDATE accounts SET max_pinned_data = $1 WHERE public_key = $2 AND deleted_at IS NULL`, *updates.MaxPinnedData, sqlPublicKey(ak))
		if err != nil {
			return fmt.Errorf("failed to update max pinned data: %w", err)
		} else if res.RowsAffected() != 1 {
			return accounts.ErrNotFound
		}
		return nil
	})
}

// PruneAccounts deletes up to `limit` combined slabs and objects from an
// account that has been soft deleted.  If there are no objects left on the
// account to delete, it will prune the associated slabs and sectors.  If there
// are no slabs left it will hard delete the account.  If there are no pending
// soft deleted accounts, accounts.ErrNotFound is returned
func (s *Store) PruneAccounts(limit int) error {
	if limit < 0 {
		return errors.New("limit can not be negative")
	}

	return s.transaction(func(ctx context.Context, tx *txn) error {
		remaining := limit // reset per transaction attempt

		var accountID int64
		err := tx.QueryRow(ctx, `SELECT id FROM accounts WHERE deleted_at IS NOT NULL ORDER by deleted_at LIMIT 1`).Scan(&accountID)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return fmt.Errorf("failed to find an account to delete: %w", err)
		}

		rows, err := tx.Query(ctx, `DELETE FROM objects o
USING (
	SELECT id
	FROM objects
	WHERE account_id = $1
	ORDER BY id
	LIMIT $2
) d
WHERE o.id = d.id
RETURNING o.object_key;`, accountID, remaining)
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		var objKeys []sqlHash256
		for rows.Next() {
			var objKey sqlHash256
			if err := rows.Scan(&objKey); err != nil {
				return fmt.Errorf("failed to scan object key: %w", err)
			}
			objKeys = append(objKeys, objKey)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to get rows: %w", err)
		}

		remaining -= len(objKeys)
		if remaining == 0 {
			return nil
		}

		rows, err = tx.Query(ctx, `SELECT slab_id FROM account_slabs WHERE account_id = $1 ORDER BY slab_id LIMIT $2`, accountID, remaining)
		if err != nil {
			return fmt.Errorf("failed to get account slabs: %w", err)
		}
		defer rows.Close()

		var slabIDs []int64
		for rows.Next() {
			var slabID int64
			if err := rows.Scan(&slabID); err != nil {
				return fmt.Errorf("failed to get slab ID: %w", err)
			}
			slabIDs = append(slabIDs, slabID)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to get account slabs: %w", err)
		}

		if err := s.unpinSlabs(ctx, tx, accountID, slabIDs); err != nil {
			return fmt.Errorf("failed to unpin slabs: %w", err)
		}

		if len(slabIDs) < remaining {
			// no slabs left, we can delete the account
			_, err = tx.Exec(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
			if err != nil {
				return fmt.Errorf("failed to delete account: %w", err)
			}

			err = incrementNumAccounts(ctx, tx, -1)
			if err != nil {
				return fmt.Errorf("failed to decrement account count: %w", err)
			}
		}

		return nil
	})
}

func addAccount(ctx context.Context, tx *txn, connectKey string, account types.PublicKey, meta accounts.AppMeta, opts ...accounts.AddAccountOption) error {
	aao := accounts.AddAccountOptions{
		MaxPinnedData: math.MaxInt64, // no limit by default
	}
	for _, opt := range opts {
		opt(&aao)
	}

	var connectKeyID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey).Scan(&connectKeyID); errors.Is(err, sql.ErrNoRows) {
		return accounts.ErrKeyNotFound
	} else if err != nil {
		return fmt.Errorf("failed to get app connect key ID: %w", err)
	}

	res, err := tx.Exec(ctx, `INSERT INTO accounts (public_key, connect_key_id, max_pinned_data, app_id, name, description, logo_url, service_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`, sqlPublicKey(account), connectKeyID, aao.MaxPinnedData, sqlHash256(meta.ID), meta.Name, meta.Description, meta.LogoURL, meta.ServiceURL)
	if err != nil {
		return fmt.Errorf("failed to add account: %w", err)
	} else if res.RowsAffected() == 0 {
		return accounts.ErrExists
	}
	if err := incrementNumAccounts(ctx, tx, 1); err != nil {
		return fmt.Errorf("failed to increment registered accounts: %w", err)
	}
	return nil
}

func scanAccount(s scanner) (account accounts.Account, err error) {
	err = s.Scan(
		(*sqlPublicKey)(&account.AccountKey),
		&account.ConnectKey,
		&account.MaxPinnedData,
		&account.QuotaMaxPinnedData,
		&account.ConnectKeyPinnedData,
		&account.PinnedData,
		&account.PinnedSize,
		&account.Ready,
		(*sqlHash256)(&account.App.ID),
		&account.App.Name,
		&account.App.Description,
		&account.App.LogoURL,
		&account.App.ServiceURL,
		&account.LastUsed,
	)
	return
}
