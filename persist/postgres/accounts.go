package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
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
			SELECT a.public_key, ak.app_key, a.max_pinned_data, a.pinned_data, a.app_id, a.description, a.logo_url, a.service_url, a.last_used
			FROM accounts a
			INNER JOIN app_connect_keys ak ON ak.id = a.connect_key_id
			WHERE a.deleted_at IS NULL AND
			($1::integer IS NULL OR connect_key_id = $1::integer)
			ORDER BY a.id
			LIMIT $2 OFFSET $3
		`, connectKeyID, limit, offset)
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
		account, err = scanAccount(tx.QueryRow(ctx, `SELECT a.public_key, ak.app_key, a.max_pinned_data, a.pinned_data, a.app_id, a.description, a.logo_url, a.service_url, a.last_used
FROM accounts a
INNER JOIN app_connect_keys ak ON ak.id = a.connect_key_id
WHERE public_key = $1`, sqlPublicKey(ak)))
		return err
	})
	return account, err
}

// HasAccount checks if the account with the given public key exists in the
// database.
func (s *Store) HasAccount(ak types.PublicKey) (bool, error) {
	var exists bool
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE public_key = $1)`, sqlPublicKey(ak)).Scan(&exists)
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

// UpdateAccount updates the account in the database with given old account key
// to the new account key, allowing the user to rotate his account key.
func (s *Store) UpdateAccount(oldAK, newAK types.PublicKey) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `UPDATE accounts SET public_key = $1 WHERE public_key = $2`, sqlPublicKey(newAK), sqlPublicKey(oldAK))
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
				return accounts.ErrExists
			}
			return fmt.Errorf("failed to update account: %w", err)
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

// HostAccountsForFunding returns up to `limit` active (after the `threshold`
// time) accounts for the given host key that are due for funding, filtered by
// quota name.
func (s *Store) HostAccountsForFunding(hk types.PublicKey, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	if limit < 0 {
		return nil, errors.New("limit can not be negative")
	} else if limit == 0 {
		return nil, nil
	}

	accs := make([]accounts.HostAccount, 0, limit)
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		accs = accs[:0]    // reuse same slice if transaction retries
		remaining := limit // reset per transaction attempt

		var hostID int64
		err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)).Scan(&hostID)
		if err != nil && errors.Is(err, sql.ErrNoRows) {
			return hosts.ErrNotFound
		} else if err != nil {
			return err
		}

		newAccs, err := newHostAccountsForFunding(ctx, tx, hk, hostID, quotaName, threshold, remaining)
		if err != nil {
			return fmt.Errorf("failed to query new accounts for funding: %w", err)
		} else if len(newAccs) >= remaining {
			accs = newAccs
			return nil
		}

		remaining -= len(newAccs)
		existingAccs, err := existingHostAccountsForFunding(ctx, tx, hk, hostID, quotaName, threshold, remaining)
		if err != nil {
			return fmt.Errorf("failed to query existing accounts for funding: %w", err)
		}

		accs = append(accs, newAccs...)
		accs = append(accs, existingAccs...)
		return nil
	}); err != nil {
		return nil, err
	}

	return accs, nil
}

// ScheduleAccountsForFunding marks all accounts for the given host key as due
// for funding.
func (s *Store) ScheduleAccountsForFunding(hostKey types.PublicKey) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
			UPDATE account_hosts
			SET next_fund = NOW()
			WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)
		`, sqlPublicKey(hostKey))
		return err
	})
}

// ScheduleAccountForFunding marks the given account for the given host key as
// due for funding.
func (s *Store) ScheduleAccountForFunding(hostKey types.PublicKey, account proto.Account) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
			UPDATE account_hosts
			SET next_fund = '1970-01-01 00:00:00+00' -- make sure it's at the front of the queue
			WHERE account_id = (SELECT id FROM accounts WHERE public_key = $1)
			AND host_id = (SELECT id FROM hosts WHERE public_key = $2)
		`, sqlPublicKey(account), sqlPublicKey(hostKey))
		return err
	})
}

// UpdateHostAccounts updates the given host accounts in the database.
func (s *Store) UpdateHostAccounts(accounts []accounts.HostAccount) error {
	if len(accounts) == 0 {
		return nil
	} else if len(accounts) > proto.MaxAccountBatchSize {
		return errors.New("too many accounts to update") // sanity check batch size against max batch size used in replenish RPC
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		vals := make([]string, 0, len(accounts))
		args := make([]any, 0, len(accounts)*4)
		for i, account := range accounts {
			ii := i * 4
			vals = append(vals, fmt.Sprintf(`($%d::bytea, $%d::bytea, $%d::int, $%d::timestamptz)`, ii+1, ii+2, ii+3, ii+4))
			args = append(args,
				sqlPublicKey(account.AccountKey),
				sqlPublicKey(account.HostKey),
				account.ConsecutiveFailedFunds,
				account.NextFund,
			)
		}

		query := fmt.Sprintf(`
INSERT INTO account_hosts (account_id, host_id, consecutive_failed_funds, next_fund)
SELECT
	a.id AS account_id,
	h.id AS host_id,
	vals.consecutive_failed_funds,
	vals.next_fund
FROM (VALUES %s) AS vals(account_pubkey, host_pubkey, consecutive_failed_funds, next_fund)
INNER JOIN accounts a ON a.public_key = vals.account_pubkey
INNER JOIN hosts h ON h.public_key = vals.host_pubkey
ON CONFLICT (account_id, host_id)
DO UPDATE SET
	consecutive_failed_funds = EXCLUDED.consecutive_failed_funds,
	next_fund = EXCLUDED.next_fund;`, strings.Join(vals, ", "))
		_, err := tx.Exec(ctx, query, args...)
		return err
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

	res, err := tx.Exec(ctx, `INSERT INTO accounts (public_key, connect_key_id, max_pinned_data, app_id, description, logo_url, service_url) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`, sqlPublicKey(account), connectKeyID, aao.MaxPinnedData, sqlHash256(meta.ID), meta.Description, meta.LogoURL, meta.ServiceURL)
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

// AccountFundingInfo returns funding info grouped by quota with
// their fund target bytes.
func (s *Store) AccountFundingInfo(threshold time.Time) ([]accounts.QuotaFundInfo, error) {
	var infos []accounts.QuotaFundInfo
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT q.name, q.fund_target_bytes, COUNT(*) as active_count
			FROM accounts a
			INNER JOIN app_connect_keys ack ON ack.id = a.connect_key_id
			INNER JOIN quotas q ON q.name = ack.quota_name
			WHERE a.last_used >= $1 AND a.deleted_at IS NULL
			GROUP BY q.name, q.fund_target_bytes
		`, threshold)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var info accounts.QuotaFundInfo
			if err := rows.Scan(&info.QuotaName, &info.FundTargetBytes, &info.ActiveAccounts); err != nil {
				return fmt.Errorf("failed to scan quota fund info: %w", err)
			}
			infos = append(infos, info)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return infos, nil
}

func newHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT a.public_key
FROM accounts a
INNER JOIN app_connect_keys ack ON ack.id = a.connect_key_id
LEFT JOIN account_hosts ah ON a.id = ah.account_id AND ah.host_id = $1
WHERE ah.account_id IS NULL AND a.deleted_at IS NULL AND (a.last_used >= $2)
AND ack.quota_name = $4
LIMIT $3;`, hostID, threshold, limit, quotaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		acc := accounts.HostAccount{HostKey: hk, NextFund: time.Now()}
		if err := rows.Scan((*sqlPublicKey)(&acc.AccountKey)); err != nil {
			return nil, fmt.Errorf("failed to scan account key: %w", err)
		}
		accs = append(accs, acc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return accs, nil
}

func existingHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT a.public_key, ha.consecutive_failed_funds, ha.next_fund
FROM account_hosts ha
INNER JOIN accounts a ON a.id = ha.account_id
INNER JOIN app_connect_keys ack ON ack.id = a.connect_key_id
WHERE ha.host_id = $1 AND ha.next_fund <= NOW() AND a.deleted_at IS NULL AND (a.last_used >= $2)
AND ack.quota_name = $4
ORDER BY ha.next_fund ASC
LIMIT $3`, hostID, threshold, limit, quotaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		acc := accounts.HostAccount{HostKey: hk}
		if err := rows.Scan((*sqlPublicKey)(&acc.AccountKey), &acc.ConsecutiveFailedFunds, &acc.NextFund); err != nil {
			return nil, fmt.Errorf("failed to scan account: %w", err)
		}
		accs = append(accs, acc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return accs, nil
}

func scanAccount(s scanner) (account accounts.Account, err error) {
	err = s.Scan((*sqlPublicKey)(&account.AccountKey), &account.ConnectKey, &account.MaxPinnedData, &account.PinnedData, (*sqlHash256)(&account.App.ID), &account.App.Description, &account.App.LogoURL, &account.App.ServiceURL, &account.LastUsed)
	return
}
