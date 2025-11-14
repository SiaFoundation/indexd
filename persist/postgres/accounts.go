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

	queryOpts := accounts.QueryAccountsOptions{
		ServiceAccount: nil, // default to all accounts
		ConnectKey:     nil, // default to all accounts
	}
	for _, opt := range opts {
		opt(&queryOpts)
	}

	if err := s.transaction(func(ctx context.Context, tx *txn) (err error) {
		var connectKeyID sql.NullInt64
		if queryOpts.ConnectKey != nil {
			if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, *queryOpts.ConnectKey).Scan(&connectKeyID); errors.Is(err, sql.ErrNoRows) {
				return accounts.ErrKeyNotFound
			} else if err != nil {
				return fmt.Errorf("failed to get connect key ID: %w", err)
			}
		}

		rows, err := tx.Query(ctx, `
			SELECT a.public_key, ak.app_key, a.service_account, a.max_pinned_data, a.pinned_data, a.description, a.logo_url, a.service_url, a.last_used
			FROM accounts a
			LEFT JOIN app_connect_keys ak ON ak.id = a.connect_key_id
			WHERE ($1::boolean IS NULL OR service_account = $1::boolean) AND
			($2::integer IS NULL OR connect_key_id = $2::integer)
			LIMIT $3 OFFSET $4
		`, queryOpts.ServiceAccount, connectKeyID, limit, offset)
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
		account, err = scanAccount(tx.QueryRow(ctx, `SELECT a.public_key, ak.app_key, a.service_account, a.max_pinned_data, a.pinned_data, a.description, a.logo_url, a.service_url, a.last_used
FROM accounts a
LEFT JOIN app_connect_keys ak ON ak.id = a.connect_key_id
WHERE public_key = $1`, sqlPublicKey(ak)))
		return err
	})
	return account, err
}

// AddServiceAccount adds a new service account in the database with given
// account key.
func (s *Store) AddServiceAccount(ak types.PublicKey, meta accounts.AccountMeta, opts ...accounts.AddAccountOption) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		return addAccount(ctx, tx, nil, ak, true, meta, opts...)
	})
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

func activeAccounts(ctx context.Context, tx *txn, threshold time.Time) (count uint64, err error) {
	err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM accounts WHERE last_used >= $1;`, threshold).Scan(&count)
	return
}

// ActiveAccounts returns the number of accounts that have been used since the threshold
// time.
func (s *Store) ActiveAccounts(threshold time.Time) (count uint64, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) (err error) {
		count, err = activeAccounts(ctx, tx, threshold)
		return
	})
	return
}

// DeleteAccount deletes the account in the database with given account key.
func (s *Store) DeleteAccount(ak types.PublicKey) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var serviceAccount bool
		err := tx.QueryRow(ctx, `DELETE FROM accounts WHERE public_key = $1 RETURNING service_account`, sqlPublicKey(ak)).Scan(&serviceAccount)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return fmt.Errorf("failed to delete account: %w", err)
		} else if serviceAccount {
			return accounts.ErrServiceAccount
		} else if err := incrementNumAccounts(ctx, tx, -1); err != nil {
			return fmt.Errorf("failed to decrement registered accounts: %w", err)
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

// HostAccountsForFunding returns up to `limit` active (after the `threshold`
// time) accounts for the given host key that are due for funding.
func (s *Store) HostAccountsForFunding(hk types.PublicKey, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	if limit < 0 {
		return nil, errors.New("limit can not be negative")
	} else if limit == 0 {
		return nil, nil
	}

	accs := make([]accounts.HostAccount, 0, limit)
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)).Scan(&hostID)
		if err != nil && errors.Is(err, sql.ErrNoRows) {
			return hosts.ErrNotFound
		} else if err != nil {
			return err
		}

		newAccs, err := newHostAccountsForFunding(ctx, tx, hk, hostID, threshold, limit)
		if err != nil {
			return fmt.Errorf("failed to query new accounts for funding: %w", err)
		} else if len(newAccs) >= limit {
			accs = newAccs
			return nil
		}

		limit -= len(newAccs)
		existingAccs, err := existingHostAccountsForFunding(ctx, tx, hk, hostID, threshold, limit)
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

// DebitServiceAccount withdraws from a service account. The balance of the
// account can't underflow, instead it will be set to 0 if the amount withdrawn
// exceeds the stored balance.
func (s *Store) DebitServiceAccount(hostKey types.PublicKey, account proto.Account, amount types.Currency) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		resp, err := tx.Exec(ctx, `
			UPDATE service_accounts
			SET balance = GREATEST(balance - $1, 0)
			WHERE account_id = (SELECT id FROM accounts WHERE public_key = $2)
			AND host_id = (SELECT id FROM hosts WHERE public_key = $3)
		`, sqlCurrency(amount), sqlPublicKey(account), sqlPublicKey(hostKey))
		if err != nil {
			return err
		} else if resp.RowsAffected() == 0 {
			return accounts.ErrNotFound
		}
		return nil
	})
}

// UpdateServiceAccountBalance updates the balance of a service account.
func (s *Store) UpdateServiceAccountBalance(hostKey types.PublicKey, account proto.Account, balance types.Currency) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO service_accounts (account_id, host_id, balance)
			VALUES (
				(SELECT id FROM accounts WHERE public_key = $1),
				(SELECT id FROM hosts WHERE public_key = $2),
				$3
			)
			ON CONFLICT (account_id, host_id) DO UPDATE SET balance = EXCLUDED.balance
		`, sqlPublicKey(account), sqlPublicKey(hostKey), sqlCurrency(balance))
		return err
	})
}

// ServiceAccountBalance returns the balance of a service account.
func (s *Store) ServiceAccountBalance(hostKey types.PublicKey, account proto.Account) (types.Currency, error) {
	var balance types.Currency
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `
			SELECT balance
			FROM service_accounts
			INNER JOIN accounts ON accounts.id = service_accounts.account_id
			INNER JOIN hosts ON hosts.id = service_accounts.host_id
			WHERE accounts.public_key = $1 AND hosts.public_key = $2
		`, sqlPublicKey(account), sqlPublicKey(hostKey)).Scan((*sqlCurrency)(&balance))
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		}
		return err
	})
	return balance, err
}

func addAccount(ctx context.Context, tx *txn, connectKey *string, account types.PublicKey, serviceAccount bool, meta accounts.AccountMeta, opts ...accounts.AddAccountOption) error {
	aao := accounts.AddAccountOptions{
		MaxPinnedData: math.MaxInt64, // no limit by default
	}
	for _, opt := range opts {
		opt(&aao)
	}

	var connectKeyID sql.NullInt64
	if connectKey != nil {
		if err := tx.QueryRow(ctx, `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey).Scan(&connectKeyID); errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrKeyNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get app connect key ID: %w", err)
		}
	}

	res, err := tx.Exec(ctx, `INSERT INTO accounts (public_key, connect_key_id, service_account, max_pinned_data, description, logo_url, service_url) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`, sqlPublicKey(account), connectKeyID, serviceAccount, aao.MaxPinnedData, meta.Description, meta.LogoURL, meta.ServiceURL)
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

func newHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT a.public_key
FROM accounts a
LEFT JOIN account_hosts ah ON a.id = ah.account_id AND ah.host_id = $1
WHERE ah.account_id IS NULL AND (a.last_used >= $2 OR a.service_account = TRUE)
LIMIT $3;`, hostID, threshold, limit)
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

func existingHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT public_key, consecutive_failed_funds, next_fund
FROM account_hosts ha
INNER JOIN accounts a ON a.id = ha.account_id
WHERE ha.host_id = $1 AND ha.next_fund <= NOW() AND (a.last_used >= $2 OR a.service_account = TRUE)
ORDER BY next_fund ASC
LIMIT $3`, hostID, threshold, limit)
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
	var connectKey sql.NullString
	err = s.Scan((*sqlPublicKey)(&account.AccountKey), &connectKey, &account.ServiceAccount, &account.MaxPinnedData, &account.PinnedData, &account.Description, &account.LogoURL, &account.ServiceURL, &account.LastUsed)
	if connectKey.Valid {
		account.ConnectKey = &connectKey.String
	}
	return
}
