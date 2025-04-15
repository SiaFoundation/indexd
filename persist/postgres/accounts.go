package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
)

// Accounts returns a list of account keys.
func (s *Store) Accounts(ctx context.Context, offset, limit int) ([]types.PublicKey, error) {
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var accs []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, `SELECT public_key FROM accounts LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query accounts: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var ak types.PublicKey
			if err := rows.Scan((*sqlPublicKey)(&ak)); err != nil {
				return fmt.Errorf("failed to scan account key: %w", err)
			}
			accs = append(accs, ak)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return accs, nil
}

// AddAccount adds a new account in the database with given account key.
func (s *Store) AddAccount(ctx context.Context, ak types.PublicKey) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `INSERT INTO accounts (public_key) VALUES ($1) ON CONFLICT DO NOTHING`, sqlPublicKey(ak))
		if err != nil {
			return fmt.Errorf("failed to add account: %w", err)
		} else if res.RowsAffected() == 0 {
			return accounts.ErrExists
		}
		return nil
	})
}

// HasAccount checks if the account with the given public key exists in the
// database.
func (s *Store) HasAccount(ctx context.Context, ak types.PublicKey) (bool, error) {
	var exists bool
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE public_key = $1)`, sqlPublicKey(ak)).Scan(&exists)
	}); err != nil {
		return false, fmt.Errorf("failed to check if account exists: %w", err)
	}
	return exists, nil
}

// DeleteAccount deletes the account in the database with given account key.
func (s *Store) DeleteAccount(ctx context.Context, ak types.PublicKey) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `DELETE FROM accounts WHERE public_key = $1`, sqlPublicKey(ak))
		if err != nil {
			return fmt.Errorf("failed to delete account: %w", err)
		} else if res.RowsAffected() != 1 {
			return accounts.ErrNotFound
		}
		return nil
	})
}

// HostAccountsForFunding returns up to limit accounts for the given host key
// that are due for funding.
func (s *Store) HostAccountsForFunding(ctx context.Context, hk types.PublicKey, limit int) ([]accounts.HostAccount, error) {
	if limit < 0 {
		return nil, errors.New("limit can not be negative")
	} else if limit == 0 {
		return nil, nil
	}

	accs := make([]accounts.HostAccount, 0, limit)
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)).Scan(&hostID)
		if err != nil && errors.Is(err, sql.ErrNoRows) {
			return hosts.ErrNotFound
		} else if err != nil {
			return err
		}

		newAccs, err := s.newHostAccountsForFunding(ctx, tx, hk, hostID, limit)
		if err != nil {
			return fmt.Errorf("failed to query new accounts for funding: %w", err)
		} else if len(newAccs) >= limit {
			accs = newAccs
			return nil
		}

		limit -= len(newAccs)
		existingAccs, err := s.existingHostAccountsForFunding(ctx, tx, hk, hostID, limit)
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

// UpdateHostAccounts updates the given host accounts in the database.
func (s *Store) UpdateHostAccounts(ctx context.Context, accounts []accounts.HostAccount) error {
	if len(accounts) == 0 {
		return nil
	} else if len(accounts) > proto.MaxAccountBatchSize {
		return errors.New("too many accounts to update") // sanity check batch size against max batch size used in replenish RPC
	}

	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
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
	})
}

func (s *Store) newHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT a.public_key 
FROM accounts a LEFT JOIN account_hosts ah ON a.id = ah.account_id AND ah.host_id = $1 
WHERE ah.account_id IS NULL 
LIMIT $2;`, hostID, limit)
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

func (s *Store) existingHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT public_key, consecutive_failed_funds, next_fund
FROM account_hosts ha 
INNER JOIN accounts a ON a.id = ha.account_id 
WHERE ha.host_id = $1 AND ha.next_fund <= NOW() 
ORDER BY next_fund ASC 
LIMIT $2`, hostID, limit)
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
