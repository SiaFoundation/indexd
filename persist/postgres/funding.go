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

// AccountFundingInfo returns funding info grouped by quota with
// their fund target bytes.
func (s *Store) AccountFundingInfo(threshold time.Time) ([]accounts.QuotaFundInfo, error) {
	var infos []accounts.QuotaFundInfo
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		infos = infos[:0] // reuse same slice if transaction retries

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

// PoolFundingInfo returns the fund target bytes and active account count for
// each pool. Used to estimate the total contract allowance needed.
func (s *Store) PoolFundingInfo() ([]accounts.PoolFundInfo, error) {
	var infos []accounts.PoolFundInfo
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		infos = infos[:0] // reuse same slice if transaction retries

		rows, err := tx.Query(ctx, `
			SELECT q.fund_target_bytes,
				(SELECT COUNT(*) FROM accounts a WHERE a.connect_key_id = ack.id AND a.deleted_at IS NULL)
			FROM pools p
			INNER JOIN app_connect_keys ack ON ack.id = p.connect_key_id
			INNER JOIN quotas q ON q.name = ack.quota_name
		`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var info accounts.PoolFundInfo
			if err := rows.Scan(&info.FundTargetBytes, &info.ActiveAccounts); err != nil {
				return fmt.Errorf("failed to scan pool fund info: %w", err)
			}
			infos = append(infos, info)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return infos, nil
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

// HostPoolsForFunding returns up to `limit` active pools for the given host
// key that are due for funding, filtered by quota name.
func (s *Store) HostPoolsForFunding(hk types.PublicKey, quotaName string, limit int) ([]accounts.HostPool, error) {
	if limit < 0 {
		return nil, errors.New("limit can not be negative")
	} else if limit == 0 {
		return nil, nil
	}

	pools := make([]accounts.HostPool, 0, limit)
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		pools = pools[:0]  // reuse same slice if transaction retries
		remaining := limit // reset per transaction attempt

		var hostID int64
		err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)).Scan(&hostID)
		if err != nil && errors.Is(err, sql.ErrNoRows) {
			return hosts.ErrNotFound
		} else if err != nil {
			return err
		}

		newPools, err := newHostPoolsForFunding(ctx, tx, hk, hostID, quotaName, remaining)
		if err != nil {
			return fmt.Errorf("failed to query new pools for funding: %w", err)
		} else if len(newPools) >= remaining {
			pools = newPools
			return nil
		}

		remaining -= len(newPools)
		existingPools, err := existingHostPoolsForFunding(ctx, tx, hk, hostID, quotaName, remaining)
		if err != nil {
			return fmt.Errorf("failed to query existing pools for funding: %w", err)
		}

		pools = append(pools, newPools...)
		pools = append(pools, existingPools...)
		return nil
	}); err != nil {
		return nil, err
	}

	return pools, nil
}

// InsertPoolAttachments records that the given attachments have been made on
// the host.
func (s *Store) InsertPoolAttachments(hk types.PublicKey, attachments []accounts.PendingAttachment) error {
	if len(attachments) == 0 {
		return nil
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		vals := make([]string, 0, len(attachments))
		args := make([]any, 0, len(attachments)+1)
		args = append(args, sqlPublicKey(hk))
		for i, a := range attachments {
			vals = append(vals, fmt.Sprintf(`($%d::bytea)`, i+2))
			args = append(args, sqlPublicKey(a.AccountKey))
		}

		query := fmt.Sprintf(`
INSERT INTO pool_attachments (pool_id, host_id, account_id)
SELECT p.id, h.id, a.id
FROM (VALUES %s) AS vals(account_pubkey)
INNER JOIN accounts a ON a.public_key = vals.account_pubkey
INNER JOIN pools p ON p.connect_key_id = a.connect_key_id
INNER JOIN hosts h ON h.public_key = $1
ON CONFLICT DO NOTHING`, strings.Join(vals, ", "))
		_, err := tx.Exec(ctx, query, args...)
		return err
	})
}

// PendingPoolAttachments returns up to `limit` (account, pool, host) combos
// where the pool has been funded on the host but not yet attached to the
// account.
func (s *Store) PendingPoolAttachments(hk types.PublicKey, limit int) ([]accounts.PendingAttachment, error) {
	if limit <= 0 {
		return nil, nil
	}

	var pending []accounts.PendingAttachment
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		pending = pending[:0]

		rows, err := tx.Query(ctx, `
SELECT a.public_key, ack.user_secret
FROM pool_hosts ph
INNER JOIN pools p ON p.id = ph.pool_id
INNER JOIN app_connect_keys ack ON ack.id = p.connect_key_id
INNER JOIN hosts h ON h.id = ph.host_id
INNER JOIN accounts a ON a.connect_key_id = ack.id AND a.deleted_at IS NULL
LEFT JOIN pool_attachments pa ON pa.pool_id = p.id AND pa.host_id = ph.host_id AND pa.account_id = a.id
WHERE h.public_key = $1
AND pa.pool_id IS NULL
AND ph.consecutive_failed_funds = 0
LIMIT $2`, sqlPublicKey(hk), limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var pa accounts.PendingAttachment
			if err := rows.Scan((*sqlPublicKey)(&pa.AccountKey), (*sqlPoolKey)(&pa.PoolKey)); err != nil {
				return fmt.Errorf("failed to scan pending attachment: %w", err)
			}
			pending = append(pending, pa)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return pending, nil
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

// SchedulePoolsForFunding marks all pools for the given host key as due for
// funding.
func (s *Store) SchedulePoolsForFunding(hostKey types.PublicKey) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
			UPDATE pool_hosts
			SET next_fund = NOW()
			WHERE host_id = (SELECT id FROM hosts WHERE public_key = $1)
		`, sqlPublicKey(hostKey))
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

// UpdateHostPools updates the given host pools in the database.
func (s *Store) UpdateHostPools(pools []accounts.HostPool) error {
	if len(pools) == 0 {
		return nil
	} else if len(pools) > proto.MaxAccountBatchSize {
		return errors.New("too many pools to update") // sanity check batch size against max batch size used in replenish RPC
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		vals := make([]string, 0, len(pools))
		args := make([]any, 0, len(pools)*4)
		for i, pool := range pools {
			ii := i * 4
			vals = append(vals, fmt.Sprintf(`($%d::text, $%d::bytea, $%d::int, $%d::timestamptz)`, ii+1, ii+2, ii+3, ii+4))
			args = append(args,
				pool.ConnectKey,
				sqlPublicKey(pool.HostKey),
				pool.ConsecutiveFailedFunds,
				pool.NextFund,
			)
		}

		query := fmt.Sprintf(`
INSERT INTO pool_hosts (pool_id, host_id, consecutive_failed_funds, next_fund)
SELECT
	p.id AS pool_id,
	h.id AS host_id,
	vals.consecutive_failed_funds,
	vals.next_fund
FROM (VALUES %s) AS vals(connect_key, host_pubkey, consecutive_failed_funds, next_fund)
INNER JOIN app_connect_keys ack ON ack.app_key = vals.connect_key
INNER JOIN pools p ON p.connect_key_id = ack.id
INNER JOIN hosts h ON h.public_key = vals.host_pubkey
ON CONFLICT (pool_id, host_id)
DO UPDATE SET
	consecutive_failed_funds = EXCLUDED.consecutive_failed_funds,
	next_fund = EXCLUDED.next_fund;`, strings.Join(vals, ", "))
		_, err := tx.Exec(ctx, query, args...)
		return err
	})
}

func newHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT a.public_key,
	(a.pinned_data >= a.max_pinned_data) OR
	(ack.pinned_data >= q.max_pinned_data)
FROM accounts a
INNER JOIN app_connect_keys ack ON ack.id = a.connect_key_id
INNER JOIN quotas q ON q.name = ack.quota_name
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
		if err := rows.Scan((*sqlPublicKey)(&acc.AccountKey), &acc.FullStorage); err != nil {
			return nil, fmt.Errorf("failed to scan account key: %w", err)
		}
		accs = append(accs, acc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return accs, nil
}

func newHostPoolsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, quotaName string, limit int) ([]accounts.HostPool, error) {
	pools := make([]accounts.HostPool, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT ack.user_secret, ack.app_key,
	(SELECT COUNT(*) FROM accounts a WHERE a.connect_key_id = ack.id AND a.deleted_at IS NULL)
FROM pools p
INNER JOIN app_connect_keys ack ON ack.id = p.connect_key_id
LEFT JOIN pool_hosts ph ON p.id = ph.pool_id AND ph.host_id = $1
WHERE ph.pool_id IS NULL AND ack.quota_name = $3
LIMIT $2;`, hostID, limit, quotaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		pool := accounts.HostPool{HostKey: hk, NextFund: time.Now()}
		if err := rows.Scan((*sqlPoolKey)(&pool.PoolKey), &pool.ConnectKey, &pool.ActiveAccounts); err != nil {
			return nil, fmt.Errorf("failed to scan pool: %w", err)
		}
		pools = append(pools, pool)
	}
	return pools, rows.Err()
}

func existingHostAccountsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, quotaName string, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	accs := make([]accounts.HostAccount, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT a.public_key, ha.consecutive_failed_funds, ha.next_fund,
	(a.pinned_data >= a.max_pinned_data) OR
	(ack.pinned_data >= q.max_pinned_data)
FROM account_hosts ha
INNER JOIN accounts a ON a.id = ha.account_id
INNER JOIN app_connect_keys ack ON ack.id = a.connect_key_id
INNER JOIN quotas q ON q.name = ack.quota_name
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
		if err := rows.Scan((*sqlPublicKey)(&acc.AccountKey), &acc.ConsecutiveFailedFunds, &acc.NextFund, &acc.FullStorage); err != nil {
			return nil, fmt.Errorf("failed to scan account: %w", err)
		}
		accs = append(accs, acc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return accs, nil
}

func existingHostPoolsForFunding(ctx context.Context, tx *txn, hk types.PublicKey, hostID int64, quotaName string, limit int) ([]accounts.HostPool, error) {
	pools := make([]accounts.HostPool, 0, limit)

	rows, err := tx.Query(ctx, `
SELECT ack.user_secret, ack.app_key, ph.consecutive_failed_funds, ph.next_fund,
	(SELECT COUNT(*) FROM accounts a WHERE a.connect_key_id = ack.id AND a.deleted_at IS NULL)
FROM pool_hosts ph
INNER JOIN pools p ON p.id = ph.pool_id
INNER JOIN app_connect_keys ack ON ack.id = p.connect_key_id
WHERE ph.host_id = $1 AND ph.next_fund <= NOW() AND ack.quota_name = $3
ORDER BY ph.next_fund ASC
LIMIT $2`, hostID, limit, quotaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		pool := accounts.HostPool{HostKey: hk}
		if err := rows.Scan((*sqlPoolKey)(&pool.PoolKey), &pool.ConnectKey, &pool.ConsecutiveFailedFunds, &pool.NextFund, &pool.ActiveAccounts); err != nil {
			return nil, fmt.Errorf("failed to scan pool: %w", err)
		}
		pools = append(pools, pool)
	}
	return pools, rows.Err()
}
