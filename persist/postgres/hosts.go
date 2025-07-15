package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
)

const (
	// uptimeHalfLife defines the duration, in seconds, over which a host's
	// recorded uptime value is exponentially decayed to half its influence. In
	// practice, after uptimeHalfLife seconds, the contribution of a single scan
	// (successful or failed) to the host's "recent uptime" metric is reduced by
	// 50%. This decay mechanism emphasizes recent scan results, giving new
	// hosts a fairer evaluation and ensuring that long-established hosts do not
	// maintain high uptime scores solely based on older data.
	uptimeHalfLife = 60 * 60 * 24 * 7 * 12 // 3 months
)

type dbHost struct {
	id int64
	hosts.Host
}

func (u *updateTx) AddHostAnnouncement(hk types.PublicKey, ha chain.V2HostAnnouncement, ts time.Time) error {
	var hostID int64
	err := u.tx.QueryRow(u.ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, $2) ON CONFLICT (public_key) DO UPDATE SET last_announcement = $2 RETURNING id;`, sqlPublicKey(hk), ts).Scan(&hostID)
	if err != nil {
		return err
	}

	_, err = u.tx.Exec(u.ctx, `DELETE FROM host_addresses WHERE host_id = $1`, hostID)
	if err != nil {
		return err
	}

	for _, na := range ha {
		_, err = u.tx.Exec(u.ctx, `INSERT INTO host_addresses (host_id, net_address, protocol) VALUES ($1, $2, $3)`, hostID, na.Address, sqlNetworkProtocol(na.Protocol))
		if err != nil {
			return fmt.Errorf("failed to insert host address: %w", err)
		}
	}

	return nil
}

// Host returns the host for given public key
func (s *Store) Host(ctx context.Context, hk types.PublicKey) (hosts.Host, error) {
	var host hosts.Host
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		dbHost, err := scanHost(tx.QueryRow(ctx, `
WITH globals AS (
	SELECT
		contracts_period,
		hosts_min_collateral,
		hosts_max_storage_price,
		hosts_max_ingress_price,
		hosts_max_egress_price,
		(get_byte(hosts_min_protocol_version, 0) << 16) + (get_byte(hosts_min_protocol_version, 1) << 8) + (get_byte(hosts_min_protocol_version, 2)) AS host_min_version,
		250000::NUMERIC AS sectors_per_tb,
		1E12::NUMERIC AS one_tb,
		1E24::NUMERIC AS one_sc
	FROM global_settings
), hosts AS (
	SELECT
		id, hosts.public_key, last_announcement, hb.public_key IS NOT NULL AS blocked, COALESCE(hb.reason, ''), lost_sectors,
		last_failed_scan, last_successful_scan, next_scan, consecutive_failed_scans, recent_uptime,
		settings_protocol_version, settings_release, settings_wallet_address,
		settings_accepting_contracts, settings_max_collateral, settings_max_contract_duration,
		settings_remaining_storage, settings_total_storage, settings_contract_price,
		settings_collateral, settings_storage_price, settings_ingress_price,
		settings_egress_price, settings_free_sector_price, settings_tip_height, settings_valid_until, settings_signature,
		last_successful_scan IS NOT NULL as has_settings,
		(get_byte(settings_protocol_version, 0) << 16) + (get_byte(settings_protocol_version, 1) << 8) + (get_byte(settings_protocol_version, 2)) as settings_version
	FROM hosts
	LEFT JOIN hosts_blocklist hb ON hosts.public_key = hb.public_key
	WHERE hosts.public_key = $1
) SELECT
	hosts.*,
	recent_uptime >= 0.9,
	has_settings AND settings_max_contract_duration >= globals.contracts_period,
	has_settings AND settings_max_collateral >= settings_collateral * globals.one_tb * globals.contracts_period,
	has_settings AND settings_version >= globals.host_min_version,
	has_settings AND settings_valid_until >= (NOW() + INTERVAL '15 minutes'),
	has_settings AND settings_accepting_contracts,
	has_settings AND settings_contract_price <= globals.one_sc,
	has_settings AND settings_collateral >= globals.hosts_min_collateral AND settings_collateral >= 2 * settings_storage_price,
	has_settings AND settings_storage_price <= globals.hosts_max_storage_price,
	has_settings AND settings_ingress_price <= globals.hosts_max_ingress_price,
	has_settings AND settings_egress_price <= globals.hosts_max_egress_price,
	has_settings AND settings_free_sector_price <= globals.one_sc / globals.sectors_per_tb
FROM hosts CROSS JOIN globals;`, sqlPublicKey(hk)))
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hk, hosts.ErrNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to query host: %w", err)
		}

		if err := decorateHostAddresses(ctx, tx, &dbHost); err != nil {
			return fmt.Errorf("failed to decorate host addresses: %w", err)
		} else if err := decorateHostNetworks(ctx, tx, &dbHost); err != nil {
			return fmt.Errorf("failed to decorate host networks: %w", err)
		}

		host = dbHost.Host
		return nil
	}); err != nil {
		return hosts.Host{}, err
	}
	return host, nil
}

// Hosts returns a list of hosts.
func (s *Store) Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error) {
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}
	opts := hosts.DefaultHostsQueryOpts
	for _, opt := range queryOpts {
		opt(&opts)
	}

	var hosts []hosts.Host
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, `
WITH globals AS (
    SELECT
		contracts_period,
		hosts_min_collateral,
		hosts_max_storage_price,
		hosts_max_ingress_price,
		hosts_max_egress_price,
		(get_byte(hosts_min_protocol_version, 0) << 16) + (get_byte(hosts_min_protocol_version, 1) << 8) + (get_byte(hosts_min_protocol_version, 2)) AS host_min_version,
		250000::NUMERIC AS sectors_per_tb,
		1E12::NUMERIC AS one_tb,
		1E24::NUMERIC AS one_sc
    FROM global_settings
), hosts AS (
	SELECT
		id, hosts.public_key, last_announcement, hb.public_key IS NOT NULL AS blocked, COALESCE(hb.reason, ''), lost_sectors,
		last_failed_scan, last_successful_scan, next_scan, consecutive_failed_scans, recent_uptime,
		settings_protocol_version, settings_release, settings_wallet_address,
		settings_accepting_contracts, settings_max_collateral, settings_max_contract_duration,
		settings_remaining_storage, settings_total_storage, settings_contract_price,
		settings_collateral, settings_storage_price, settings_ingress_price,
		settings_egress_price, settings_free_sector_price, settings_tip_height, settings_valid_until, settings_signature,
		last_successful_scan IS NOT NULL as has_settings,
		(get_byte(settings_protocol_version, 0) << 16) + (get_byte(settings_protocol_version, 1) << 8) + (get_byte(settings_protocol_version, 2)) as settings_version
	FROM hosts
	LEFT JOIN hosts_blocklist hb ON hosts.public_key = hb.public_key
) SELECT
 	hosts.*,
	recent_uptime >= 0.9,
	has_settings AND settings_max_contract_duration >= globals.contracts_period,
	has_settings AND settings_max_collateral >= settings_collateral * globals.one_tb * globals.contracts_period,
	has_settings AND settings_version >= globals.host_min_version,
	has_settings AND settings_valid_until >= (NOW() + INTERVAL '15 minutes'),
	has_settings AND settings_accepting_contracts,
	has_settings AND settings_contract_price <= globals.one_sc,
	has_settings AND settings_collateral >= globals.hosts_min_collateral AND settings_collateral >= 2 * settings_storage_price,
	has_settings AND settings_storage_price <= globals.hosts_max_storage_price,
	has_settings AND settings_ingress_price <= globals.hosts_max_ingress_price,
	has_settings AND settings_egress_price <= globals.hosts_max_egress_price,
	has_settings AND settings_free_sector_price <= globals.one_sc / globals.sectors_per_tb
FROM hosts CROSS JOIN globals
WHERE
	-- good host filter
	(($3::boolean IS NULL) OR ($3::boolean = (
		recent_uptime >= 0.9 AND
		has_settings AND
		settings_max_contract_duration >= globals.contracts_period AND
		settings_max_collateral >= settings_collateral * globals.one_tb * globals.contracts_period AND
		settings_version >= globals.host_min_version AND
		settings_valid_until >= (NOW() + INTERVAL '15 minutes') AND
		settings_accepting_contracts AND
		settings_contract_price <= globals.one_sc AND
		settings_collateral >= globals.hosts_min_collateral AND
		settings_collateral >= 2 * settings_storage_price AND
		settings_storage_price <= globals.hosts_max_storage_price AND
		settings_ingress_price <= globals.hosts_max_ingress_price AND
		settings_egress_price <= globals.hosts_max_egress_price AND
		settings_free_sector_price <= globals.one_sc / globals.sectors_per_tb
		)
	))
	-- blocked host filter
	AND (($4::boolean IS NULL) OR ($4::boolean = hosts.blocked))
	-- active contracts filter
	AND (($5::boolean IS NULL) OR ($5::boolean = EXISTS (SELECT 1 FROM contracts WHERE host_id = hosts.id AND state >= $6 AND state <= $7)))
	LIMIT $1 OFFSET $2
;`, limit, offset, opts.Good, opts.Blocked, opts.ActiveContracts, contracts.ContractStatePending, contracts.ContractStateActive)
		if err != nil {
			return fmt.Errorf("failed to query hosts: %w", err)
		}
		defer rows.Close()

		var dbHosts []*dbHost
		for rows.Next() {
			var host dbHost
			host, err := scanHost(rows)
			if err != nil {
				return fmt.Errorf("failed to scan host: %w", err)
			}
			dbHosts = append(dbHosts, &host)
		}
		if err := rows.Err(); err != nil {
			return err
		} else if len(dbHosts) == 0 {
			return nil
		}

		if err := decorateHostAddresses(ctx, tx, dbHosts...); err != nil {
			return fmt.Errorf("failed to decorate host addresses: %w", err)
		} else if err := decorateHostNetworks(ctx, tx, dbHosts...); err != nil {
			return fmt.Errorf("failed to decorate host networks: %w", err)
		}

		for _, h := range dbHosts {
			hosts = append(hosts, h.Host)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return hosts, nil
}

// BlockedHosts returns a list of blocked hostkeys.
func (s *Store) BlockedHosts(ctx context.Context, offset, limit int) ([]types.PublicKey, error) {
	// sanity check input
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var blocklist []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT public_key FROM hosts_blocklist LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query hosts blocklist: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var hk types.PublicKey
			if err := rows.Scan((*sqlPublicKey)(&hk)); err != nil {
				return fmt.Errorf("failed to scan host: %w", err)
			}
			blocklist = append(blocklist, hk)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return blocklist, nil
}

// BlockHosts adds the given host keys to the blocklist and marks all of its
// contracts as bad.
// If a host is already on the blocklist, the reason remains unchanged to
// preserve the original reason for blocking.
func (s *Store) BlockHosts(ctx context.Context, hks []types.PublicKey, reason string) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		for _, hk := range hks {
			_, err := tx.Exec(ctx, "INSERT INTO hosts_blocklist (public_key, reason) VALUES ($1, $2) ON CONFLICT (public_key) DO NOTHING", sqlPublicKey(hk), reason)
			if err != nil {
				return fmt.Errorf("failed to add host %q to blocklist: %w", hk, err)
			}
			_, err = tx.Exec(ctx, `
				UPDATE contracts
				SET good = FALSE
				FROM contracts c
				INNER JOIN hosts h ON h.id = c.host_id
				INNER JOIN hosts_blocklist hb ON hb.public_key = h.public_key
				WHERE contracts.id = c.id AND h.public_key = $1
				`, sqlPublicKey(hk))
			if err != nil {
				return fmt.Errorf("failed to update contracts: %w", err)
			}
		}
		return nil
	})
}

// UnblockHost removes the given host key from the blocklist and marks its
// contracts as good again.
func (s *Store) UnblockHost(ctx context.Context, hk types.PublicKey) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, "DELETE FROM hosts_blocklist WHERE public_key = $1", sqlPublicKey(hk))
		if err != nil {
			return fmt.Errorf("failed to remove host %q from blocklist: %w", hk, err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE contracts
			SET good = TRUE
			FROM contracts c
			INNER JOIN hosts h ON h.id = c.host_id
			WHERE contracts.id = c.id AND h.public_key = $1
			`, sqlPublicKey(hk))
		if err != nil {
			return fmt.Errorf("failed to update contracts: %w", err)
		}
		return nil
	})
}

// HostsForScanning returns a list of hosts where the next scan is due.
func (s *Store) HostsForScanning(ctx context.Context) ([]types.PublicKey, error) {
	var hosts []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT public_key FROM hosts WHERE next_scan <= NOW() ORDER BY next_scan ASC`)
		if err != nil {
			return fmt.Errorf("failed to query hosts for scanning: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var hk sqlPublicKey
			if err := rows.Scan(&hk); err != nil {
				return err
			}
			hosts = append(hosts, types.PublicKey(hk))
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return hosts, nil
}

// PruneHosts removes hosts that have not been successfully scanned since the
// given cutoff time and have failed scans consecutively for at least
// minConsecutiveFailedScans times.
func (s *Store) PruneHosts(ctx context.Context, minLastSuccessfulScan time.Time, minConsecutiveFailedScans int) (int64, error) {
	var n int64
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `DELETE FROM hosts WHERE (last_successful_scan IS NULL OR last_successful_scan <= $1) AND consecutive_failed_scans >= $2 AND NOT EXISTS (SELECT 1 FROM contracts WHERE host_id = hosts.id)`, minLastSuccessfulScan, minConsecutiveFailedScans)
		if err != nil {
			return fmt.Errorf("failed to prune hosts: %w", err)
		}
		n = res.RowsAffected()
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// UpdateHost updates a host in the database, the given parameters are the result of scanning the host.
func (s *Store) UpdateHost(ctx context.Context, hk types.PublicKey, networks []net.IPNet, hs proto4.HostSettings, scanSucceeded bool, nextScan time.Time) error {
	if len(networks) == 0 && scanSucceeded {
		return hosts.ErrNoNetworks
	}
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if !scanSucceeded {
			if res, err := tx.Exec(ctx, `
WITH computed AS (
	SELECT
		id,
		EXP(- (LN(2) / $2::double precision) * elapsed_time) AS decay_factor
	FROM (
		SELECT
			id,
			CASE
				WHEN GREATEST(last_failed_scan, last_successful_scan) IS NULL
				THEN 0
				ELSE EXTRACT(EPOCH FROM (NOW() - GREATEST(last_successful_scan, last_failed_scan)))
			END AS elapsed_time
		FROM hosts
		WHERE public_key = $1
	) AS _
)
UPDATE hosts
SET
 	recent_uptime = recent_uptime * decay_factor,
	consecutive_failed_scans = consecutive_failed_scans + 1,
	last_failed_scan = NOW(),
	next_scan = $3
FROM computed
WHERE hosts.id = computed.id`, sqlPublicKey(hk), uptimeHalfLife, nextScan); err != nil {
				return err
			} else if res.RowsAffected() == 0 {
				return fmt.Errorf("host %q: %w", hk, hosts.ErrNotFound)
			}
			return nil
		}

		var hostID int64
		err := tx.QueryRow(ctx, `
WITH computed AS (
	SELECT
		id,
		EXP(- (LN(2) / $2::double precision) * elapsed_time) AS decay_factor
	FROM (
		SELECT
			id,
			CASE
				WHEN GREATEST(last_failed_scan, last_successful_scan) IS NULL
				THEN 0
				ELSE EXTRACT(EPOCH FROM (NOW() - GREATEST(last_successful_scan, last_failed_scan)))
			END AS elapsed_time
		FROM hosts
		WHERE public_key = $1
	) AS _
)
UPDATE hosts
SET
	recent_uptime = 1 * (1 - decay_factor) + recent_uptime * decay_factor,
	consecutive_failed_scans = 0,
	last_successful_scan = NOW(),
	next_scan = $3,
	settings_protocol_version = $4,
	settings_release = $5,
	settings_wallet_address = $6,
	settings_accepting_contracts = $7,
	settings_max_collateral = $8,
	settings_max_contract_duration = $9,
	settings_remaining_storage = $10,
	settings_total_storage = $11,
	settings_contract_price = $12,
	settings_collateral = $13,
	settings_storage_price = $14,
	settings_ingress_price = $15,
	settings_egress_price = $16,
	settings_free_sector_price = $17,
	settings_tip_height = $18,
	settings_valid_until = $19,
	settings_signature = $20
FROM computed
WHERE hosts.id = computed.id RETURNING hosts.id`,
			sqlPublicKey(hk),
			uptimeHalfLife,
			nextScan,
			sqlProtocolVersion(hs.ProtocolVersion),
			hs.Release,
			sqlHash256(hs.WalletAddress),
			hs.AcceptingContracts,
			sqlCurrency(hs.MaxCollateral),
			hs.MaxContractDuration,
			hs.RemainingStorage,
			hs.TotalStorage,
			sqlCurrency(hs.Prices.ContractPrice),
			sqlCurrency(hs.Prices.Collateral),
			sqlCurrency(hs.Prices.StoragePrice),
			sqlCurrency(hs.Prices.IngressPrice),
			sqlCurrency(hs.Prices.EgressPrice),
			sqlCurrency(hs.Prices.FreeSectorPrice),
			hs.Prices.TipHeight,
			hs.Prices.ValidUntil,
			sqlSignature(hs.Prices.Signature),
		).Scan(&hostID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hk, hosts.ErrNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to update host with scan: %w", err)
		} else if hostID == 0 {
			return errors.New("failed to return host id after successful update") // sanity check
		}

		if scanSucceeded {
			_, err = tx.Exec(ctx, `DELETE FROM host_resolved_cidrs WHERE host_id = $1`, hostID)
			if err != nil {
				return err
			}

			for _, cidr := range networks {
				_, err = tx.Exec(ctx, `INSERT INTO host_resolved_cidrs (host_id, cidr) VALUES ($1, $2)`, hostID, cidr.String())
				if err != nil {
					return fmt.Errorf("failed to insert host resolved CIDR: %w", err)
				}
			}
		}

		return nil
	})
}

// UsableHosts returns a list of hosts that are not blocked, usable and have an
// active contract. It returns only the host's public key and addresses.
func (s *Store) UsableHosts(ctx context.Context, offset, limit int) ([]hosts.HostInfo, error) {
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var usable []hosts.HostInfo
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, `
WITH globals AS (
    SELECT
		contracts_period,
		hosts_min_collateral,
		hosts_max_storage_price,
		hosts_max_ingress_price,
		hosts_max_egress_price,
		(get_byte(hosts_min_protocol_version, 0) << 16) + (get_byte(hosts_min_protocol_version, 1) << 8) + (get_byte(hosts_min_protocol_version, 2)) AS host_min_version,
		250000::NUMERIC AS sectors_per_tb,
		1E12::NUMERIC AS one_tb,
		1E24::NUMERIC AS one_sc
    FROM global_settings
), hosts AS (
	SELECT
		id,
		hosts.public_key,
		recent_uptime,
		settings_protocol_version,
		settings_release,
		settings_wallet_address,
		settings_accepting_contracts, 
		settings_max_collateral, 
		settings_max_contract_duration,
		settings_remaining_storage, 
		settings_total_storage, 
		settings_contract_price,
		settings_collateral, 
		settings_storage_price, 
		settings_ingress_price,
		settings_egress_price, 
		settings_free_sector_price,
		settings_tip_height,
		settings_valid_until,
		settings_signature,
		(get_byte(settings_protocol_version, 0) << 16) + (get_byte(settings_protocol_version, 1) << 8) + (get_byte(settings_protocol_version, 2)) as settings_version
	FROM hosts
	LEFT JOIN hosts_blocklist hb ON hosts.public_key = hb.public_key
	WHERE hb.public_key IS NULL AND last_successful_scan IS NOT NULL -- not blocked and has settings
) 
SELECT 
	hosts.id,
	hosts.public_key
FROM hosts 
CROSS JOIN globals
WHERE
	-- usable
	recent_uptime >= 0.9 AND
	settings_max_contract_duration >= globals.contracts_period AND
	settings_max_collateral >= settings_collateral * globals.one_tb * globals.contracts_period AND
	settings_version >= globals.host_min_version AND
	settings_valid_until >= (NOW() + INTERVAL '15 minutes') AND
	settings_accepting_contracts AND
	settings_contract_price <= globals.one_sc AND
	settings_collateral >= globals.hosts_min_collateral AND
	settings_collateral >= 2 * settings_storage_price AND
	settings_storage_price <= globals.hosts_max_storage_price AND
	settings_ingress_price <= globals.hosts_max_ingress_price AND
	settings_egress_price <= globals.hosts_max_egress_price AND
	settings_free_sector_price <= globals.one_sc / globals.sectors_per_tb AND 
	-- active contracts
	EXISTS (SELECT 1 FROM contracts WHERE host_id = hosts.id AND state <= 1)
LIMIT $1 OFFSET $2;`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query hosts: %w", err)
		}
		defer rows.Close()

		var dbHosts []*dbHost
		for rows.Next() {
			var host dbHost
			if err := rows.Scan(&host.id, (*sqlPublicKey)(&host.PublicKey)); err != nil {
				return fmt.Errorf("failed to scan host: %w", err)
			}
			dbHosts = append(dbHosts, &host)
		}
		if err := rows.Err(); err != nil {
			return err
		} else if len(dbHosts) == 0 {
			return nil
		}

		if err := decorateHostAddresses(ctx, tx, dbHosts...); err != nil {
			return fmt.Errorf("failed to decorate host addresses: %w", err)
		}

		for _, h := range dbHosts {
			usable = append(usable, hosts.HostInfo{
				PublicKey: h.PublicKey,
				Addresses: h.Addresses,
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return usable, nil
}

func decorateHostAddresses(ctx context.Context, tx *txn, hosts ...*dbHost) error {
	hostIDs := make([]int64, 0, len(hosts))
	idToIdx := make(map[int64]int64, len(hosts))
	for i := range hosts {
		idToIdx[hosts[i].id] = int64(i)
		hostIDs = append(hostIDs, hosts[i].id)
	}

	rows, err := tx.Query(ctx, `SELECT host_id, net_address, protocol FROM host_addresses WHERE host_id = ANY($1)`, hostIDs)
	if err != nil {
		return fmt.Errorf("failed to query host addresses: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hostID int64
		var na chain.NetAddress
		if err := rows.Scan(&hostID, &na.Address, (*sqlNetworkProtocol)(&na.Protocol)); err != nil {
			return fmt.Errorf("failed to scan host address: %w", err)
		}
		hosts[idToIdx[hostID]].Addresses = append(hosts[idToIdx[hostID]].Addresses, na)
	}

	return rows.Err()
}

func decorateHostNetworks(ctx context.Context, tx *txn, hosts ...*dbHost) error {
	hostIDs := make([]int64, 0, len(hosts))
	idToIdx := make(map[int64]int64, len(hosts))
	for i := range hosts {
		idToIdx[hosts[i].id] = int64(i)
		hostIDs = append(hostIDs, hosts[i].id)
	}

	rows, err := tx.Query(ctx, `SELECT host_id, cidr FROM host_resolved_cidrs WHERE host_id = ANY($1)`, hostIDs)
	if err != nil {
		return fmt.Errorf("failed to query host networks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hostID int64
		var cidr net.IPNet
		if err := rows.Scan(&hostID, &cidr); err != nil {
			return fmt.Errorf("failed to scan host network: %w", err)
		}
		hosts[idToIdx[hostID]].Networks = append(hosts[idToIdx[hostID]].Networks, cidr)
	}

	return rows.Err()
}

func scanHost(s scanner) (dbHost, error) {
	var host dbHost
	var lastFailedScan, lastSuccessfulScan, validUntil sql.NullTime
	var ignore any
	if err := s.Scan(
		&host.id,
		(*sqlPublicKey)(&host.PublicKey),
		&host.LastAnnouncement,
		&host.Blocked,
		&host.BlockedReason,
		&host.LostSectors,
		&lastFailedScan,
		&lastSuccessfulScan,
		&host.NextScan,
		&host.ConsecutiveFailedScans,
		&host.RecentUptime,
		(*sqlProtocolVersion)(&host.Settings.ProtocolVersion),
		&host.Settings.Release,
		(*sqlHash256)(&host.Settings.WalletAddress),
		&host.Settings.AcceptingContracts,
		(*sqlCurrency)(&host.Settings.MaxCollateral),
		&host.Settings.MaxContractDuration,
		&host.Settings.RemainingStorage,
		&host.Settings.TotalStorage,
		(*sqlCurrency)(&host.Settings.Prices.ContractPrice),
		(*sqlCurrency)(&host.Settings.Prices.Collateral),
		(*sqlCurrency)(&host.Settings.Prices.StoragePrice),
		(*sqlCurrency)(&host.Settings.Prices.IngressPrice),
		(*sqlCurrency)(&host.Settings.Prices.EgressPrice),
		(*sqlCurrency)(&host.Settings.Prices.FreeSectorPrice),
		&host.Settings.Prices.TipHeight,
		&validUntil,
		(*sqlSignature)(&host.Settings.Prices.Signature),
		&ignore,
		&ignore,
		&host.Usability.Uptime,
		&host.Usability.MaxContractDuration,
		&host.Usability.MaxCollateral,
		&host.Usability.ProtocolVersion,
		&host.Usability.PriceValidity,
		&host.Usability.AcceptingContracts,
		&host.Usability.ContractPrice,
		&host.Usability.Collateral,
		&host.Usability.StoragePrice,
		&host.Usability.IngressPrice,
		&host.Usability.EgressPrice,
		&host.Usability.FreeSectorPrice,
	); err != nil {
		return dbHost{}, err
	}

	if lastFailedScan.Valid {
		host.LastFailedScan = lastFailedScan.Time
	}
	if lastSuccessfulScan.Valid {
		host.LastSuccessfulScan = lastSuccessfulScan.Time
	}
	if validUntil.Valid {
		host.Settings.Prices.ValidUntil = validUntil.Time
	}

	return host, nil
}

// HostsForIntegrityChecks returns a list of hosts that have sectors
// requiring integrity checks.
func (s *Store) HostsForIntegrityChecks(ctx context.Context, maxLastCheck time.Time, limit int) ([]types.PublicKey, error) {
	var hosts []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			WITH to_check AS (
				SELECT h.id
				FROM hosts h
				LEFT JOIN hosts_blocklist hb ON h.public_key = hb.public_key
				WHERE EXISTS (
					SELECT 1
					FROM sectors
					WHERE sectors.host_id = h.id
						AND sectors.next_integrity_check <= NOW()
				)
				AND hb.public_key IS NULL
				AND h.last_integrity_check <= $1
				LIMIT $2
			)
			UPDATE hosts
			SET last_integrity_check = NOW()
			FROM to_check
			WHERE hosts.id = to_check.id
			RETURNING hosts.public_key
		`, maxLastCheck, limit)
		if err != nil {
			return fmt.Errorf("failed to query hosts for integrity checks: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var hk sqlPublicKey
			if err := rows.Scan(&hk); err != nil {
				return err
			}
			hosts = append(hosts, types.PublicKey(hk))
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return hosts, nil
}

// HostsForPinning returns a list of host keys that can be used for sector
// pinning. A host is eligble for pinning if it is not blocked, has unpinned
// sectors and has an active contract.
func (s *Store) HostsForPinning(ctx context.Context) ([]types.PublicKey, error) {
	var hosts []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT h.public_key
			FROM hosts h
			WHERE
				EXISTS (
					SELECT 1
					FROM sectors
					WHERE sectors.host_id = h.id AND contract_sectors_map_id IS NULL
				) AND
				EXISTS (
					SELECT 1
					FROM contracts
					WHERE contracts.host_id = h.id AND contracts.state <= $1 AND contracts.good = TRUE
				) AND
				NOT EXISTS (SELECT 1 FROM hosts_blocklist hb WHERE hb.public_key = h.public_key)
				 `, contracts.ContractStateActive)
		if err != nil {
			return fmt.Errorf("failed to query hosts for pinning: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var hk sqlPublicKey
			if err := rows.Scan(&hk); err != nil {
				return err
			}
			hosts = append(hosts, types.PublicKey(hk))
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return hosts, nil
}

// HostsForPruning returns a list of host keys that have contracts that need
// pruning.
func (s *Store) HostsForPruning(ctx context.Context) ([]types.PublicKey, error) {
	var hosts []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT h.public_key
			FROM hosts h
			WHERE
				EXISTS (
					SELECT 1
					FROM contracts
					WHERE contracts.host_id = h.id AND contracts.state <= $1 AND contracts.good = TRUE AND contracts.next_prune < NOW()
				) AND
				NOT EXISTS (
					SELECT 1 
					FROM hosts_blocklist hb 
					WHERE hb.public_key = h.public_key
				)`, contracts.ContractStateActive)
		if err != nil {
			return fmt.Errorf("failed to query hosts for pruning: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var hk sqlPublicKey
			if err := rows.Scan(&hk); err != nil {
				return err
			}
			hosts = append(hosts, types.PublicKey(hk))
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return hosts, nil
}

// HostsWithLostSectors returns a list of host keys that have contracts with
// lost sectors.
func (s *Store) HostsWithLostSectors(ctx context.Context) ([]types.PublicKey, error) {
	var hks []types.PublicKey
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT public_key
			FROM hosts
			WHERE lost_sectors > 0`)
		if err != nil {
			return fmt.Errorf("failed to query hosts for lost sectors alert: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var hk sqlPublicKey
			if err := rows.Scan(&hk); err != nil {
				return err
			}
			hks = append(hks, types.PublicKey(hk))
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return hks, nil
}
