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
	"go.sia.tech/indexd/hosts"
)

// ErrHostNotFound is returned by database operations that fail due to a host
// not being found.
var ErrHostNotFound = errors.New("host not found")

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
		h, err := scanHost(tx.QueryRow(ctx, `
SELECT
	id, public_key, last_announcement, total_scans, failed_scans, consecutive_failed_scans, last_successful_scan, next_scan,
	settings_protocol_version, settings_release, settings_wallet_address, settings_accepting_contracts,
	settings_max_collateral, settings_max_contract_duration, settings_remaining_storage, settings_total_storage,
	settings_contract_price, settings_collateral, settings_storage_price, settings_ingress_price, settings_egress_price,
	settings_free_sector_price, settings_tip_height, settings_valid_until
FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)))
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hk, ErrHostNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to query host: %w", err)
		}
		h.Addresses, err = queryHostAddresses(ctx, tx, h.id)
		if err != nil {
			return err
		}
		h.Networks, err = queryHostNetworks(ctx, tx, h.id)
		if err != nil {
			return err
		}
		host = h.Host
		return nil
	}); err != nil {
		return hosts.Host{}, err
	}
	return host, nil
}

// Hosts returns a list of hosts.
func (s *Store) Hosts(ctx context.Context, offset, limit int) ([]hosts.Host, error) {
	// sanity check input
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var hosts []hosts.Host
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		dbHosts, err := queryHosts(ctx, tx, offset, limit)
		if err != nil {
			return err
		}
		for _, h := range dbHosts {
			h.Addresses, err = queryHostAddresses(ctx, tx, h.id)
			if err != nil {
				return err
			}
			h.Networks, err = queryHostNetworks(ctx, tx, h.id)
			if err != nil {
				return err
			}

			// TODO: perform host checks

			hosts = append(hosts, h.Host)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return hosts, nil
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
				return fmt.Errorf("failed to scan host: %w", err)
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
		res, err := tx.Exec(ctx, `DELETE FROM hosts WHERE last_successful_scan <= $1 AND consecutive_failed_scans >= $2 AND NOT EXISTS (SELECT 1 FROM contracts WHERE host_id = hosts.id)`, minLastSuccessfulScan, minConsecutiveFailedScans)
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
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if !scanSucceeded {
			if res, err := tx.Exec(ctx, `
UPDATE hosts SET 
	total_scans = total_scans + 1,
	failed_scans = failed_scans + 1,
	consecutive_failed_scans = consecutive_failed_scans + 1,
	next_scan = $1 
WHERE public_key = $2`, nextScan, sqlPublicKey(hk)); err != nil {
				return err
			} else if res.RowsAffected() == 0 {
				return ErrHostNotFound
			}
			return nil
		}

		var hostID int64
		err := tx.QueryRow(ctx, `
UPDATE hosts SET 
	total_scans = total_scans + 1,
	consecutive_failed_scans = 0,
	last_successful_scan = NOW(),
	next_scan = $1,
	settings_protocol_version = $2,
	settings_release = $3,
	settings_wallet_address = $4,
	settings_accepting_contracts = $5,
	settings_max_collateral = $6,
	settings_max_contract_duration = $7,
	settings_remaining_storage = $8,
	settings_total_storage = $9,
	settings_contract_price = $10,
	settings_collateral = $11,
	settings_storage_price = $12,
	settings_ingress_price = $13,
	settings_egress_price = $14,
	settings_free_sector_price = $15,
	settings_tip_height = $16,
	settings_valid_until = $17
WHERE public_key = $18 RETURNING id`,
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
			sqlPublicKey(hk),
		).Scan(&hostID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrHostNotFound
		} else if err != nil {
			return fmt.Errorf("failed to update host with scan: %w", err)
		}

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

		return nil
	})
}

func queryHosts(ctx context.Context, tx *txn, offset, limit int) ([]dbHost, error) {
	rows, err := tx.Query(ctx, `
SELECT 
	id, public_key, last_announcement, total_scans, failed_scans, consecutive_failed_scans, last_successful_scan, next_scan,
	settings_protocol_version, settings_release, settings_wallet_address, settings_accepting_contracts,
	settings_max_collateral, settings_max_contract_duration, settings_remaining_storage, settings_total_storage,
	settings_contract_price, settings_collateral, settings_storage_price, settings_ingress_price, settings_egress_price,
	settings_free_sector_price, settings_tip_height, settings_valid_until
FROM hosts LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query hosts: %w", err)
	}
	defer rows.Close()

	var hosts []dbHost
	for rows.Next() {
		host, err := scanHost(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan host: %w", err)
		}
		hosts = append(hosts, host)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return hosts, nil
}

func queryHostAddresses(ctx context.Context, tx *txn, hostID int64) ([]chain.NetAddress, error) {
	rows, err := tx.Query(ctx, `SELECT net_address, protocol FROM host_addresses WHERE host_id = $1`, hostID)
	if err != nil {
		return nil, fmt.Errorf("failed to query host addresses: %w", err)
	}
	defer rows.Close()

	var addresses []chain.NetAddress
	for rows.Next() {
		var na chain.NetAddress
		if err := rows.Scan(&na.Address, (*sqlNetworkProtocol)(&na.Protocol)); err != nil {
			return nil, fmt.Errorf("failed to scan host address: %w", err)
		}
		addresses = append(addresses, na)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return addresses, nil
}

func queryHostNetworks(ctx context.Context, tx *txn, hostID int64) ([]net.IPNet, error) {
	rows, err := tx.Query(ctx, `SELECT cidr FROM host_resolved_cidrs WHERE host_id = $1`, hostID)
	if err != nil {
		return nil, fmt.Errorf("failed to query host resolved CIDRs: %w", err)
	}
	defer rows.Close()

	var networks []net.IPNet
	for rows.Next() {
		var cidr net.IPNet
		if err := rows.Scan(&cidr); err != nil {
			return nil, fmt.Errorf("failed to scan host address: %w", err)
		}
		networks = append(networks, cidr)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return networks, nil
}

func scanHost(s scanner) (host dbHost, err error) {
	err = s.Scan(
		&host.id,
		(*sqlPublicKey)(&host.PublicKey),
		&host.LastAnnouncement,
		&host.TotalScans,
		&host.FailedScans,
		&host.ConsecutiveFailedScans,
		&host.LastSuccessfulScan,
		&host.NextScan,
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
		&host.Settings.Prices.ValidUntil)
	return
}
