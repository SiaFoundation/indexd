package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

	totalScans             int
	failedScans            int
	consecutiveFailedScans int
	nextScan               time.Time

	networks []string

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

// UpdateHost updates a host in the database, the given parameters are the result of scanning the host.
func (s *Store) UpdateHost(ctx context.Context, hk types.PublicKey, networks []string, hs proto4.HostSettings, scanSucceeded bool, nextScan time.Time) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var query string
		if scanSucceeded {
			query = `UPDATE hosts SET total_scans = total_scans + 1, consecutive_failed_scans = 0, next_scan = $1 WHERE public_key = $2 RETURNING id`
		} else {
			query = `UPDATE hosts SET total_scans = total_scans + 1, failed_scans = failed_scans + 1, consecutive_failed_scans = consecutive_failed_scans + 1, next_scan = $1 WHERE public_key = $2 RETURNING id`
		}

		var hostID int64
		err := tx.QueryRow(ctx, query, nextScan, sqlPublicKey(hk)).Scan(&hostID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrHostNotFound
		} else if err != nil {
			return fmt.Errorf("failed to update host with scan: %w", err)
		} else if !scanSucceeded {
			return nil
		}

		if _, err := tx.Exec(ctx, `INSERT INTO host_settings (
host_id, protocol_version, release, wallet_address, accepting_contracts, 
max_collateral, max_contract_duration, remaining_storage, total_storage, 
contract_price, collateral, storage_price, ingress_price, egress_price, 
free_sector_price, tip_height, valid_until) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT (host_id) 
DO UPDATE SET 
    protocol_version = EXCLUDED.protocol_version,
    release = EXCLUDED.release,
    wallet_address = EXCLUDED.wallet_address,
    accepting_contracts = EXCLUDED.accepting_contracts,
    max_collateral = EXCLUDED.max_collateral,
    max_contract_duration = EXCLUDED.max_contract_duration,
    remaining_storage = EXCLUDED.remaining_storage,
    total_storage = EXCLUDED.total_storage,
    contract_price = EXCLUDED.contract_price,
    collateral = EXCLUDED.collateral,
    storage_price = EXCLUDED.storage_price,
    ingress_price = EXCLUDED.ingress_price,
    egress_price = EXCLUDED.egress_price,
    free_sector_price = EXCLUDED.free_sector_price,
    tip_height = EXCLUDED.tip_height,
    valid_until = EXCLUDED.valid_until;`,
			hostID,
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
		); err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `DELETE FROM host_resolved_cidrs WHERE host_id = $1`, hostID)
		if err != nil {
			return err
		}

		for _, cidr := range networks {
			_, err = tx.Exec(ctx, `INSERT INTO host_resolved_cidrs (host_id, cidr) VALUES ($1, $2)`, hostID, cidr)
			if err != nil {
				return fmt.Errorf("failed to insert host resolved CIDR: %w", err)
			}
		}

		return nil
	})
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
			h.Settings, err = queryHostSettings(ctx, tx, h.id)
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

// HostAddresses returns the addresses of a host. If the host is not found, ErrHostNotFound is returned.
func (s *Store) HostAddresses(ctx context.Context, hk types.PublicKey) ([]chain.NetAddress, error) {
	var addresses []chain.NetAddress
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)).Scan(&hostID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hk, ErrHostNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to query host: %w", err)
		}

		rows, err := tx.Query(ctx, `SELECT net_address, protocol FROM host_addresses WHERE host_id = $1`, hostID)
		if err != nil {
			return fmt.Errorf("failed to query host addresses: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var na chain.NetAddress
			if err := rows.Scan(&na.Address, (*sqlNetworkProtocol)(&na.Protocol)); err != nil {
				return fmt.Errorf("failed to scan host address: %w", err)
			}
			addresses = append(addresses, na)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return addresses, nil
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

func queryHosts(ctx context.Context, tx *txn, offset, limit int) ([]dbHost, error) {
	rows, err := tx.Query(ctx, `SELECT id, public_key, last_announcement FROM hosts LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query hosts: %w", err)
	}
	defer rows.Close()

	var hosts []dbHost
	for rows.Next() {
		var host dbHost
		if err := rows.Scan(&host.id, (*sqlPublicKey)(&host.PublicKey), &host.LastAnnouncement); err != nil {
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

func queryHostSettings(ctx context.Context, tx *txn, hostID int64) (proto4.HostSettings, error) {
	var hs proto4.HostSettings
	err := tx.QueryRow(ctx, `SELECT protocol_version, release, wallet_address, accepting_contracts, max_collateral, max_contract_duration, remaining_storage, total_storage, contract_price, collateral, storage_price, ingress_price, egress_price, free_sector_price, tip_height, valid_until FROM host_settings WHERE host_id = $1`, hostID).Scan(
		(*sqlProtocolVersion)(&hs.ProtocolVersion),
		&hs.Release,
		(*sqlHash256)(&hs.WalletAddress),
		&hs.AcceptingContracts,
		(*sqlCurrency)(&hs.MaxCollateral),
		&hs.MaxContractDuration,
		&hs.RemainingStorage,
		&hs.TotalStorage,
		(*sqlCurrency)(&hs.Prices.ContractPrice),
		(*sqlCurrency)(&hs.Prices.Collateral),
		(*sqlCurrency)(&hs.Prices.StoragePrice),
		(*sqlCurrency)(&hs.Prices.IngressPrice),
		(*sqlCurrency)(&hs.Prices.EgressPrice),
		(*sqlCurrency)(&hs.Prices.FreeSectorPrice),
		&hs.Prices.TipHeight,
		&hs.Prices.ValidUntil,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return proto4.HostSettings{}, nil
	} else if err != nil {
		return proto4.HostSettings{}, fmt.Errorf("failed to query host settings: %w", err)
	}
	return hs, nil
}
