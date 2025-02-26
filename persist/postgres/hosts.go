package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/api"
)

var ErrHostNotFound = errors.New("host not found")

type dbHost struct {
	id int64
	api.Host
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

// Hosts returns a list of hosts.
func (s *Store) Hosts(ctx context.Context, offset, limit int) ([]api.Host, error) {
	// sanity check input
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var hosts []api.Host
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
			hosts = append(hosts, h.Host)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return hosts, nil
}

func queryHosts(ctx context.Context, tx *txn, offset, limit int) ([]dbHost, error) {
	rows, err := tx.Query(ctx, `SELECT h.id, h.public_key, h.last_announcement FROM hosts h LIMIT $1 OFFSET $2`, limit, offset)
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
