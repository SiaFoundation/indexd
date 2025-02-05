package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"go.sia.tech/coreutils/syncer"
	"go.uber.org/zap"
)

// AddPeer adds a peer to the store. If the peer already exists, nil is
// returned.
func (s *Store) AddPeer(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid peer address: %w", err)
	}
	return s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		const query = `INSERT INTO syncer_peers (ip_address, port, first_seen, last_connect, synced_blocks, sync_duration) VALUES ($1, $2, $3, '0001-01-01'::TIMESTAMP WITH TIME ZONE, 0, 0) ON CONFLICT (ip_address) DO NOTHING`
		_, err := tx.Exec(query, host, port, time.Now())
		return err
	})
}

// PeerInfo returns the metadata for the specified peer or ErrPeerNotFound if
// the peer wasn't found in the store.
func (s *Store) PeerInfo(addr string) (info syncer.PeerInfo, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		err = fmt.Errorf("invalid peer address: %w", err)
		return
	}
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		const query = `SELECT first_seen, last_connect, synced_blocks, sync_duration FROM syncer_peers WHERE ip_address=$1 AND port=$2`
		err = tx.QueryRow(query, host, port).Scan(&info.FirstSeen, &info.LastConnect, &info.SyncedBlocks, (*sqlDurationMS)(&info.SyncDuration))
		if errors.Is(err, sql.ErrNoRows) {
			err = syncer.ErrPeerNotFound
		} else if err == nil {
			info.Address = addr
		}
		return err
	})
	return
}

// Peers returns the set of known peers.
func (s *Store) Peers() (infos []syncer.PeerInfo, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		const query = `SELECT ip_address, port, first_seen, last_connect, synced_blocks, sync_duration FROM syncer_peers`
		rows, err := tx.Query(query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var host string
			var port int
			var info syncer.PeerInfo
			if err := rows.Scan(&host, &port, &info.FirstSeen, &info.LastConnect, &info.SyncedBlocks, (*sqlDurationMS)(&info.SyncDuration)); err != nil {
				return err
			}
			info.Address = net.JoinHostPort(host, fmt.Sprint(port))
			infos = append(infos, info)
		}
		return rows.Err()
	})
	return
}

// UpdatePeerInfo updates the info for the given peer.
func (s *Store) UpdatePeerInfo(addr string, fn func(*syncer.PeerInfo)) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid peer address: %w", err)
	}

	return s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		var info syncer.PeerInfo
		err := tx.
			QueryRow(`SELECT first_seen, last_connect, synced_blocks, sync_duration FROM syncer_peers WHERE ip_address=$1 AND port=$2`, host, port).
			Scan(&info.FirstSeen, &info.LastConnect, &info.SyncedBlocks, (*sqlDurationMS)(&info.SyncDuration))
		if errors.Is(err, sql.ErrNoRows) {
			return syncer.ErrPeerNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get peer info: %w", err)
		}
		info.Address = addr

		fn(&info)

		res, err := tx.Exec(`UPDATE syncer_peers SET first_seen=$1, last_connect=$2, synced_blocks=$3, sync_duration=$4 WHERE ip_address=$5 AND port=$6`, info.FirstSeen, info.LastConnect, info.SyncedBlocks, sqlDurationMS(info.SyncDuration), host, port)
		if err != nil {
			return fmt.Errorf("failed to update peer info: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n != 1 {
			return fmt.Errorf("expected 1 row to be updated, got %d", n)
		}
		return nil
	})
}

// Ban temporarily bans one or more IPs. The addr should either be a single
// IP with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
func (s *Store) Ban(addr string, duration time.Duration, reason string) error {
	address, err := normalizePeer(addr)
	if err != nil {
		return fmt.Errorf("failed to normalize peer: %w", err)
	}
	expiration := time.Now().Add(duration)
	s.log.Debug("banning peer", zap.String("peer", addr), zap.Time("expiration", expiration), zap.Duration("duration", duration), zap.String("reason", reason))
	return s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		const query = `INSERT INTO syncer_bans (net_cidr, expiration, reason) VALUES ($1::INET, $2, $3) ON CONFLICT (net_cidr) DO UPDATE SET expiration=EXCLUDED.expiration, reason=EXCLUDED.reason RETURNING net_cidr`
		var updatedSubnet string
		err := tx.QueryRow(query, address, expiration, reason).Scan(&updatedSubnet)
		s.log.Debug("banned peer", zap.String("subnet", updatedSubnet), zap.Time("expiration", expiration), zap.String("reason", reason))
		return err
	})
}

// Banned returns true if the peer is banned.
func (s *Store) Banned(peer string) (bool, error) {
	// normalize the peer into a CIDR subnet
	peer, err := normalizePeer(peer)
	if err != nil {
		s.log.Error("failed to normalize peer", zap.Error(err))
		return false, err
	}

	_, net, err := net.ParseCIDR(peer)
	if err != nil {
		s.log.Error("failed to parse CIDR", zap.Error(err))
		return false, err
	}

	var banned bool
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		var subnet string
		var expiration time.Time
		query := `SELECT net_cidr, expiration FROM syncer_bans WHERE net_cidr >>= $1::INET ORDER BY expiration DESC LIMIT 1`
		err := tx.QueryRow(query, net.String()).Scan(&subnet, &expiration)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			s.log.Error("failed to check ban status", zap.Error(err))
			return err
		}

		remaining := time.Until(expiration)
		banned = remaining > 0 // will be true even if err != nil
		s.log.Debug("found ban", zap.Bool("banned", banned), zap.Stringer("peer", net), zap.String("subnet", subnet), zap.Time("expiration", expiration), zap.Duration("remaining", remaining))
		return err
	})
	return banned, err
}

// normalizePeer normalizes a peer address to a CIDR subnet.
func normalizePeer(peer string) (string, error) {
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		host = peer
	}
	if strings.IndexByte(host, '/') != -1 {
		_, subnet, err := net.ParseCIDR(host)
		if err != nil {
			return "", fmt.Errorf("failed to parse CIDR: %w", err)
		}
		return subnet.String(), nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", errors.New("invalid IP address")
	}

	var maskLen int
	if ip.To4() != nil {
		maskLen = 32
	} else {
		maskLen = 128
	}

	_, normalized, err := net.ParseCIDR(fmt.Sprintf("%s/%d", ip.String(), maskLen))
	if err != nil {
		panic("failed to parse CIDR")
	}
	return normalized.String(), nil
}
