package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type connEntry struct {
	hostKey types.PublicKey

	mu   sync.Mutex
	dial chan struct{} // signals dial completion
	tc   HostClient
}

func (c *connEntry) close(log *zap.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tc != nil {
		if err := c.tc.Close(); err != nil {
			log.Debug("Failed to close connection", zap.Stringer("pk", c.hostKey), zap.Error(err))
		}
	}
	c.tc = nil
}

func (c *connEntry) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tc != nil {
		c.tc.Close()
		c.tc = nil
	}
}

func (c *connEntry) setTransport(tc HostClient, assign bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if assign {
		c.tc = tc
	}
	close(c.dial)
	c.dial = nil
}

// connPool is a connection pool that wraps a [Dialer] and caches its
// connections.
type connPool struct {
	log    *zap.Logger
	dialer Dialer

	mu    sync.Mutex
	conns map[types.PublicKey]*connEntry
}

// newConnPool returns a new connection pool.
func newConnPool(d Dialer, log *zap.Logger) *connPool {
	return &connPool{
		log:    log.Named("dialer"),
		dialer: d,

		conns: make(map[types.PublicKey]*connEntry),
	}
}

// Close closes all the open connections on the dialer.
func (c *connPool) Close() {
	c.mu.Lock()
	conns := slices.Collect(maps.Values(c.conns))
	clear(c.conns)
	c.mu.Unlock()

	for _, entry := range conns {
		entry.close(c.log)
	}
}

// clearHostConnection clears the connection for a host
func (c *connPool) clearHostConnection(hostKey types.PublicKey) {
	c.mu.Lock()
	entry, exists := c.conns[hostKey]
	c.mu.Unlock()
	if !exists {
		return
	}

	entry.clear()
}

func (c *connPool) dialHost(ctx context.Context, hostKey types.PublicKey, addrs []chain.NetAddress) (HostClient, error) {
	c.mu.Lock()
	// Get or create connection entry
	entry, exists := c.conns[hostKey]
	if !exists {
		entry = &connEntry{hostKey: hostKey}
		c.conns[hostKey] = entry
	}
	c.mu.Unlock()

	for {
		entry.mu.Lock()
		if entry.dial == nil {
			// No dial in progress
			if entry.tc != nil {
				tc := entry.tc
				entry.mu.Unlock()
				return tc, nil
			}

			// Start new dial
			entry.dial = make(chan struct{})
			entry.mu.Unlock()
			break
		}

		// Wait for any ongoing dial to complete
		ch := entry.dial
		entry.mu.Unlock()

		select {
		case <-ch:
			// dial finished
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Actually dial outside the lock
	var tc HostClient
	var err error
	defer func() {
		entry.setTransport(tc, err == nil)
	}()

	tc, err = c.dialer.DialHost(ctx, hostKey, addrs)
	if err != nil {
		return nil, fmt.Errorf("failed to dial host: %w", err)
	}
	return tc, nil
}

func (c *connPool) retry(ctx context.Context, hostKey types.PublicKey, addrs []chain.NetAddress, fn func(context.Context, HostClient) error) error {
	// first attempt
	tc, err := c.dialHost(ctx, hostKey, addrs)
	if err != nil {
		return fmt.Errorf("failed to dial host: %w", err)
	}
	if err := fn(ctx, tc); err == nil {
		return nil
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	} else if proto.ErrorCode(err) != proto.ErrorCodeTransport {
		return err
	}

	// clear connection if we got transport error
	c.clearHostConnection(hostKey)

	// second attempt
	tc, err = c.dialHost(ctx, hostKey, addrs)
	if err != nil {
		return fmt.Errorf("failed to redial host: %w", err)
	}
	return fn(ctx, tc)
}

func (c *connPool) downloadShard(ctx context.Context, h hosts.Host, migrationToken proto.AccountToken, sector Sector) (proto.Usage, []byte, error) {
	var result rhp.RPCReadSectorResult
	var buf bytes.Buffer
	err := c.retry(ctx, h.PublicKey, h.RHP4Addrs(), func(ctx context.Context, client HostClient) error {
		buf.Reset()

		settings, err := client.Settings(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch host settings: %w", err)
		}

		result, err = client.ReadSector(ctx, settings.Prices, migrationToken, &buf, sector.Root, 0, proto.SectorSize)
		if err != nil {
			return fmt.Errorf("failed to read sector: %w", err)
		}
		return nil
	})
	if err != nil {
		return proto.Usage{}, nil, err
	}

	return result.Usage, buf.Bytes(), nil
}

func (c *connPool) uploadShard(ctx context.Context, h hosts.Host, migrationToken proto.AccountToken, shard io.Reader) (proto.Usage, types.Hash256, error) {
	var result rhp.RPCWriteSectorResult
	err := c.retry(ctx, h.PublicKey, h.RHP4Addrs(), func(ctx context.Context, client HostClient) error {
		settings, err := client.Settings(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch host settings: %w", err)
		}

		result, err = client.WriteSector(ctx, settings.Prices, migrationToken, shard, proto.SectorSize)
		if err != nil {
			return fmt.Errorf("failed to write sector: %w", err)
		}
		return nil
	})
	if err != nil {
		return proto.Usage{}, types.Hash256{}, err
	}

	return result.Usage, result.Root, nil
}
