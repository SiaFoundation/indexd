package slabs

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
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

// dialer implements the [HostDialer] interface.
type dialer struct {
	log    *zap.Logger
	dialer Dialer

	mu    sync.Mutex
	conns map[types.PublicKey]*connEntry
}

// newDialer returns a new dialer that reuses existing connections if possible.
func newDialer(d Dialer, log *zap.Logger) *dialer {
	return &dialer{
		log:    log.Named("dialer"),
		dialer: d,

		conns: make(map[types.PublicKey]*connEntry),
	}
}

// Close closes all the open connections on the dialer.
func (d *dialer) Close() {
	d.mu.Lock()
	conns := slices.Collect(maps.Values(d.conns))
	d.mu.Unlock()

	for _, entry := range conns {
		entry.close(d.log)
	}
	clear(d.conns)
}

// clearHostConnection clears the connection for a host
func (d *dialer) clearHostConnection(hostKey types.PublicKey) {
	d.mu.Lock()
	entry, exists := d.conns[hostKey]
	d.mu.Unlock()
	if !exists {
		return
	}

	entry.clear()
}

func (d *dialer) dialHost(ctx context.Context, hostKey types.PublicKey, addrs []chain.NetAddress) (HostClient, error) {
	d.mu.Lock()
	// Get or create connection entry
	entry, exists := d.conns[hostKey]
	if !exists {
		entry = &connEntry{hostKey: hostKey}
		d.conns[hostKey] = entry
	}
	d.mu.Unlock()

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

	tc, err = d.dialer.DialHost(ctx, hostKey, addrs)
	if err != nil {
		return nil, fmt.Errorf("failed to dial host: %w", err)
	}
	return tc, nil
}

func (d *dialer) retry(ctx context.Context, hostKey types.PublicKey, addrs []chain.NetAddress, fn func(HostClient) error) error {
	// First attempt
	tc, err := d.dialHost(ctx, hostKey, addrs)
	if err != nil {
		return fmt.Errorf("failed to dial host: %w", err)
	}
	if err := fn(tc); err == nil {
		return nil
	} else if proto.ErrorCode(err) != proto.ErrorCodeTransport {
		return err
	}

	// Clear connection if we got transport error
	d.clearHostConnection(hostKey)

	// Second attempt
	tc, err = d.dialHost(ctx, hostKey, addrs)
	if err != nil {
		return fmt.Errorf("failed to redial host: %w", err)
	}
	return fn(tc)
}
