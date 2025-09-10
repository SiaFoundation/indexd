package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"maps"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/api"
	"go.uber.org/zap"
)

var _ HostDialer = (*Dialer)(nil)

type connEntry struct {
	hostKey types.PublicKey

	mu   sync.Mutex
	dial chan struct{} // signals dial completion
	tc   rhp.TransportClient
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

func (c *connEntry) isConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tc != nil
}

func (c *connEntry) setTransport(tc rhp.TransportClient, assign bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if assign {
		c.tc = tc
	}
	close(c.dial)
	c.dial = nil
}

// Dialer implements the [HostDialer] interface.
type Dialer struct {
	log *zap.Logger
	tg  *threadgroup.ThreadGroup

	c      AppClient
	appKey types.PrivateKey

	mu             sync.Mutex
	addrs          map[types.PublicKey][]chain.NetAddress
	conns          map[types.PublicKey]*connEntry
	cachedSettings map[types.PublicKey]proto.HostSettings
}

// NewDialer returns a new Dialer.
func NewDialer(c AppClient, appKey types.PrivateKey, log *zap.Logger) (*Dialer, error) {
	d := &Dialer{
		log: log.Named("dialer"),

		c:      c,
		appKey: appKey,

		tg:             threadgroup.New(),
		addrs:          make(map[types.PublicKey][]chain.NetAddress),
		conns:          make(map[types.PublicKey]*connEntry),
		cachedSettings: make(map[types.PublicKey]proto.HostSettings),
	}
	if err := d.initHosts(); err != nil {
		return nil, err
	}

	return d, nil
}

// Close closes all the open connections on the dialer.
func (d *Dialer) Close() {
	d.tg.Stop()

	d.mu.Lock()
	conns := slices.Collect(maps.Values(d.conns))
	d.mu.Unlock()

	for _, entry := range conns {
		entry.close(d.log)
	}
	clear(d.conns)
}

func (d *Dialer) initHosts() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// init hosts
	err := d.updateHosts(ctx)
	if err != nil {
		return fmt.Errorf("failed to refresh hosts list: %w", err)
	}

	// refresh in background thread
	ctx, cancel, err = d.tg.AddContext(context.Background())
	if err != nil {
		return err
	}

	go func() {
		defer cancel()

		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := d.updateHosts(context.Background()); err != nil {
					d.log.Warn("Failed to refresh hosts list", zap.Error(err))
				}
			case <-d.tg.Done():
				return
			}
		}
	}()

	return nil
}

func (d *Dialer) updateHosts(ctx context.Context) error {
	ctx, cancel, err := d.tg.AddContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to add thread to thread group: %w", err)
	}
	defer cancel()

	offset, limit := 0, 100
	addrs := make(map[types.PublicKey][]chain.NetAddress)
	for {
		hosts, err := d.c.Hosts(ctx, api.WithOffset(offset), api.WithLimit(limit))
		if err != nil {
			return fmt.Errorf("failed to get hosts: %w", err)
		}

		for _, host := range hosts {
			addrs[host.PublicKey] = host.Addresses
		}

		offset += len(hosts)
		if len(hosts) < limit {
			break
		}
	}

	d.mu.Lock()
	d.addrs = addrs
	d.mu.Unlock()
	return nil
}

// clearHostConnection clears the connection for a host
func (d *Dialer) clearHostConnection(hostKey types.PublicKey) {
	d.mu.Lock()
	entry, exists := d.conns[hostKey]
	d.mu.Unlock()
	if !exists {
		return
	}

	entry.clear()
}

// Hosts implements the [HostDialer] interface.
func (d *Dialer) Hosts() (hks []types.PublicKey) {
	// grab current state
	d.mu.Lock()
	addrs := slices.Collect(maps.Keys(d.addrs))
	conns := slices.Collect(maps.Values(d.conns))
	d.mu.Unlock()

	// prioritize hosts we already have a connection with
	seen := make(map[types.PublicKey]struct{})
	for _, entry := range conns {
		if entry.isConnected() {
			hks = append(hks, entry.hostKey)
			seen[entry.hostKey] = struct{}{}
		}
	}

	// add remaining hosts
	for _, hk := range addrs {
		if _, ok := seen[hk]; !ok {
			hks = append(hks, hk)
		}
	}

	return
}

func (d *Dialer) dialHost(ctx context.Context, hostKey types.PublicKey) (rhp.TransportClient, error) {
	d.mu.Lock()
	h, ok := d.addrs[hostKey]
	if !ok {
		d.mu.Unlock()
		return nil, fmt.Errorf("missing host with key: %v", hostKey)
	}
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
	var tc rhp.TransportClient
	var err error
	defer func() {
		entry.setTransport(tc, err == nil)
	}()

	for _, addr := range h {
		if addr.Protocol == siamux.Protocol {
			tc, err = siamux.Dial(ctx, addr.Address, hostKey)
			if err == nil {
				return tc, nil
			} else {
				return nil, err
			}
		}
	}
	for _, addr := range h {
		if addr.Protocol == quic.Protocol {
			tc, err = quic.Dial(ctx, addr.Address, hostKey)
			if err == nil {
				return tc, nil
			} else {
				return nil, err
			}
		}
	}

	err = errors.New("host has no supported protocols")
	return nil, err
}

func (d *Dialer) retry(ctx context.Context, hostKey types.PublicKey, fn func(rhp.TransportClient) error) error {
	// First attempt
	tc, err := d.dialHost(ctx, hostKey)
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
	tc, err = d.dialHost(ctx, hostKey)
	if err != nil {
		return fmt.Errorf("failed to redial host: %w", err)
	}
	return fn(tc)
}

func (d *Dialer) prices(ctx context.Context, hostKey types.PublicKey) (proto.HostPrices, error) {
	d.mu.Lock()
	if settings, ok := d.cachedSettings[hostKey]; ok && time.Now().Add(5*time.Second).Before(settings.Prices.ValidUntil) {
		d.mu.Unlock()
		return settings.Prices, nil
	}
	d.mu.Unlock()

	var settings proto.HostSettings
	err := d.retry(ctx, hostKey, func(tc rhp.TransportClient) (err error) {
		settings, err = rhp.RPCSettings(ctx, tc)
		return err
	})
	if err != nil {
		return proto.HostPrices{}, err
	}

	d.mu.Lock()
	d.cachedSettings[hostKey] = settings
	d.mu.Unlock()

	return settings.Prices, nil
}

// WriteSector implements the [HostDialer] interface.
func (d *Dialer) WriteSector(ctx context.Context, hostKey types.PublicKey, sector *[proto.SectorSize]byte) (types.Hash256, error) {
	ctx, cancel, err := d.tg.AddContext(ctx)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to add thread to thread group: %w", err)
	}
	defer cancel()

	prices, err := d.prices(ctx, hostKey)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to get prices: %w", err)
	}

	var result rhp.RPCWriteSectorResult
	err = d.retry(ctx, hostKey, func(tc rhp.TransportClient) (err error) {
		token := proto.NewAccountToken(d.appKey, hostKey)
		result, err = rhp.RPCWriteSector(ctx, tc, prices, token, bytes.NewReader(sector[:]), proto.SectorSize)
		return
	})
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to write sector: %w", err)
	}

	return result.Root, nil
}

// ReadSector implements the [HostDialer] interface.
func (d *Dialer) ReadSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) (*[proto.SectorSize]byte, error) {
	ctx, cancel, err := d.tg.AddContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to add thread to thread group: %w", err)
	}
	defer cancel()

	prices, err := d.prices(ctx, hostKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get prices: %w", err)
	}

	var buf bytes.Buffer
	err = d.retry(ctx, hostKey, func(tc rhp.TransportClient) (err error) {
		token := proto.NewAccountToken(d.appKey, hostKey)
		_, err = rhp.RPCReadSector(ctx, tc, prices, token, &buf, sectorRoot, 0, proto.SectorSize)
		return
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sector: %w", err)
	} else if buf.Len() != proto.SectorSize {
		return nil, fmt.Errorf("did not receive full sector: expected length %d, got %d", proto.SectorSize, buf.Len())
	}

	var result [proto.SectorSize]byte
	copy(result[:], buf.Bytes())
	return &result, nil
}
