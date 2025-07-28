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
	"go.sia.tech/indexd/api"
	"go.uber.org/zap"
)

var _ HostDialer = (*Dialer)(nil)

type connEntry struct {
	mu   sync.Mutex
	dial chan struct{} // signals dial completion
	tc   rhp.TransportClient
}

// Dialer implements the HostDialer interface.
type Dialer struct {
	mu  sync.Mutex
	log *zap.Logger

	c      AppClient
	appKey types.PrivateKey

	addrs          map[types.PublicKey][]chain.NetAddress
	conns          map[types.PublicKey]*connEntry
	cachedSettings map[types.PublicKey]proto.HostSettings
}

// NewDialer returns a new Dialer.
func NewDialer(c AppClient, appKey types.PrivateKey, log *zap.Logger) *Dialer {
	return &Dialer{
		log: log,

		c:      c,
		appKey: appKey,

		addrs:          make(map[types.PublicKey][]chain.NetAddress),
		conns:          make(map[types.PublicKey]*connEntry),
		cachedSettings: make(map[types.PublicKey]proto.HostSettings),
	}
}

// Start starts the goroutine that periodically refreshes the map of host
// addresses
func (d *Dialer) Start(ctx context.Context) (func(), error) {
	if err := d.updateHosts(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(10 * time.Minute)

	go func() {
		for {
			select {
			case <-ticker.C:
				if err := d.updateHosts(ctx); err != nil {
					d.log.Warn("Failed to refresh hosts list", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		ticker.Stop()
		cancel()
	}, nil
}

// Close closes all the open connections on the dialer.
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var errs []error
	for _, entry := range d.conns {
		entry.mu.Lock()
		if entry.tc != nil {
			if err := entry.tc.Close(); err != nil {
				errs = append(errs, err)
			}
			entry.tc = nil
		}
		entry.mu.Unlock()
	}
	clear(d.conns)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (d *Dialer) updateHosts(ctx context.Context) error {
	offset, limit := 0, 100
	addrs := make(map[types.PublicKey][]chain.NetAddress)
	for {
		hosts, err := d.c.Hosts(ctx, api.WithOffset(offset), api.WithLimit(limit))
		if err != nil {
			return err
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

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.tc != nil {
		entry.tc.Close()
		entry.tc = nil
	}
}

// Hosts returns the public keys of all hosts that are available for
// upload or download.
func (d *Dialer) Hosts() []types.PublicKey {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Collect(maps.Keys(d.addrs))
}

func (d *Dialer) dialHost(ctx context.Context, hostKey types.PublicKey) (rhp.TransportClient, error) {
	// Get or create connection entry
	d.mu.Lock()
	entry, exists := d.conns[hostKey]
	if !exists {
		entry = &connEntry{}
		d.conns[hostKey] = entry
	}
	d.mu.Unlock()

	entry.mu.Lock()

	// Wait for any ongoing dial to complete
	for entry.dial != nil {
		entry.mu.Unlock()
		select {
		case <-entry.dial:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		entry.mu.Lock()
	}

	// Return existing connection if available
	if entry.tc != nil {
		tc := entry.tc
		entry.mu.Unlock()
		return tc, nil
	}

	// Start new dial operation
	entry.dial = make(chan struct{})
	entry.mu.Unlock()

	// Actually dial outside the lock
	var tc rhp.TransportClient
	var err error
	defer func() {
		entry.mu.Lock()
		if err == nil {
			entry.tc = tc
		}
		close(entry.dial)
		entry.dial = nil
		entry.mu.Unlock()
	}()

	// Find valid address and dial
	h, ok := d.addrs[hostKey]
	if !ok {
		return nil, fmt.Errorf("missing host with key: %v", hostKey)
	}

	for _, addr := range h {
		if addr.Protocol == quic.Protocol {
			tc, err = quic.Dial(ctx, addr.Address, hostKey)
			if err == nil {
				return tc, nil
			}
		}
	}
	for _, addr := range h {
		if addr.Protocol == siamux.Protocol {
			tc, err = siamux.Dial(ctx, addr.Address, hostKey)
			if err == nil {
				return tc, nil
			}
		}
	}

	return nil, errors.New("host has no supported protocols")
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
		return proto.HostPrices{}, nil
	}

	d.mu.Lock()
	d.cachedSettings[hostKey] = settings
	d.mu.Unlock()

	return settings.Prices, nil
}

// WriteSector writes a sector to the host identified by the public key.
func (d *Dialer) WriteSector(ctx context.Context, hostKey types.PublicKey, sector *[proto.SectorSize]byte) (types.Hash256, error) {
	prices, err := d.prices(ctx, hostKey)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to get prices: %w", err)
	}

	var result rhp.RPCWriteSectorResult
	err = d.retry(ctx, hostKey, func(tc rhp.TransportClient) (err error) {
		token := (&proto.Account{}).Token(d.appKey, hostKey)
		result, err = rhp.RPCWriteSector(ctx, tc, prices, token, bytes.NewReader(sector[:]), proto.SectorSize)
		return
	})
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to write sector: %w", err)
	}

	return result.Root, nil
}

// ReadSector reads a sector from the host identified by the public key.
func (d *Dialer) ReadSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) (*[proto.SectorSize]byte, error) {
	prices, err := d.prices(ctx, hostKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get prices: %w", err)
	}

	var buf bytes.Buffer
	err = d.retry(ctx, hostKey, func(tc rhp.TransportClient) (err error) {
		token := (&proto.Account{}).Token(d.appKey, hostKey)
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
