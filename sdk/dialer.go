package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/api"
)

var _ HostDialer = (*Dialer)(nil)

// Dialer implements the HostDialer interface.
type Dialer struct {
	mu sync.Mutex

	c      AppClient
	appKey types.PrivateKey

	addrs          map[types.PublicKey][]chain.NetAddress
	conns          map[types.PublicKey]rhp.TransportClient
	cachedSettings map[types.PublicKey]proto.HostSettings
}

// NewDialer returns a new Dialer.
func NewDialer(c AppClient, appKey types.PrivateKey) *Dialer {
	return &Dialer{
		c:      c,
		appKey: appKey,

		addrs:          make(map[types.PublicKey][]chain.NetAddress),
		conns:          make(map[types.PublicKey]rhp.TransportClient),
		cachedSettings: make(map[types.PublicKey]proto.HostSettings),
	}
}

// Close closes all the open connections on the dialer.
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, conn := range d.conns {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	clear(d.conns)

	return nil
}

// Hosts returns the public keys of all hosts that are available for
// upload or download.
func (d *Dialer) Hosts(ctx context.Context) ([]types.PublicKey, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	clear(d.addrs)
	offset, limit := 0, 100
	var pks []types.PublicKey
	for {
		hosts, err := d.c.Hosts(ctx, api.WithOffset(offset), api.WithLimit(limit))
		if err != nil {
			return nil, err
		}

		for _, host := range hosts {
			pks = append(pks, host.PublicKey)
			d.addrs[host.PublicKey] = host.Addresses
		}

		offset += len(hosts)
		if len(hosts) < limit {
			break
		}
	}
	return pks, nil
}

func (d *Dialer) dialHost(ctx context.Context, hostKey types.PublicKey, reuse bool) (rhp.TransportClient, error) {
	if tc, ok := d.conns[hostKey]; ok && reuse {
		// we have an existing connection
		return tc, nil
	}

	h, ok := d.addrs[hostKey]
	if !ok {
		return nil, fmt.Errorf("we haven't seen host")
	}

	for _, addr := range h {
		if addr.Protocol == siamux.Protocol {
			tc, err := siamux.Dial(ctx, addr.Address, hostKey)
			if err != nil {
				return nil, fmt.Errorf("failed to dial host over siamux: %w", err)
			}
			d.conns[hostKey] = tc
			return tc, nil
		} else if addr.Protocol == quic.Protocol {
			tc, err := quic.Dial(ctx, addr.Address, hostKey)
			if err != nil {
				return nil, fmt.Errorf("failed to dial host over quic: %w", err)
			}
			d.conns[hostKey] = tc
			return tc, nil
		}
	}
	return nil, errors.New("host has no supported protocols")
}

func (d *Dialer) retry(ctx context.Context, hostKey types.PublicKey, fn func(rhp.TransportClient) error) error {
	// reuse connection if we can
	d.mu.Lock()
	tc, err := d.dialHost(ctx, hostKey, true)
	if err != nil {
		d.mu.Unlock()
		return fmt.Errorf("failed to dial host: %w", err)
	}
	d.mu.Unlock()

	if err := fn(tc); err != nil && proto.ErrorCode(err) != proto.ErrorCodeTransport {
		// we received a non transport related error
		return err
	} else if err == nil {
		// success
		return nil
	}

	// we got a transport error, try reconnecting and trying again
	d.mu.Lock()
	tc, err = d.dialHost(ctx, hostKey, false)
	if err != nil {
		d.mu.Unlock()
		return fmt.Errorf("failed to redial host: %w", err)
	}
	d.mu.Unlock()

	return fn(tc)
}

func (d *Dialer) prices(ctx context.Context, hostKey types.PublicKey) (proto.HostPrices, error) {
	d.mu.Lock()
	if settings, ok := d.cachedSettings[hostKey]; ok && time.Now().Before(settings.Prices.ValidUntil) {
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
