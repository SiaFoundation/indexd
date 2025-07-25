package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/api"
)

var _ HostDialer = (*Dialer)(nil)

type Dialer struct {
	mu sync.Mutex

	c      AppClient
	appKey types.PrivateKey

	addrs map[types.PublicKey][]chain.NetAddress
	conns map[types.PublicKey]rhp.TransportClient
}

func newDialer(c AppClient, appKey types.PrivateKey) *Dialer {
	return &Dialer{
		c:      c,
		appKey: appKey,

		addrs: make(map[types.PublicKey][]chain.NetAddress),
		conns: make(map[types.PublicKey]rhp.TransportClient),
	}
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

func (d *Dialer) settings(ctx context.Context, hostKey types.PublicKey) (rhp.TransportClient, proto.HostPrices, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// reuse connection if we can
	tc, err := d.dialHost(ctx, hostKey, true)
	if err != nil {
		return nil, proto.HostPrices{}, fmt.Errorf("failed to dial host: %w", err)
	}

	settings, err := rhp.RPCSettings(ctx, tc)
	if proto.ErrorCode(err) == proto.ErrorCodeTransport {
		tc, err = d.dialHost(ctx, hostKey, false)
		if err != nil {
			return nil, proto.HostPrices{}, fmt.Errorf("failed to retry connecting to host: %w", err)
		}
		settings, err = rhp.RPCSettings(ctx, tc)
		if err != nil {
			return nil, proto.HostPrices{}, fmt.Errorf("failed to retry getting settings: %w", err)
		}
	} else if err != nil {
		return nil, proto.HostPrices{}, fmt.Errorf("failed to get settings: %w", err)
	}

	return tc, settings.Prices, nil
}

// WriteSector writes a sector to the host identified by the public key.
func (d *Dialer) WriteSector(ctx context.Context, hostKey types.PublicKey, sector *[proto.SectorSize]byte) (types.Hash256, error) {
	tc, prices, err := d.settings(ctx, hostKey)
	if err != nil {
		return types.Hash256{}, err
	}

	token := (&proto.Account{}).Token(d.appKey, hostKey)
	result, err := rhp.RPCWriteSector(ctx, tc, prices, token, bytes.NewReader(sector[:]), proto.SectorSize)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to write sector: %w", err)
	}

	return result.Root, nil
}

// ReadSector reads a sector from the host identified by the public key.
func (d *Dialer) ReadSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) (*[proto.SectorSize]byte, error) {
	tc, prices, err := d.settings(ctx, hostKey)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	token := (&proto.Account{}).Token(d.appKey, hostKey)
	if _, err := rhp.RPCReadSector(ctx, tc, prices, token, &buf, sectorRoot, 0, proto.SectorSize); err != nil {
		return nil, fmt.Errorf("failed to read sector: %w", err)
	} else if buf.Len() != proto.SectorSize {
		return nil, fmt.Errorf("did not receive full sector: expected length %d, got %d", proto.SectorSize, buf.Len())
	}

	var result [proto.SectorSize]byte
	copy(result[:], buf.Bytes())

	return &result, nil
}
