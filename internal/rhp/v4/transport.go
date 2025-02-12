package rhp

import (
	"context"
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
)

type transportPool struct {
	mu   sync.Mutex
	pool map[string]*transport
}

func newTransportPool() *transportPool {
	return &transportPool{
		pool: make(map[string]*transport),
	}
}

func (p *transportPool) withTransport(ctx context.Context, hk types.PublicKey, addr string, fn func(rhp.TransportClient) error) (err error) {
	// fetch or create transport
	p.mu.Lock()
	t, found := p.pool[addr]
	if !found {
		t = &transport{}
		p.pool[addr] = t
	}
	t.refCount++
	p.mu.Unlock()

	// execute function
	err = func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic (withTransport): %v", r)
			}
		}()
		client, err := t.Dial(ctx, hk, addr)
		if err != nil {
			return err
		}
		return fn(client)
	}()

	// Decrement refcounter again and clean up pool.
	p.mu.Lock()
	t.refCount--
	if t.refCount == 0 {
		// Cleanup
		if t.t != nil {
			_ = t.t.Close()
			t.t = nil
		}
		delete(p.pool, addr)
	}
	p.mu.Unlock()
	return err
}

type transport struct {
	refCount uint64 // locked by pool

	mu sync.Mutex
	t  rhp.TransportClient
}

// DialStream dials a new stream on the transport.
func (t *transport) Dial(ctx context.Context, hk types.PublicKey, addr string) (rhp.TransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.t == nil {
		newTransport, err := rhp.DialSiaMux(ctx, addr, hk)
		if err != nil {
			return nil, fmt.Errorf("failed to upgrade connection: %w", err)
		}
		t.t = newTransport
	}
	return t.t, nil
}
