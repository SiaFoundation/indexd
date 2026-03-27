package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/mux/v3"
	"go.uber.org/zap"
)

type transport struct {
	mu     sync.Mutex
	dialCh chan struct{} // signals dial completion
	tc     rhp.TransportClient
}

func (t *transport) close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tc != nil {
		t.tc.Close()
	}
	t.tc = nil
}

func (t *transport) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tc != nil {
		t.tc.Close()
		t.tc = nil
	}
}

func (t *transport) dial(ctx context.Context, hostKey types.PublicKey, addresses []chain.NetAddress) (rhp.TransportClient, error) {
	t.mu.Lock()
	if t.tc != nil {
		// use cached transport
		defer t.mu.Unlock()
		return t.tc, nil
	}

	dialCh := t.dialCh
	for dialCh != nil {
		// wait for ongoing dial to complete
		t.mu.Unlock()

		select {
		case <-dialCh:
			// check if the transport was established
			t.mu.Lock()
			if t.tc != nil {
				tc := t.tc
				t.mu.Unlock()
				return tc, nil
			}
			dialCh = t.dialCh
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// start a new dial
	dialCh = make(chan struct{})
	defer func() {
		t.mu.Lock()
		t.dialCh = nil
		close(dialCh)
		t.mu.Unlock()
	}()
	t.dialCh = dialCh
	t.tc = nil
	t.mu.Unlock()

	var wg sync.WaitGroup
	sema := make(chan struct{}, 2)
	var dialCtx, dialCancel = context.WithCancel(ctx)
	defer dialCancel()

	connectErrs := make([]error, len(addresses))

top:
	for i, addr := range addresses {
		select {
		case <-dialCtx.Done():
			break top
		case sema <- struct{}{}:
		}
		wg.Add(1)
		go func(i int, addr chain.NetAddress) {
			defer func() {
				<-sema
				wg.Done()
			}()
			var transport rhp.TransportClient
			var err error
			switch addr.Protocol {
			case siamux.Protocol:
				transport, err = siamux.Dial(dialCtx, addr.Address, hostKey)
			// TODO: re-enable QUIC once we have increased the max streams on the host side
			/*case quic.Protocol:
			transport, err = quic.Dial(dialCtx, addr.Address, hostKey)*/
			default:
				return
			}
			connectErrs[i] = err
			if err != nil || dialCtx.Err() != nil {
				// failed to connect or already connected elsewhere
				if err == nil {
					_ = transport.Close()
				}
				return
			}
			dialCancel()
			t.mu.Lock()
			if t.tc == nil {
				t.tc = transport
			} else {
				_ = transport.Close()
			}
			t.mu.Unlock()
		}(i, addr)
	}
	// wait for all dial attempts to finish
	wg.Wait()
	// cache the transport
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tc == nil {
		return nil, fmt.Errorf("failed to connect to host %s (%w)", hostKey.String(), errors.Join(connectErrs...))
	}
	return t.tc, nil
}

// A Client is used to interact with Sia hosts over RHP4.
type Client struct {
	log   *zap.Logger
	tg    *threadgroup.ThreadGroup
	hosts *Provider

	mu           sync.Mutex // protects the fields below
	cachedPrices map[types.PublicKey]proto.HostPrices
	transports   map[types.PublicKey]*transport
}

func (c *Client) resetTransport(hostKey types.PublicKey) {
	c.mu.Lock()
	t := c.transports[hostKey]
	if t != nil {
		t.reset()
	}
	c.mu.Unlock()
}

// hostTransport opens a transport connection to the specified host.
func (c *Client) hostTransport(ctx context.Context, hostKey types.PublicKey) (rhp.TransportClient, error) {
	addresses, err := c.hosts.Addresses(hostKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get host addresses: %w", err)
	} else if len(addresses) == 0 {
		return nil, errors.New("no addresses found for host")
	}

	c.mu.Lock()
	t := c.transports[hostKey]
	if t == nil {
		t = &transport{}
		c.transports[hostKey] = t
	}
	c.mu.Unlock()
	return t.dial(ctx, hostKey, addresses)
}

func (c *Client) rpcFn(ctx context.Context, hostKey types.PublicKey, fn func(ctx context.Context, transport rhp.TransportClient) error) (err error) {
	defer func() {
		// increment the failed RPC count for the host if the RPC failed
		if err != nil {
			c.hosts.AddFailedRPC(hostKey, err)
		}
	}()

	transport, err := c.hostTransport(ctx, hostKey)
	if err != nil {
		return fmt.Errorf("failed to get transport: %w", err)
	}

	err = fn(ctx, transport)
	if err == nil {
		return nil
	} else if shouldResetTransport(err) {
		c.log.Debug("resetting transport", zap.Stringer("host", hostKey), zap.Error(err))
		c.resetTransport(hostKey)
	}

	// decorate ErrClosedStream with the context error if it exists since
	// ErrClosedStream is usually a consequence of context cancellation or timeout
	if errors.Is(err, mux.ErrClosedStream) && context.Cause(ctx) != nil {
		err = fmt.Errorf("%w: %w", err, context.Cause(ctx))
	}

	return err
}

// AccountBalance fetches the account balance from the specified host.
func (c *Client) AccountBalance(ctx context.Context, hostKey types.PublicKey, accountKey proto.Account) (balance types.Currency, err error) {
	err = c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		balance, err = rhp.RPCAccountBalance(ctx, transport, accountKey)
		return err
	})
	return
}

// Prices fetches the host prices from the specified host.
//
// If the prices are cached and valid, the cached prices are returned.
func (c *Client) Prices(ctx context.Context, hostKey types.PublicKey) (prices proto.HostPrices, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return proto.HostPrices{}, err
	}
	defer done()

	err = c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err = c.prices(ctx, hostKey, transport)
		return err
	})
	return
}

// WriteSector writes a sector to the specified host.
//
// It returns the verified Merkle root of the written sector.
func (c *Client) WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (result rhp.RPCWriteSectorResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCWriteSectorResult{}, err
	}
	defer done()

	start := time.Now()
	err = c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.prices(ctx, hostKey, transport)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		token := proto.NewAccountToken(accountKey, hostKey)
		result, err = rhp.RPCWriteSector(ctx, transport, prices, token, bytes.NewReader(data), uint64(len(data)))
		return err
	})
	if err == nil {
		c.hosts.AddWriteSample(hostKey, time.Since(start))
	}
	return
}

// ReadSector writes the data of a sector from the specified host into w.
// If an error is returned, the contents of w must be discarded.
func (c *Client) ReadSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, root types.Hash256, w io.Writer, offset, length uint64) (result rhp.RPCReadSectorResult, err error) {
	done, err := c.tg.Add()
	if err != nil {
		return rhp.RPCReadSectorResult{}, err
	}
	defer done()

	start := time.Now()
	err = c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		prices, err := c.prices(ctx, hostKey, transport)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		token := proto.NewAccountToken(accountKey, hostKey)
		result, err = rhp.RPCReadSector(ctx, transport, prices, token, w, root, offset, length)
		return err
	})
	if err == nil {
		c.hosts.AddReadSample(hostKey, time.Since(start))
	}
	return
}

// Candidates returns all host candidates ordered by their
// historical performance.
func (c *Client) Candidates() (*Candidates, error) {
	return c.hosts.Candidates()
}

// UploadCandidates returns host candidates that are good for uploading,
// ordered by their historical performance.
func (c *Client) UploadCandidates() (*Candidates, error) {
	return c.hosts.UploadCandidates()
}

// Prioritize reorders the given hosts based on their historical performance.
// The reordered slice is returned with unusable hosts removed.
func (c *Client) Prioritize(hosts []types.PublicKey) []types.PublicKey {
	return c.hosts.Prioritize(hosts)
}

// Close waits for all inflight RPCs to complete then closes all open transports.
func (c *Client) Close() error {
	c.tg.Stop()

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, transport := range c.transports {
		transport.close()
	}
	return nil
}

// prices fetches the host prices using an existing transport connection.
//
// If the prices are cached and valid, the cached prices are returned.
func (c *Client) prices(ctx context.Context, hostKey types.PublicKey, transport rhp.TransportClient) (proto.HostPrices, error) {
	c.mu.Lock()
	prices := c.cachedPrices[hostKey]
	if prices.Validate(hostKey) == nil && time.Until(prices.ValidUntil) > 30*time.Second {
		c.mu.Unlock()
		return prices, nil
	}
	c.mu.Unlock()

	settings, err := rhp.RPCSettings(ctx, transport)
	if err != nil {
		return proto.HostPrices{}, err
	}
	if settings.Prices.Validate(hostKey) == nil {
		c.mu.Lock()
		c.cachedPrices[hostKey] = settings.Prices
		c.mu.Unlock()
	}
	return settings.Prices, nil
}

func shouldResetTransport(err error) bool {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	case errors.Is(err, mux.ErrClosedStream):
		// ErrClosedStream indicates that we closed the stream gracefully, so
		// the transport is still intact. This usually happens because we close
		// streams by cancelling or timing out contexts.
		return false
	case errors.Is(err, os.ErrDeadlineExceeded):
		// os.ErrDeadlineExceeded indicates that the stream hit a timeout which was set
		// using SetDeadline. In this case, the mux is still healthy.
		return false
	default:
		return proto.ErrorCode(err) == proto.ErrorCodeTransport
	}
}

// New creates a new Client.
func New(hosts *Provider, log *zap.Logger) *Client {
	return &Client{
		log:          log,
		tg:           threadgroup.New(),
		hosts:        hosts,
		cachedPrices: make(map[types.PublicKey]proto.HostPrices),
		transports:   make(map[types.PublicKey]*transport),
	}
}
