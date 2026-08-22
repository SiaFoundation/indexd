package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
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

// defaultConnectTimeout bounds connection establishment separately from the
// RPC context, so an unresponsive host costs seconds instead of the full RPC
// budget.
const defaultConnectTimeout = 10 * time.Second

var (
	errTransportDropped = errors.New("transport was dropped before use")
	errTransportClosed  = errors.New("transport is closed")
)

type transport struct {
	connectTimeout time.Duration

	mu      sync.Mutex
	dialCh  chan struct{} // signals dial completion
	closed  bool
	current *transportRef
	open    map[*transportRef]struct{}
}

// transportRef is a counted reference to a cached transport.
type transportRef struct {
	tc       rhp.TransportClient
	refs     int
	detached bool
	closed   bool
}

func newTransport(connectTimeout time.Duration) *transport {
	return &transport{
		connectTimeout: connectTimeout,
		open:           make(map[*transportRef]struct{}),
	}
}

// close closes every transport the host has open. Later dials are refused
// rather than cached.
func (t *transport) close() {
	t.mu.Lock()
	t.closed = true
	clients := make([]rhp.TransportClient, 0, len(t.open))
	for ref := range t.open {
		ref.detached = true
		if t.closeLocked(ref) {
			clients = append(clients, ref.tc)
		}
	}
	t.current = nil
	t.mu.Unlock()

	for _, tc := range clients {
		_ = tc.Close()
	}
}

func (t *transport) addLocked(tc rhp.TransportClient) *transportRef {
	ref := &transportRef{tc: tc}
	t.open[ref] = struct{}{}
	t.current = ref
	return ref
}

func (t *transport) closeLocked(ref *transportRef) bool {
	if ref.closed {
		return false
	}
	ref.closed = true
	delete(t.open, ref)
	return true
}

// detach stops handing ref's transport to new RPCs and reports whether it was
// cached. The transport stays open until the last reference to it is released.
func (t *transport) detach(ref *transportRef) bool {
	t.mu.Lock()
	if ref == nil || t.current != ref {
		t.mu.Unlock()
		return false
	}
	t.current = nil
	ref.detached = true
	shouldClose := ref.refs == 0 && t.closeLocked(ref)
	t.mu.Unlock()

	if shouldClose {
		_ = ref.tc.Close()
	}
	return true
}

// release drops a reference, closing the transport if it was detached.
func (t *transport) release(ref *transportRef) {
	t.mu.Lock()
	if ref.refs <= 0 {
		t.mu.Unlock()
		panic("released transport without a reference") // developer error
	}
	ref.refs--
	shouldClose := ref.detached && ref.refs == 0 && t.closeLocked(ref)
	t.mu.Unlock()

	if shouldClose {
		_ = ref.tc.Close()
	}
}

func (t *transport) acquireLocked() *transportRef {
	t.current.refs++
	return t.current
}

func (t *transport) dial(ctx context.Context, hostKey types.PublicKey, addresses []chain.NetAddress) (*transportRef, error) {
	var dialCh chan struct{}
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return nil, errTransportClosed
		} else if t.current != nil {
			ref := t.acquireLocked()
			t.mu.Unlock()
			return ref, nil
		} else if t.dialCh == nil {
			dialCh = make(chan struct{})
			t.dialCh = dialCh
			t.mu.Unlock()
			break
		}
		dialCh = t.dialCh
		t.mu.Unlock()

		select {
		case <-dialCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	var wg sync.WaitGroup
	sema := make(chan struct{}, 2)
	var dialCtx, dialCancel = context.WithTimeout(ctx, t.connectTimeout)
	defer dialCancel()

	connectErrs := make([]error, len(addresses))
	var dialed bool // guarded by t.mu

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
				connectErrs[i] = fmt.Errorf("unsupported protocol %q", addr.Protocol)
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
			keep := !dialed && !t.closed
			if keep {
				t.addLocked(transport)
				dialed = true
			}
			t.mu.Unlock()
			if !keep {
				_ = transport.Close()
			}
		}(i, addr)
	}
	// wait for all dial attempts to finish
	wg.Wait()

	t.mu.Lock()
	t.dialCh = nil
	close(dialCh)
	if t.current != nil {
		ref := t.acquireLocked()
		t.mu.Unlock()
		return ref, nil
	}
	closed, dropped := t.closed, dialed
	t.mu.Unlock()

	if closed {
		return nil, errTransportClosed
	} else if dropped {
		return nil, fmt.Errorf("%w: host %s", errTransportDropped, hostKey.String())
	} else if err := errors.Join(connectErrs...); err != nil {
		return nil, fmt.Errorf("failed to connect to host %s (%w)", hostKey.String(), err)
	}
	// connectErrs is empty when no attempt finished before the connect timeout
	return nil, fmt.Errorf("failed to connect to host %s (%w)", hostKey.String(), dialCtx.Err())
}

// A Client is used to interact with Sia hosts over RHP4.
type Client struct {
	log             *zap.Logger
	tg              *threadgroup.ThreadGroup
	hosts           *Provider
	heightTolerance uint64

	mu             sync.Mutex // protects the fields below
	cachedSettings map[types.PublicKey]proto.HostSettings
	transports     map[types.PublicKey]*transport
}

// hostTransport opens a transport connection to the specified host, returning
// the host's transport and a reference to the connection to use.
func (c *Client) hostTransport(ctx context.Context, hostKey types.PublicKey) (*transport, *transportRef, error) {
	addresses, err := c.hosts.Addresses(hostKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get host addresses: %w", err)
	} else if len(addresses) == 0 {
		return nil, nil, errors.New("no addresses found for host")
	}

	c.mu.Lock()
	t := c.transports[hostKey]
	if t == nil {
		t = newTransport(defaultConnectTimeout)
		c.transports[hostKey] = t
	}
	c.mu.Unlock()

	ref, err := t.dial(ctx, hostKey, addresses)
	if err != nil {
		return nil, nil, err
	}
	return t, ref, nil
}

func isFailedRPC(ctx context.Context, err error) bool {
	switch {
	case ctx.Err() != nil:
		return false // interrupted RPC
	case errors.Is(err, proto.ErrSectorNotFound), errors.Is(err, proto.ErrSectorCorrupt):
		return false
	case errors.Is(err, errTransportDropped), errors.Is(err, errTransportClosed):
		return false // the client dropped the transport, not the host
	default:
		return err != nil
	}
}

// streamTransport records whether an RPC opened a stream on the transport,
// separating a deadline that expired before the RPC reached the network from
// one that expired on the wire.
type streamTransport struct {
	rhp.TransportClient
	dialed atomic.Bool
}

func (t *streamTransport) DialStream(ctx context.Context) (net.Conn, error) {
	s, err := t.TransportClient.DialStream(ctx)
	if err != nil {
		return nil, err
	}
	t.dialed.Store(true)
	return s, nil
}

// isStalledRPC reports whether an RPC failed because its deadline expired.
// Cancellation is excluded because speculative reads are routinely cancelled
// after another host wins the race.
func isStalledRPC(ctx context.Context, err error) bool {
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, mux.ErrClosedStream)
}

func (c *Client) rpcFn(ctx context.Context, hostKey types.PublicKey, fn func(ctx context.Context, transport rhp.TransportClient) error) (err error) {
	// nothing can be blamed on the host if the RPC never started
	if err := ctx.Err(); err != nil {
		return err
	}

	defer func() {
		// increment the failed RPC count if the RPC is considered failed
		if isFailedRPC(ctx, err) {
			c.hosts.AddFailedRPC(hostKey)
		}
	}()

	t, ref, err := c.hostTransport(ctx, hostKey)
	if err != nil {
		return fmt.Errorf("failed to get transport: %w", err)
	}
	defer t.release(ref)

	if err := ctx.Err(); err != nil {
		// no RPC used the transport, so leave it cached
		return err
	}

	transport := &streamTransport{TransportClient: ref.tc}
	err = fn(ctx, transport)
	if err == nil {
		return nil
	}

	stalled := transport.dialed.Load() && isStalledRPC(ctx, err)
	if stalled || shouldResetTransport(err) {
		c.log.Debug("dropping transport", zap.Bool("stalled", stalled), zap.Stringer("host", hostKey), zap.Error(err))
		t.detach(ref)
	}

	return err
}

// AddFailedRPC records a failed RPC attempt to the specified host, lowering its
// priority in future queues.
// NOTE: This should only be used after an RPC failed due to context
// cancellation or a context deadline as those failures won't be recorded by the
// client implicitly. The client can't know whether the caller intends to treat
// those as failed RPCs or not and punishing a good host for a cancelled RPC
// could severely degrade performance.
func (c *Client) AddFailedRPC(hostKey types.PublicKey) {
	c.hosts.AddFailedRPC(hostKey)
}

// AddTimedOutRPC records an RPC that burned its full deadline without
// completing, demoting the host and recording the worst-case throughput it
// implies. Prefer it over [Client.AddFailedRPC] whenever the caller knows its
// own deadline expired: without a throughput sample the host stays in the
// unsampled bucket that outranks every measured host.
//
// The same NOTE as AddFailedRPC applies — only the caller can tell whether its
// deadline expiring means the host misbehaved, since a parent deadline
// propagates context.DeadlineExceeded down to the RPC's own context.
func (c *Client) AddTimedOutRPC(hostKey types.PublicKey, write bool, bytes uint64, elapsed time.Duration) {
	c.hosts.AddTimedOutRPC(hostKey, write, bytes, elapsed)
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

	var settings proto.HostSettings
	var cached bool
	start := time.Now()
	err = c.rpcFn(ctx, hostKey, func(ctx context.Context, transport rhp.TransportClient) error {
		settings, cached, err = c.settings(ctx, hostKey, transport)
		return err
	})
	if err == nil && !cached {
		c.hosts.AddSettingsSample(hostKey, time.Since(start))
	}
	prices = settings.Prices
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
		settings, _, err := c.settings(ctx, hostKey, transport)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		token := proto.NewAccountToken(accountKey, hostKey)
		result, err = rhp.RPCWriteSector(ctx, transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
		return err
	})
	if err == nil {
		c.hosts.AddWriteSample(hostKey, uint64(len(data)), time.Since(start))
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
		settings, _, err := c.settings(ctx, hostKey, transport)
		if err != nil {
			return fmt.Errorf("failed to get host prices: %w", err)
		}
		token := proto.NewAccountToken(accountKey, hostKey)
		result, err = rhp.RPCReadSector(ctx, transport, settings.Prices, token, w, root, offset, length)
		return err
	})
	if err == nil {
		c.hosts.AddReadSample(hostKey, length, time.Since(start))
	}
	return
}

// HostQueue returns all usable hosts ordered by their historical
// performance.
func (c *Client) HostQueue() (*HostQueue, error) {
	return c.hosts.HostQueue()
}

// UploadQueue returns hosts that are good for uploading, ordered
// by their historical performance.
func (c *Client) UploadQueue() (*HostQueue, error) {
	return c.hosts.UploadQueue()
}

// Prioritize reorders the given hosts based on their historical performance.
// The reordered slice is returned with unusable hosts removed.
func (c *Client) Prioritize(hosts []types.PublicKey) []types.PublicKey {
	return c.hosts.Prioritize(hosts)
}

// ReadEstimate returns the expected time to read the given number of bytes
// based on the network-wide observed read throughput. See
// [Provider.ReadEstimate].
func (c *Client) ReadEstimate(bytes uint64) time.Duration {
	return c.hosts.ReadEstimate(bytes)
}

// WriteEstimate returns the expected time to write the given number of bytes
// based on the network-wide observed write throughput. See
// [Provider.WriteEstimate].
func (c *Client) WriteEstimate(bytes uint64) time.Duration {
	return c.hosts.WriteEstimate(bytes)
}

// PickWrite atomically selects the highest-scoring write candidate and
// reserves an inflight slot. See [Provider.PickWrite].
func (c *Client) PickWrite(candidates []types.PublicKey) (host types.PublicKey, release func(), remaining []types.PublicKey, ok bool) {
	return c.hosts.PickWrite(candidates)
}

// PickReads atomically sorts candidates and reserves inflight read
// slots on the top n. See [Provider.PickReads].
func (c *Client) PickReads(candidates []types.PublicKey, n int) (picked []types.PublicKey, releases []func(), remaining []types.PublicKey) {
	return c.hosts.PickReads(candidates, n)
}

// TrackInflightRead increments the host's inflight read counter and
// returns a function that decrements it. See [Provider.TrackInflightRead].
func (c *Client) TrackInflightRead(hostKey types.PublicKey) func() {
	return c.hosts.TrackInflightRead(hostKey)
}

// TrackInflightWrite increments the host's inflight write counter and
// returns a function that decrements it. See [Provider.TrackInflightWrite].
func (c *Client) TrackInflightWrite(hostKey types.PublicKey) func() {
	return c.hosts.TrackInflightWrite(hostKey)
}

// WarmConnections establishes siamux connections and fetches prices from
// good for upload hosts concurrently, seeding latency metrics for host ordering.
func (c *Client) WarmConnections(ctx context.Context) error {
	// fetch usable hosts
	his, err := c.hosts.UsableHosts()
	if err != nil {
		c.log.Error("failed to warm connections", zap.Error(err))
		return err
	}

	// filter it down to good for upload hosts
	var hosts []types.PublicKey
	for _, hi := range his {
		if hi.GoodForUpload {
			hosts = append(hosts, hi.PublicKey)
		}
	}
	if len(hosts) == 0 {
		return nil
	}

	// add ourselves once to avoid errors on close
	cancel, err := c.tg.Add()
	if err != nil {
		return err
	}
	defer cancel()

	// warm connections in parallel with a limit of 15 concurrent dials
	var wg sync.WaitGroup
	var warmed atomic.Uint64
	sema := make(chan struct{}, 15)
	for _, hk := range hosts {
		select {
		case <-c.tg.Done():
			return nil
		case sema <- struct{}{}:
		}

		wg.Add(1)
		go func(hk types.PublicKey) {
			defer func() {
				<-sema
				wg.Done()
			}()

			pCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, err := c.Prices(pCtx, hk)
			cancel()

			if err == nil {
				warmed.Add(1)
			}
		}(hk)
	}

	// wait for all warmups to complete
	wg.Wait()

	c.log.Debug("warmed connections", zap.Uint64("warmed", warmed.Load()), zap.Int("total", len(hosts)))
	return nil
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

// ErrPricesHeightDrift is returned when the tip height a host claims in its
// signed prices is too far behind the local tip height.
var ErrPricesHeightDrift = errors.New("host prices tip height too far behind local tip height")

// checkPricesHeight verifies that the tip height a host claims in its signed
// prices is no more than the client's height tolerance behind the local tip
// height.
func (c *Client) checkPricesHeight(prices proto.HostPrices, localHeight uint64) error {
	if prices.TipHeight < localHeight && localHeight-prices.TipHeight > c.heightTolerance {
		return fmt.Errorf("%w: host height %d, local height %d, tolerance %d", ErrPricesHeightDrift, prices.TipHeight, localHeight, c.heightTolerance)
	}
	return nil
}

// refreshSettings fetches the host settings from the host, bypassing the
// cache. Valid settings are cached for future use.
func (c *Client) refreshSettings(ctx context.Context, hostKey types.PublicKey, transport rhp.TransportClient) (proto.HostSettings, error) {
	settings, err := rhp.RPCSettings(ctx, transport)
	if err != nil {
		return proto.HostSettings{}, err
	} else if err := settings.Prices.Validate(hostKey); err != nil {
		return proto.HostSettings{}, fmt.Errorf("host returned invalid prices: %w", err)
	}

	c.mu.Lock()
	c.cachedSettings[hostKey] = settings
	c.mu.Unlock()
	return settings, nil
}

// settings fetches the host settings using an existing transport connection.
//
// If the settings are cached and valid, the cached settings are returned with a
// boolean flag set to 'true'. Otherwise, they are refreshed from the host.
func (c *Client) settings(ctx context.Context, hostKey types.PublicKey, transport rhp.TransportClient) (proto.HostSettings, bool, error) {
	c.mu.Lock()
	settings, ok := c.cachedSettings[hostKey]
	c.mu.Unlock()
	// settings are cached only after signature validation, so the signature never needs
	// rechecking here. this keeps the ed25519 verify off the per-rpc hot path
	if ok && time.Until(settings.Prices.ValidUntil) > 30*time.Second {
		return settings, true, nil
	}

	settings, err := c.refreshSettings(ctx, hostKey, transport)
	if err != nil {
		return proto.HostSettings{}, false, err
	}
	return settings, false, nil
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
		heightTolerance: 36, // ~6 hours of blocks
		log:             log,
		tg:              threadgroup.New(),
		hosts:           hosts,
		cachedSettings:  make(map[types.PublicKey]proto.HostSettings),
		transports:      make(map[types.PublicKey]*transport),
	}
}
