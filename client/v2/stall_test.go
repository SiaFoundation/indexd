package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

const (
	stallDeadline  = 3 * time.Second
	stallTolerance = 10 * time.Second
	stallOvershoot = 2 * time.Second
)

// stallConn gates bytes crossing a net.Conn. While stalled, Read and Write
// park instead of touching the underlying connection, which is what a peer
// that stops draining looks like to the mux.
type stallConn struct {
	net.Conn

	cond         *sync.Cond
	stalled      bool
	released     bool
	parkedWrites int
}

func newStallConn(conn net.Conn) *stallConn {
	return &stallConn{Conn: conn, cond: sync.NewCond(new(sync.Mutex))}
}

func (c *stallConn) gate(write bool) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	for c.stalled && !c.released {
		if write {
			c.parkedWrites++
		}
		c.cond.Broadcast()
		c.cond.Wait()
		if write {
			c.parkedWrites--
		}
	}
}

func (c *stallConn) Read(p []byte) (int, error) {
	c.gate(false)
	return c.Conn.Read(p)
}

func (c *stallConn) Write(p []byte) (int, error) {
	c.gate(true)
	return c.Conn.Write(p)
}

func (c *stallConn) stall() {
	c.cond.L.Lock()
	c.stalled = true
	c.cond.L.Unlock()
}

// release permanently opens the gate so cleanup cannot hang.
func (c *stallConn) release() {
	c.cond.L.Lock()
	c.released = true
	c.cond.Broadcast()
	c.cond.L.Unlock()
}

func (c *stallConn) parkedWriteCalls() int {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	return c.parkedWrites
}

type singleAddrStore struct {
	addr string
}

func (s singleAddrStore) UsableHosts() ([]hosts.HostInfo, error) { return nil, nil }
func (s singleAddrStore) Usable(types.PublicKey) (bool, error)   { return true, nil }
func (s singleAddrStore) Addresses(types.PublicKey) ([]chain.NetAddress, error) {
	return []chain.NetAddress{{Protocol: siamux.Protocol, Address: s.addr}}, nil
}

type stubChain struct{ rhp.ChainManager }

func (stubChain) Tip() types.ChainIndex          { return types.ChainIndex{} }
func (stubChain) TipState() consensus.State      { return consensus.State{} }
func (stubChain) RecommendedFee() types.Currency { return types.ZeroCurrency }

type stubWallet struct{ rhp.Wallet }

type stubContractor struct{ rhp.Contractor }

func (stubContractor) DebitAccount(proto.Account, proto.Usage) error { return nil }

// serveHost starts an RHP4 siamux host that accepts unpaid sector writes.
func serveHost(t *testing.T, hostKey types.PrivateKey, log *zap.Logger) string {
	t.Helper()

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto.HostSettings{
		AcceptingContracts: true,
		MaxCollateral:      types.Siacoins(10000),
		TotalStorage:       1 << 40,
		RemainingStorage:   1 << 40,
	})
	server := rhp.NewServer(hostKey, stubChain{}, stubContractor{}, stubWallet{}, sr, testutil.NewEphemeralSectorStore())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go siamux.Serve(l, server, log.Named("siamux"))
	return l.Addr().String()
}

func TestStalledRPCDropsTransport(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey := types.GeneratePrivateKey()
	hk := hostKey.PublicKey()
	accountKey := types.GeneratePrivateKey()
	hostAddr := serveHost(t, hostKey, log)

	prov := NewProvider(singleAddrStore{addr: hostAddr})
	c := New(prov, log)
	t.Cleanup(func() { _ = c.Close() })

	// install a working transport whose underlying connection can be silenced.
	conn, err := net.Dial("tcp", hostAddr)
	if err != nil {
		t.Fatal(err)
	}
	sc := newStallConn(conn)
	t.Cleanup(sc.release)

	upgradeCtx, cancelUpgrade := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelUpgrade()
	tc, err := siamux.Upgrade(upgradeCtx, sc, hk)
	if err != nil {
		t.Fatal(err)
	}
	tracked := &closeTrackingTransport{TransportClient: tc}

	tr := &transport{connectTimeout: defaultConnectTimeout}
	tr.mu.Lock()
	tr.addLocked(tracked)
	tr.mu.Unlock()
	c.mu.Lock()
	c.transports[hk] = tr
	c.mu.Unlock()

	cachedTransport := func() rhp.TransportClient {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		if tr.current == nil {
			return nil
		}
		return tr.current.client
	}
	hostFailRate := func() float64 {
		prov.mu.Lock()
		defer prov.mu.Unlock()
		return prov.metric(hk).rpcFailRate.Value()
	}
	waitFor := func(desc string, fn func() bool) {
		t.Helper()
		for range 500 {
			if fn() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("timed out waiting for", desc)
	}

	sector := frand.Bytes(proto.SectorSize)
	if _, err := c.WriteSector(t.Context(), accountKey, hk, sector); err != nil {
		t.Fatal(err)
	}
	healthy := hostFailRate()

	// hold the transport on behalf of an RPC that was already in flight when
	// the stalled RPC reaches its deadline
	tr.mu.Lock()
	existing := tr.acquireLocked()
	tr.mu.Unlock()
	existingReleased := false
	defer func() {
		if !existingReleased {
			tr.release(existing)
		}
	}()

	// stop relaying bytes after the handshake and cached settings exchange.
	sc.stall()
	ctx, cancel := context.WithTimeout(t.Context(), stallDeadline)
	defer cancel()
	deadline, _ := ctx.Deadline()

	done := make(chan error, 1)
	go func() {
		_, err := c.WriteSector(ctx, accountKey, hk, sector)
		done <- err
	}()
	waitFor("the mux write loop to park", func() bool { return sc.parkedWriteCalls() > 0 })

	select {
	case err = <-done:
	case <-time.After(time.Until(deadline) + stallTolerance):
		_ = tc.Close()
		t.Fatal("stalled write never returned at its deadline")
	}
	if err == nil {
		t.Fatal("expected the stalled write to fail")
	} else if overshoot := time.Since(deadline); overshoot > stallOvershoot {
		t.Fatal("stalled write returned late", overshoot)
	}

	// the transport is evicted, but host scoring remains the caller's job.
	if cachedTransport() != nil {
		t.Fatal("stalled transport was not dropped")
	} else if rate := hostFailRate(); rate != healthy {
		t.Fatal("client demoted the host", rate)
	}

	// existing streams keep the transport alive until they finish
	if tracked.closes.Load() != 0 {
		t.Fatal("detached transport was closed while still in use")
	}
	sc.release()
	if _, err := rhp.RPCSettings(t.Context(), existing.client); err != nil {
		t.Fatal("dropped transport was closed while still in use", err)
	}
	tr.release(existing)
	existingReleased = true
	if tracked.closes.Load() != 1 {
		t.Fatal("detached transport was not closed after its last RPC finished")
	}

	// the next RPC reaches the same host over a fresh connection.
	if _, err := c.WriteSector(t.Context(), accountKey, hk, sector); err != nil {
		t.Fatal(err)
	} else if fresh := cachedTransport(); fresh == nil || fresh == tracked {
		t.Fatal("next RPC did not cache a fresh transport")
	}
}
