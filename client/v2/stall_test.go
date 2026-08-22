package client

import (
	"context"
	"errors"
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

// stallConn gates bytes crossing a net.Conn. While stalled, Read and Write
// park instead of touching the underlying connection, mimicking a host that
// stops draining.
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

// singleAddrStore is a Store holding one siamux host.
type singleAddrStore struct {
	hostKey types.PublicKey
	addr    string
}

func (s singleAddrStore) UsableHosts() ([]hosts.HostInfo, error) {
	return []hosts.HostInfo{{
		PublicKey:     s.hostKey,
		Addresses:     s.addresses(),
		GoodForUpload: true,
	}}, nil
}

func (s singleAddrStore) Usable(hostKey types.PublicKey) (bool, error) {
	return hostKey == s.hostKey, nil
}

func (s singleAddrStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	if hostKey != s.hostKey {
		return nil, errors.New("unknown host")
	}
	return s.addresses(), nil
}

func (s singleAddrStore) addresses() []chain.NetAddress {
	return []chain.NetAddress{{Protocol: siamux.Protocol, Address: s.addr}}
}

// the host only serves unpaid sector writes, so everything the RPCs below never
// reach is left nil.
type (
	stubChain struct{ rhp.ChainManager }

	stubWallet struct{ rhp.Wallet }

	stubContractor struct{ rhp.Contractor }
)

func (stubChain) Tip() types.ChainIndex          { return types.ChainIndex{} }
func (stubChain) TipState() consensus.State      { return consensus.State{} }
func (stubChain) RecommendedFee() types.Currency { return types.ZeroCurrency }

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
	return testutil.ServeSiaMux(t, server, log.Named("siamux"))
}

func TestStalledRPCDropsTransport(t *testing.T) {
	const (
		rpcDeadline = 3 * time.Second
		tolerance   = 5 * time.Second // how late the stalled RPC may return
	)

	log := zaptest.NewLogger(t)
	hostKey := types.GeneratePrivateKey()
	hk := hostKey.PublicKey()
	accountKey := types.GeneratePrivateKey()
	hostAddr := serveHost(t, hostKey, log)

	prov := NewProvider(singleAddrStore{hostKey: hk, addr: hostAddr})
	c := New(prov, log)
	t.Cleanup(func() { _ = c.Close() })

	// install a working transport whose underlying connection can be silenced
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

	tr := newTransport(defaultConnectTimeout)
	install(tr, tracked)
	c.mu.Lock()
	c.transports[hk] = tr
	c.mu.Unlock()

	hostFailRate := func() float64 {
		prov.mu.Lock()
		defer prov.mu.Unlock()
		return prov.metric(hk).rpcFailRate.Value()
	}
	waitFor := func(desc string, fn func() bool) {
		t.Helper()
		for range 50 {
			if fn() {
				return
			}
			time.Sleep(100 * time.Millisecond)
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
	existing := acquire(tr)

	// stop relaying bytes after the handshake and cached settings exchange
	sc.stall()
	ctx, cancel := context.WithTimeout(t.Context(), rpcDeadline)
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
	case <-time.After(time.Until(deadline) + tolerance):
		_ = tc.Close()
		t.Fatal("stalled write never returned at its deadline")
	}
	if err == nil {
		t.Fatal("expected the stalled write to fail")
	}

	// the transport is dropped, but host scoring remains the caller's job
	if cached(tr) != nil {
		t.Fatal("stalled transport was not dropped")
	} else if rate := hostFailRate(); rate != healthy {
		t.Fatalf("expected fail rate %f, got %f", healthy, rate)
	}

	// existing streams keep the transport alive until they finish
	if closes := tracked.closes.Load(); closes != 0 {
		t.Fatalf("expected 0 closes while the transport was in use, got %d", closes)
	}
	sc.release()
	if _, err := rhp.RPCSettings(t.Context(), existing.tc); err != nil {
		t.Fatal("dropped transport was closed while still in use", err)
	}
	tr.release(existing)
	if closes := tracked.closes.Load(); closes != 1 {
		t.Fatalf("expected 1 close after the last RPC finished, got %d", closes)
	}

	// the next RPC reaches the same host over a fresh connection
	if _, err := c.WriteSector(t.Context(), accountKey, hk, sector); err != nil {
		t.Fatal(err)
	} else if fresh := cached(tr); fresh == nil || fresh == tracked {
		t.Fatal("next RPC did not cache a fresh transport")
	}
}
