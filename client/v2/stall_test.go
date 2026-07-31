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

// stallDeadline is the context deadline given to the stalled RPC.
const stallDeadline = 3 * time.Second

// stallConn gates the bytes crossing a net.Conn. While stalled, Read and Write
// park instead of touching the underlying connection, which is what a peer that
// goes silent looks like to the mux: the write loop parks in conn.Write and a
// large Stream.Write ends up waiting for buffer space that never frees up.
type stallConn struct {
	net.Conn

	cond     *sync.Cond
	stalled  bool
	released bool
	parked   int
}

func newStallConn(conn net.Conn) *stallConn {
	return &stallConn{Conn: conn, cond: sync.NewCond(new(sync.Mutex))}
}

func (c *stallConn) gate() {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	for c.stalled && !c.released {
		c.parked++
		c.cond.Broadcast()
		c.cond.Wait()
		c.parked--
	}
}

func (c *stallConn) Read(p []byte) (int, error) {
	c.gate()
	return c.Conn.Read(p)
}

func (c *stallConn) Write(p []byte) (int, error) {
	c.gate()
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

func (c *stallConn) parkedCalls() int {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	return c.parked
}

type stallStore struct {
	addr string
}

func (s stallStore) UsableHosts() ([]hosts.HostInfo, error) { return nil, nil }
func (s stallStore) Usable(types.PublicKey) (bool, error)   { return true, nil }
func (s stallStore) Addresses(types.PublicKey) ([]chain.NetAddress, error) {
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
		TotalStorage:       1 << 40, // 1 TiB
		RemainingStorage:   1 << 40, // 1 TiB
	})
	server := rhp.NewServer(hostKey, stubChain{}, stubContractor{}, stubWallet{}, sr, testutil.NewEphemeralSectorStore())
	return testutil.ServeSiaMux(t, server, log.Named("siamux"))
}

func TestStalledRPCResetsTransport(t *testing.T) {
	log := zaptest.NewLogger(t)

	hostKey := types.GeneratePrivateKey()
	hk := hostKey.PublicKey()
	accountKey := types.GeneratePrivateKey()
	hostAddr := serveHost(t, hostKey, log)

	prov := NewProvider(stallStore{addr: hostAddr})
	c := New(prov, log)
	t.Cleanup(func() { c.Close() })

	// dial over a connection that can be silenced and install it as the
	// client's cached transport
	conn, err := net.Dial("tcp", hostAddr)
	if err != nil {
		t.Fatal(err)
	}
	sc := newStallConn(conn)
	t.Cleanup(sc.release)

	upCtx, upCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer upCancel()
	tc, err := siamux.Upgrade(upCtx, sc, hk)
	if err != nil {
		t.Fatal(err)
	}

	tr := &transport{connectTimeout: defaultConnectTimeout, tc: tc}
	c.mu.Lock()
	c.transports[hk] = tr
	c.mu.Unlock()

	cachedTransport := func() rhp.TransportClient {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		return tr.tc
	}

	failRate := func() float64 {
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

	// the transport works and the prices get cached, so the stalled write below
	// reaches the sector data
	if _, err := c.WriteSector(t.Context(), accountKey, hk, sector); err != nil {
		t.Fatal(err)
	}
	healthy := failRate()

	// silence the peer mid write
	sc.stall()

	ctx, cancel := context.WithTimeout(t.Context(), stallDeadline)
	defer cancel()
	deadline, _ := ctx.Deadline()

	done := make(chan error, 1)
	go func() {
		_, err := c.WriteSector(ctx, accountKey, hk, sector)
		done <- err
	}()
	waitFor("conn.Write to park", func() bool { return sc.parkedCalls() > 0 })

	// closing the stream on deadline unwinds the write, so the RPC returns on
	// time even though nothing is leaving the connection
	select {
	case err = <-done:
	case <-time.After(time.Until(deadline) + 15*time.Second):
		tc.Close() // unwind the write so cleanup cannot hang
		t.Fatal("stalled write never returned at its deadline")
	}
	if err == nil {
		t.Fatal("expected the stalled write to fail")
	} else if overshoot := time.Since(deadline); overshoot > time.Second {
		t.Fatal("stalled write returned late", overshoot)
	}

	// the connection it stalled on must not be handed out again and the host
	// must carry the failure
	if cachedTransport() != nil {
		t.Fatal("stalled transport was not dropped")
	} else if rate := failRate(); rate <= healthy {
		t.Fatal("host was not demoted", rate)
	}

	// the next RPC dials a fresh connection and succeeds
	if _, err := c.WriteSector(t.Context(), accountKey, hk, sector); err != nil {
		t.Fatal(err)
	}
}
