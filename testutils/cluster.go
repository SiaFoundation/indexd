package testutils

import (
	"context"
	"math"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/indexd/contracts"
	"go.uber.org/zap"
)

type (
	clusterCfg struct {
		network     *consensus.Network
		genesis     types.Block
		indexerOpts []IndexerOpt
		logger      *zap.Logger
		hosts       int
	}

	// ClusterOpt is a functional option for configuring a cluster for testing
	ClusterOpt func(*clusterCfg)
)

// Cluster is a test cluster that contains an indexer, hosts and other helper
// types as needed for integration testing.
type Cluster struct {
	ConsensusNode *ConsensusNode
	Hosts         []*Host
	Indexer       *Indexer

	log *zap.Logger
}

var (
	defaultClusterCfg = func(fn func() (*consensus.Network, types.Block)) clusterCfg {
		n, g := fn()
		return clusterCfg{
			network: n,
			genesis: g,
			logger:  zap.NewNop(),
			hosts:   5,
		}
	}
)

// WithLogger allows for attaching a custom logger to the cluster for debugging
// if necessary
func WithLogger(logger *zap.Logger) ClusterOpt {
	return func(cfg *clusterCfg) {
		cfg.logger = logger
	}
}

// WithHosts allows for overriding the default number of hosts in the cluster
func WithHosts(n int) ClusterOpt {
	return func(cfg *clusterCfg) {
		cfg.hosts = n
	}
}

// WithIndexer allows for passing options to the indexer when creating the
// cluster. This is useful for configuring slab options, logger, etc.
func WithIndexer(opts ...IndexerOpt) ClusterOpt {
	return func(cfg *clusterCfg) {
		cfg.indexerOpts = opts
	}
}

// NewCluster creates a cluster for testing. A cluster contains an indexer and
// multiple hosts.
func NewCluster(t testing.TB, opts ...ClusterOpt) *Cluster {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cfg := defaultClusterCfg(testutil.V2Network)
	for _, opt := range opts {
		opt(&cfg)
	}

	// create indexer and mine until after V2 allowheight
	c := NewConsensusNode(t, cfg.logger)
	indexer := NewIndexer(t, c, cfg.logger.Named("indexer"), cfg.indexerOpts...)
	c.MineBlocks(t, indexer.WalletAddr(), 50)

	// create cluster
	cluster := &Cluster{
		ConsensusNode: c,

		Indexer: indexer,
		log:     cfg.logger,
	}

	// add hosts
	hosts := cluster.NewHosts(t, cfg.hosts)
	cluster.AddHosts(ctx, t, hosts...)
	cluster.FundHosts(ctx, t, hosts...)
	cluster.AnnounceHosts(ctx, t, hosts...)

	// TODO: implement as needed
	// - add volumes to hosts
	// - wait for contracts

	return cluster
}

// AddAccount adds a new account to the indexer and returns the private key.
func (c *Cluster) AddAccount(t testing.TB) types.PrivateKey {
	sk := types.GeneratePrivateKey()
	c.Indexer.Store().AddTestAccount(t, sk.PublicKey())
	return sk
}

// AddHosts adds the given hosts to the cluster.
func (c *Cluster) AddHosts(ctx context.Context, t testing.TB, hosts ...*Host) {
	t.Helper()

	for _, h := range hosts {
		err := h.Connect(ctx, c.Indexer.syncer.Addr())
		if err != nil {
			t.Fatal(err)
		}
		c.Hosts = append(c.Hosts, h)
	}
}

// AnnounceHosts announces the hosts and blocks until they are indexed.
func (c *Cluster) AnnounceHosts(ctx context.Context, t testing.TB, hosts ...*Host) {
	t.Helper()

	start := time.Now().Round(time.Second)
	announced := make(map[types.PublicKey]struct{})
	for _, h := range hosts {
		if err := h.Announce(); err != nil {
			t.Fatal(err)
		}
		announced[h.PublicKey()] = struct{}{}
	}

	c.ConsensusNode.MineBlocks(t, types.VoidAddress, 1) // mine attestations
	knownHosts, err := c.Indexer.store.Hosts(0, math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for _, h := range knownHosts {
		if _, ok := announced[h.PublicKey]; !ok || h.LastAnnouncement.Before(start) {
			continue
		}
		n++
	}
	if n != len(announced) {
		t.Fatalf("expected %d hosts to be announced, got %d", len(announced), n)
	}
}

// FundHosts funds the hosts with multiple blocks, then waits for the funds to mature.
func (c *Cluster) FundHosts(ctx context.Context, t testing.TB, hosts ...*Host) {
	t.Helper()

	for _, h := range hosts {
		c.ConsensusNode.MineBlocks(t, h.w.Address(), 5)
	}
	c.ConsensusNode.MineBlocks(t, types.Address{}, c.ConsensusNode.network.MaturityDelay)

	for _, h := range hosts {
		if res, err := h.w.Balance(); err != nil {
			t.Fatal(err)
		} else if res.Confirmed.IsZero() {
			t.Fatal("host not funded")
		}
	}
}

// NewHosts creates n new hosts using the cluster's network and genesis block.
func (c *Cluster) NewHosts(t testing.TB, n int) []*Host {
	t.Helper()
	cn := c.ConsensusNode

	var hosts []*Host
	for range n {
		pk := types.GeneratePrivateKey()
		hosts = append(hosts, cn.NewHost(t, pk, c.log.Named("host-"+pk.PublicKey().String())))
	}
	return hosts
}

// WaitForAccountFunding waits until the given account is funded on every host
// in the cluster.
func (c *Cluster) WaitForAccountFunding(t *testing.T, pk proto.Account) {
	t.Helper()
	for _, h := range c.Hosts {
		var funded bool
		for i := 0; i < 120; i++ {
			b, err := c.Indexer.Client().AccountBalance(t.Context(), h.PublicKey(), pk)
			if err != nil {
				t.Fatal("failed to get account balance:", err)
			} else if !b.IsZero() {
				funded = true
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if !funded {
			t.Fatalf("account not funded on host %v", h.PublicKey())
		}
	}
}

// WaitForContracts waits until all contracts are formed
func (c *Cluster) WaitForContracts(t *testing.T) {
	t.Helper()

	start := time.Now()

	// fetch maintenance settings
	cm := c.Indexer.Contracts()
	ms, err := cm.MaintenanceSettings(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// calculate how many are required
	numHosts := len(c.Hosts)
	numWanted := ms.WantedContracts
	numRequired := min(numHosts, int(numWanted))

	// wait until contracts get formed
	for {
		contracts, err := cm.Contracts(t.Context(), 0, math.MaxInt, contracts.WithGood(true), contracts.WithRevisable(true))
		if err != nil {
			t.Fatal(err)
		}

		unique := make(map[types.PublicKey]struct{})
		for _, c := range contracts {
			unique[c.HostKey] = struct{}{}
		}

		if len(unique) >= numRequired {
			// mine a block to confirm all pending contracts
			c.ConsensusNode.MineBlocks(t, types.VoidAddress, 1)
			time.Sleep(time.Second) // wait for indexing
			return
		}

		time.Sleep(200 * time.Millisecond)
		if time.Since(start) > time.Minute {
			t.Fatalf("timed out waiting for contracts to form: formed %d/%d", len(unique), numRequired)
		}
	}
}
