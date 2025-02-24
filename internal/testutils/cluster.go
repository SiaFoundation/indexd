package testutils

import (
	"context"
	"math"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
)

type (
	clusterCfg struct {
		network *consensus.Network
		genesis types.Block
		logger  *zap.Logger
		hosts   int
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
	cn := NewConsensusNode(t, cfg.logger)
	indexer := cn.NewIndexer(t, cfg.logger.Named("indexer"))
	w, err := indexer.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cn.MineBlocks(w.Address, 1)

	// create cluster
	cluster := &Cluster{
		ConsensusNode: cn,

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
		if err := h.Announce(chain.V2HostAnnouncement{{
			Protocol: rhp4.ProtocolTCPSiaMux,
			Address:  h.l.Addr().String(),
		}}); err != nil {
			t.Fatal(err)
		}
		announced[h.PublicKey()] = struct{}{}
	}

	c.ConsensusNode.MineBlocks(types.VoidAddress, 1) // mine attestations
	knownHosts, err := c.Indexer.db.Hosts(ctx, 0, math.MaxInt)
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

// FundHosts funds the hosts with one block, then waits for the funds to mature.
func (c *Cluster) FundHosts(ctx context.Context, t testing.TB, hosts ...*Host) {
	t.Helper()

	for _, h := range hosts {
		c.ConsensusNode.MineBlocks(h.w.Address(), 1)
	}
	c.ConsensusNode.MineBlocks(types.Address{}, c.ConsensusNode.network.MaturityDelay)

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
	for i := 0; i < n; i++ {
		pk := types.GeneratePrivateKey()
		hosts = append(hosts, cn.NewHost(t, pk, c.log.Named("host-"+pk.PublicKey().String())))
	}
	return hosts
}
