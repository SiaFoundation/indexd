package testutils

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
)

type (
	clusterOpts struct {
		logger *zap.Logger
		nHosts int
	}

	// ClusterOpt is a functional option for configuring a cluster for testing
	ClusterOpt func(*clusterOpts)
)

// Cluster is a test cluster that contains an indexer, multiple hosts and an app
// that interacts with them.
type Cluster struct {
	app     *App
	indexer *Indexer
	hosts   []*Host
}

var (
	defaultClusterOpts = clusterOpts{
		logger: zap.NewNop(),
		nHosts: 5,
	}
)

// WithLogger allows for attaching a custom logger to the cluster for debugging
// if necessary
func WithLogger(logger *zap.Logger) ClusterOpt {
	return func(cfg *clusterOpts) {
		cfg.logger = logger
	}
}

// WithNHosts allows for overriding the default number of hosts in the cluster
func WithNHosts(n int) ClusterOpt {
	return func(cfg *clusterOpts) {
		cfg.nHosts = n
	}
}

// NewCluster creates a cluster for testing. A cluster contains an indexer and
// multiple hosts.
func NewCluster(t testing.TB, opts ...ClusterOpt) *Cluster {
	cfg := defaultClusterOpts
	for _, opt := range opts {
		opt(&cfg)
	}

	n, genesis := testutil.V2Network()
	indexer := NewIndexer(t, n, genesis, zap.NewNop())

	// mine until after v2 height to fund indexer
	indexer.MineBlocks(t, types.Address{}, int(n.HardforkV2.AllowHeight)) // TODO: add wallet to indexer and actually fund it

	// TODO: create hosts and connect them to the indexer

	// TODO: fund hosts

	// TODO: announce hosts

	// TODO: wait for contracts with the hosts

	// TODO: mine blocks and sync up

	// TODO: create app

	return &Cluster{
		indexer: indexer,
	}
}
