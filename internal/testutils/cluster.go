package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
)

type (
	clusterOpts struct {
		logger *zap.Logger
		hosts  int
	}

	// ClusterOpt is a functional option for configuring a cluster for testing
	ClusterOpt func(*clusterOpts)
)

// Cluster is a test cluster that contains an indexer, hosts and other helper
// types as needed for integration testing.
type Cluster struct {
	Hosts   []*Host
	Indexer *Indexer
}

var (
	defaultClusterOpts = clusterOpts{
		logger: zap.NewNop(),
		hosts:  5,
	}
)

// WithLogger allows for attaching a custom logger to the cluster for debugging
// if necessary
func WithLogger(logger *zap.Logger) ClusterOpt {
	return func(cfg *clusterOpts) {
		cfg.logger = logger
	}
}

// WithHosts allows for overriding the default number of hosts in the cluster
func WithHosts(n int) ClusterOpt {
	return func(cfg *clusterOpts) {
		cfg.hosts = n
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
	indexer := NewIndexer(t, n, genesis, cfg.logger.Named("indexer"))

	// mine until after v2 height to reach v2 and to fund indexer
	indexer.MineBlocks(t, indexer.wallet.Address(), int(n.HardforkV2.AllowHeight))

	// create hosts and connect them to the indexer
	var hosts []*Host
	for i := 0; i < cfg.hosts; i++ {
		pk := types.GeneratePrivateKey()
		h := NewHost(t, pk, n, genesis, cfg.logger)
		_, err := h.s.Connect(context.Background(), indexer.syncer.Addr())
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, h)
	}

	// fund hosts with one block each
	for _, h := range hosts {
		indexer.MineBlocks(t, h.w.Address(), 1)
	}

	// mine more blocks to get outputs to mature
	indexer.MineBlocks(t, types.Address{}, int(n.MaturityDelay))

	// TODO: implement as needed
	// - add volumes to hosts
	// - announce hosts
	// - wait for contracts
	// - sync cluster

	Retry(t, 100, 100*time.Millisecond, func() error {
		tip := indexer.cm.Tip()
		for _, h := range hosts {
			if h.c.Tip() != tip {
				return fmt.Errorf("host's tip doesn't match indexer's: %v %v", tip, h.c.Tip())
			}
		}

		if state, err := indexer.State(context.Background()); err != nil {
			return err
		} else if state.ScanHeight < tip.Height {
			return fmt.Errorf("indexer's scan height doesn't match tip: %v %v", tip, state.ScanHeight)
		}

		return nil
	})

	return &Cluster{
		Hosts:   hosts,
		Indexer: indexer,
	}
}

// Retry retries a function until it returns nil or the number of tries is
// reached.
func Retry(t testing.TB, tries int, durationBetweenAttempts time.Duration, fn func() error) {
	t.Helper()
	var err error
	for i := 0; i < tries; i++ {
		err = fn()
		if err == nil {
			return
		}
		time.Sleep(durationBetweenAttempts)
	}
	t.Fatal(err)
}
