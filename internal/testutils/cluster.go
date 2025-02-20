package testutils

import (
	"context"
	"errors"
	"fmt"
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
	Network *consensus.Network
	Genesis types.Block
	Hosts   []*Host
	Indexer *Indexer

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
	cfg := defaultClusterCfg(testutil.V2Network)
	for _, opt := range opts {
		opt(&cfg)
	}

	// create indexer and mine until after V2 allowheight
	indexer := NewIndexer(t, cfg.network, cfg.genesis, cfg.logger.Named("indexer"))
	indexer.MineBlocks(t, indexer.wallet.Address(), int(cfg.network.HardforkV2.AllowHeight))

	// create cluster
	cluster := &Cluster{
		Network: cfg.network,
		Genesis: cfg.genesis,

		Indexer: indexer,
		log:     cfg.logger,
	}

	// add hosts
	hosts := cluster.NewHosts(t, cfg.hosts)
	cluster.AddHosts(t, hosts...)
	cluster.FundHosts(t, hosts...)
	cluster.AnnounceHosts(t, hosts...)

	// TODO: implement as needed
	// - add volumes to hosts
	// - wait for contracts

	cluster.Sync(t)
	return cluster
}

// AddHosts adds the given hosts to the cluster.
func (c *Cluster) AddHosts(t testing.TB, hosts ...*Host) {
	t.Helper()

	for _, h := range hosts {
		err := h.Connect(c.Indexer.syncer.Addr())
		if err != nil {
			t.Fatal(err)
		}
		c.Hosts = append(c.Hosts, h)
	}
}

// AnnounceHosts announces the hosts and blocks until they are indexed.
func (c *Cluster) AnnounceHosts(t testing.TB, hosts ...*Host) {
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

	Retry(t, 100, 100*time.Millisecond, func() error {
		hosts, err := c.Indexer.db.Hosts(context.Background(), 0, math.MaxInt)
		if err != nil {
			t.Fatal(err)
		}

		var n int
		for _, h := range hosts {
			if _, ok := announced[h.PublicKey]; !ok || h.LastAnnouncement.Before(start) {
				continue
			}
			n++
		}

		if n != len(announced) {
			c.Indexer.MineBlocks(t, types.Address{}, 1)
			return fmt.Errorf("expected %d hosts to be announced, got %d", len(announced), n)
		}
		return nil
	})
}

// FundHosts funds the hosts with one block, then waits for the funds to mature.
func (c *Cluster) FundHosts(t testing.TB, hosts ...*Host) {
	t.Helper()

	for _, h := range hosts {
		c.Indexer.MineBlocks(t, h.w.Address(), 1)
	}
	c.Indexer.MineBlocks(t, types.Address{}, int(c.Network.MaturityDelay))

	Retry(t, 100, 100*time.Millisecond, func() error {
		for _, h := range hosts {
			if res, err := h.w.Balance(); err != nil {
				t.Fatal(err)
			} else if res.Confirmed.IsZero() {
				return errors.New("host not funded")
			}
		}
		return nil
	})
}

// NewHosts creates n new hosts using the cluster's network and genesis block.
func (c *Cluster) NewHosts(t testing.TB, n int) []*Host {
	t.Helper()

	var hosts []*Host
	for i := 0; i < n; i++ {
		pk := types.GeneratePrivateKey()
		hosts = append(hosts, NewHost(t, pk, c.Network, c.Genesis, c.log.Named("host-"+pk.PublicKey().String())))
	}
	return hosts
}

// Sync waits for the cluster to be in sync.
func (c *Cluster) Sync(t testing.TB) {
	t.Helper()

	Retry(t, 100, 100*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tip := c.Indexer.cm.Tip()
		for _, h := range c.Hosts {
			if h.c.Tip() != tip {
				return fmt.Errorf("host's tip doesn't match indexer's: %v %v", tip, h.c.Tip())
			}
		}

		if state, err := c.Indexer.State(ctx); err != nil {
			return err
		} else if state.ScanHeight < tip.Height {
			return fmt.Errorf("indexer's scan height doesn't match tip: %v < %v", state.ScanHeight, tip.Height)
		}

		return nil
	})
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
