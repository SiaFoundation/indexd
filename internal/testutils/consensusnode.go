package testutils

import (
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
)

// ConsensusNode is a helper type that serves as the source of truth for
// anything consensus-related during testing. It can be used to create test
// types such as the Indexer or Wallet and will guarantee that a call to
// `MineBlock` won't return before all created components are done subscribing
// to the newly mined blocks.
type ConsensusNode struct {
	cm      *chain.Manager
	genesis types.Block
	network *consensus.Network

	syncFns []func()
}

// NewConsensusNode creates a new node ready for testing. It will mine enough
// blocks to reach the V2 require height before returning.
func NewConsensusNode(t testing.TB, log *zap.Logger) *ConsensusNode {
	network, genesis := testutil.V2Network()
	dbstore, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis, chain.NewZapMigrationLogger(log.Named("chaindb")))
	if err != nil {
		t.Fatalf("failed to create chain store: %v", err)
	}
	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))
	c := &ConsensusNode{
		cm:      cm,
		genesis: genesis,
		network: network,
	}
	c.MineBlocks(t, types.VoidAddress, network.HardforkV2.RequireHeight)
	return c
}

// MineBlocks is a helper to mine blocks and broadcast the headers
func (c *ConsensusNode) MineBlocks(t testing.TB, addr types.Address, n uint64) {
	t.Helper()

	for i := uint64(0); i < n; i++ {
		b, ok := coreutils.MineBlock(c.cm, addr, 5*time.Second)
		if !ok {
			t.Fatal("failed to mine block")
		} else if err := c.cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
	}
	for _, fn := range c.syncFns {
		fn()
	}
}

// Network returns the used network configuration of the ConsensusNode
func (c *ConsensusNode) Network() *consensus.Network {
	return c.network
}

// AddSyncFn adds a function to call whenever blocks are mined
func (c *ConsensusNode) addSyncFn(fn func()) {
	c.syncFns = append(c.syncFns, fn)
	fn()
}
