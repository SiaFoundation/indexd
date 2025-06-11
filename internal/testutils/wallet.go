package testutils

import (
	"math"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
)

type mockSyncer struct{}

// NewWallet creates a new SingleAddressWallet for testing which is connected to
// all the other components created via the ConsensusNode.
func NewWallet(t testing.TB, c *ConsensusNode, walletKey types.PrivateKey) *wallet.SingleAddressWallet {
	ws := testutil.NewEphemeralWalletStore()
	s := NewSyncer(t, c.genesis.ID(), c.cm)
	w, err := wallet.NewSingleAddressWallet(walletKey, c.cm, ws, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })

	// sync the wallet
	syncFn := func() {
		t.Helper()
		index, err := ws.Tip()
		if err != nil {
			t.Fatal(err)
		}
		reverted, applied, err := c.cm.UpdatesSince(index, math.MaxInt)
		if err != nil {
			t.Fatal(err)
		}
		if err := ws.UpdateChainState(func(tx wallet.UpdateTx) error {
			return w.UpdateChainState(tx, reverted, applied)
		}); err != nil {
			t.Fatal(err)
		}
	}
	c.addSyncFn(syncFn)
	return w
}
