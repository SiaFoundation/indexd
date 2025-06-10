package testutils

import (
	"math"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
)

// NewWallet creates a new SingleAddressWallet for testing which is connected to
// all the other components created via the ConsensusNode.
func NewWallet(t testing.TB, c *ConsensusNode) *wallet.SingleAddressWallet {
	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), c.cm, ws, testutil.MockSyncer{})
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
