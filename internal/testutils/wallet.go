package testutils

import (
	"math"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
)

func (c *ConsensusNode) NewWallet() *wallet.SingleAddressWallet {
	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), c.cm, ws)
	if err != nil {
		c.tb.Fatal(err)
	}
	c.tb.Cleanup(func() { w.Close() })

	// sync the wallet
	syncFn := func() {
		c.tb.Helper()
		index, err := ws.Tip()
		if err != nil {
			c.tb.Fatal(err)
		}
		reverted, applied, err := c.cm.UpdatesSince(index, math.MaxInt)
		if err != nil {
			c.tb.Fatal(err)
		}
		if err := ws.UpdateChainState(func(tx wallet.UpdateTx) error {
			return w.UpdateChainState(tx, reverted, applied)
		}); err != nil {
			c.tb.Fatal(err)
		}
	}
	syncFn()
	c.syncFns = append(c.syncFns, syncFn)
	return w
}
