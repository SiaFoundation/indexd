package testutils

import (
	"context"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
)

type (
	// Wallet is an ephemeral wallet that can be used for testing.
	Wallet struct {
		*wallet.SingleAddressWallet
		s  *syncer.Syncer
		ws *testutil.EphemeralWalletStore
	}
)

// NewWallet creates a new ephemeral wallet.
func NewWallet(t testing.TB, n *consensus.Network, genesis types.Block) *Wallet {
	db, tipstate, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(db, tipstate)

	syncerListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syncerListener.Close() })

	s := syncer.New(syncerListener, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerListener.Addr().String(),
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
		syncer.WithSyncInterval(100*time.Millisecond),
	)
	t.Cleanup(func() { s.Close() })
	go s.Run()

	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), cm, ws)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })

	reorgCh := make(chan struct{}, 1)
	t.Cleanup(func() { close(reorgCh) })
	go func() {
		for range reorgCh {
			reverted, applied, err := cm.UpdatesSince(w.Tip(), 1000)
			if err != nil {
				panic(err)
			}

			if err := ws.UpdateChainState(func(tx wallet.UpdateTx) error {
				return w.UpdateChainState(tx, reverted, applied)
			}); err != nil {
				panic(err)
			}
		}
	}()
	stop := cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})
	t.Cleanup(stop)

	return &Wallet{w, s, ws}
}

// Connect connects the wallet's syncer with the given peer.
func (w *Wallet) Connect(ctx context.Context, addr string) error {
	_, err := w.s.Connect(ctx, addr)
	return err
}

// Tip returns the tip of the wallet.
func (w *Wallet) Tip() (types.ChainIndex, error) {
	return w.ws.Tip()
}
