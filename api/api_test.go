package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap"
)

func TestWalletAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// create indexer
	network, genesis := testutil.V2Network()
	indexer := testutils.NewIndexer(t, network, genesis, zap.NewNop())

	// mine until v2 and fund the indexer
	indexer.MineBlocksBlocking(ctx, t, indexer.WalletAddr(), network.HardforkV2.AllowHeight)

	// assert events are being persisted
	events, err := indexer.WalletEvents(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("no events")
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, %+v", events[0])
	}

	// assert wallet is empty
	res, err := indexer.Wallet(ctx)
	if err != nil {
		t.Fatal(err)
	} else if !res.Confirmed.Add(res.Unconfirmed).IsZero() {
		t.Fatal("expected wallet to be empty")
	}

	// mine until funds mature
	indexer.MineBlocksBlocking(ctx, t, types.Address{}, network.MaturityDelay)

	// assert wallet is funded
	res, err = indexer.Wallet(ctx)
	if err != nil {
		t.Fatal(err)
	} else if res.Confirmed.IsZero() {
		t.Fatal("expected wallet to be funded")
	}

	// assert sending siacoins to void address fails
	_, err = indexer.WalletSendSiacoins(ctx, types.VoidAddress, types.Siacoins(1), false, false)
	if err == nil || !strings.Contains(err.Error(), "cannot send to void address") {
		t.Fatal("unexpected error", err)
	}

	// create a wallet
	w := testutils.NewWallet(t, network, genesis)
	err = w.Connect(ctx, indexer.SyncerAddr())
	if err != nil {
		t.Fatal(err)
	}

	// assert host wallet is empty
	bal, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !bal.Confirmed.IsZero() || !bal.Unconfirmed.IsZero() {
		t.Fatal("expected empty balance", bal)
	}

	// assert we can send siacoins to that host
	txnID, err := indexer.WalletSendSiacoins(ctx, w.Address(), types.Siacoins(1), false, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert the transaction is pending
	pending, err := indexer.WalletPending(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 1 {
		t.Fatal("expected pending transaction")
	} else if pending[0].Type != wallet.EventTypeV2Transaction {
		t.Fatal("unexpected transaction type", pending[0].Type)
	} else if pending[0].ID != types.Hash256(txnID) {
		t.Fatal("expected transaction id to match")
	}

	// mine a block
	indexer.MineBlocks(t, types.Address{}, 1)
	indexer.BlockUntilSynced(ctx, t, w)

	// assert siacons arrived successfully
	bal, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !bal.Confirmed.Equals(types.Siacoins(1)) {
		t.Fatal("expected balance to be 1 SC", bal)
	}

	// assert the transaction is no longer pending
	pending, err = indexer.WalletPending(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending transaction")
	}
}
