package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap"
)

func TestExplorerAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())

	rate, err := indexer.ExplorerSiacoinExchangeRate(context.Background(), "usd")
	if err != nil {
		t.Fatal(err)
	} else if rate == 0 {
		t.Fatal("expected non-zero rate")
	}
}

func TestHostsAPI(t *testing.T) {
	// create cluster
	c := testutils.NewConsensusNode(t, zap.NewNop())
	h1 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	h2 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())

	// fund hosts
	c.MineBlocks(t, h1.WalletAddress(), 1)
	c.MineBlocks(t, h2.WalletAddress(), 1)
	c.MineBlocks(t, types.Address{}, c.Network().MaturityDelay)

	// announce hosts
	err := errors.Join(h1.Announce(), h2.Announce())
	if err != nil {
		t.Fatal(err)
	}

	// mine a block and allow hosts to be scanned
	c.MineBlocks(t, types.Address{}, 1)
	time.Sleep(time.Second)

	// assert both hosts got scanned
	if hosts, err := indexer.Hosts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hosts) != 2 {
		t.Fatal("expected 2 hosts", len(hosts))
	} else if h1, err := indexer.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1.LastSuccessfulScan.IsZero() {
		t.Fatal("expected h1 to be scanned successfully")
	} else if !h1.LastFailedScan.IsZero() {
		t.Fatal("expected h1 to not have failed scans")
	} else if h2, err := indexer.Host(context.Background(), h2.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h2.LastSuccessfulScan.IsZero() {
		t.Fatal("expected h2 to be scanned successfully")
	} else if !h2.LastFailedScan.IsZero() {
		t.Fatal("expected h2 to not have failed scans")
	}

	// assert blocklist is empty and unblocking unknown host is noop
	if blocklist, err := indexer.HostsBlocklist(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(blocklist) != 0 {
		t.Fatal("expected 0 blocklisted hosts", len(blocklist))
	} else if indexer.HostsBlocklistRemove(context.Background(), types.GeneratePrivateKey().PublicKey()) != nil {
		t.Fatal("expected error")
	}

	// block both hosts
	if err := indexer.HostsBlocklistAdd(context.Background(), []types.PublicKey{h1.PublicKey(), h2.PublicKey()}); err != nil {
		t.Fatal(err)
	} else if blocklist, err := indexer.HostsBlocklist(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(blocklist) != 2 {
		t.Fatal("expected 2 blocklisted hosts", len(blocklist))
	} else if h1, err := indexer.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h1.Blocked {
		t.Fatal("expected host to be blocked", h1.Blocked)
	} else if h2, err := indexer.Host(context.Background(), h2.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h2.Blocked {
		t.Fatal("expected host to be blocked", h2.Blocked)
	}

	// unblock h1
	if err := indexer.HostsBlocklistRemove(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1, err := indexer.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1.Blocked {
		t.Fatal("expected host to be unblocked", h1.Blocked)
	}
}

func TestWalletAPI(t *testing.T) {
	// create indexer
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())
	c.MineBlocks(t, indexer.WalletAddr(), 1)

	// assert events are being persisted
	events, err := indexer.WalletEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("no events")
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, %+v", events[0])
	}

	// assert wallet is empty
	res, err := indexer.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !res.Confirmed.Add(res.Unconfirmed).IsZero() {
		t.Fatal("expected wallet to be empty")
	}

	// mine until funds mature
	c.MineBlocks(t, types.Address{}, c.Network().MaturityDelay)

	// assert wallet is funded
	res, err = indexer.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if res.Confirmed.IsZero() {
		t.Fatal("expected wallet to be funded")
	} else if res.Address != indexer.WalletAddr() {
		t.Fatal("invalid address")
	}

	// assert sending siacoins to void address fails
	_, err = indexer.WalletSendSiacoins(context.Background(), types.VoidAddress, types.Siacoins(1), false, false)
	if err == nil || !strings.Contains(err.Error(), "cannot send to void address") {
		t.Fatal("unexpected error", err)
	}

	// create a wallet
	w := testutils.NewWallet(t, c)

	// assert host wallet is empty
	bal, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !bal.Confirmed.IsZero() || !bal.Unconfirmed.IsZero() {
		t.Fatal("expected empty balance", bal)
	}

	// assert we can send siacoins to that host
	txnID, err := indexer.WalletSendSiacoins(context.Background(), w.Address(), types.Siacoins(1), false, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert the transaction is pending
	pending, err := indexer.WalletPending(context.Background())
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
	c.MineBlocks(t, types.Address{}, 1)

	// assert siacons arrived successfully
	bal, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !bal.Confirmed.Equals(types.Siacoins(1)) {
		t.Fatal("expected balance to be 1 SC", bal)
	}

	// assert the transaction is no longer pending
	pending, err = indexer.WalletPending(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(pending) != 0 {
		t.Fatal("expected no pending transaction")
	}
}
