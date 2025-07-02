package api_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/pins"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

func TestAccountsAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())

	var accs []types.PublicKey
	for range 10 {
		accs = append(accs, types.GeneratePrivateKey().PublicKey())
		err := indexer.AccountsAdd(context.Background(), accs[len(accs)-1])
		if err != nil {
			t.Fatal(err)
		}
	}

	err := indexer.AccountsAdd(context.Background(), accs[len(accs)-1])
	if err == nil || !strings.Contains(err.Error(), accounts.ErrExists.Error()) {
		t.Fatal("expected ErrExists", err)
	}

	accounts, err := indexer.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(accs, accounts) {
		t.Fatal("unexpected accounts", accounts)
	}

	accounts, err = indexer.Accounts(context.Background(), api.WithOffset(7), api.WithLimit(2))
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(accs[7:9], accounts) {
		t.Fatal("unexpected accounts", accounts)
	}

	accounts, err = indexer.Accounts(context.Background(), api.WithOffset(10), api.WithLimit(2))
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 0 {
		t.Fatal("unexpected accounts", accounts)
	}

	for _, acc := range accs {
		err = indexer.AccountsDelete(context.Background(), acc)
		if err != nil {
			t.Fatal(err)
		}
	}
	accounts, err = indexer.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 0 {
		t.Fatal("unexpected accounts", len(accounts))
	}

	pk1 := types.GeneratePrivateKey().PublicKey()
	err = indexer.AccountsAdd(context.Background(), pk1)
	if err != nil {
		t.Fatal(err)
	}

	accounts, err = indexer.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("unexpected accounts", len(accounts))
	} else if accounts[0] != pk1 {
		t.Fatal("unexpected key", accounts[0])
	}

	pk2 := types.GeneratePrivateKey().PublicKey()
	err = indexer.AccountsUpdate(context.Background(), pk1, pk2)
	if err != nil {
		t.Fatal(err)
	}

	accounts, err = indexer.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(accounts) != 1 {
		t.Fatal("unexpected accounts", len(accounts))
	} else if accounts[0] != pk2 {
		t.Fatal("unexpected key", accounts[0])
	}
}

func TestContractsAPI(t *testing.T) {
	// create cluster with one host
	logger := newTestLogger(false)
	c := testutils.NewConsensusNode(t, logger)
	h := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())

	// create indexer
	indexer := testutils.NewIndexer(t, c, logger)

	// fund host and indexer wallet
	c.MineBlocks(t, h.WalletAddress(), 1)
	c.MineBlocks(t, indexer.WalletAddr(), 1)
	c.MineBlocks(t, types.Address{}, c.Network().MaturityDelay)

	// announce host
	if err := h.Announce(); err != nil {
		t.Fatal(err)
	}
	c.MineBlocks(t, types.Address{}, 1)
	time.Sleep(time.Second)

	// assert it got scanned
	if h, err := indexer.Host(context.Background(), h.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h.Usability.Usable() {
		v := reflect.ValueOf(h.Usability)
		var failedFields []string
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).Kind() == reflect.Bool && !v.Field(i).Bool() {
				failedFields = append(failedFields, v.Type().Field(i).Name)
			}
		}
		t.Fatalf("expected host to be usable, but got false for: %v", failedFields)
	}

	// assert a contract was formed
	time.Sleep(time.Second)
	var contract contracts.Contract
	if contracts, err := indexer.Contracts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else {
		contract = contracts[0]
	}

	// assert we can fetch the contract by ID
	if c, err := indexer.Contract(context.Background(), contract.ID); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(c, contract) {
		t.Fatal("unexpected contract", c)
	}

	// assert fetching a non-existing contract returns an error
	if _, err := indexer.Contract(context.Background(), types.FileContractID{}); err == nil || !strings.Contains(err.Error(), contracts.ErrNotFound.Error()) {
		t.Fatal("expected ErrNotFound", err)
	}

	// assert WithGood filters out bad contracts
	if contracts, err := indexer.Contracts(context.Background(), api.WithGood(true)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else if contracts, err := indexer.Contracts(context.Background(), api.WithGood(false)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatal("expected no contract", len(contracts))
	}

	// assert WithRevisable filters out non-revisable contracts
	if contracts, err := indexer.Contracts(context.Background(), api.WithRevisable(true)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else if contracts, err := indexer.Contracts(context.Background(), api.WithRevisable(false)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatal("expected no contract", len(contracts))
	}

	// block host and assert it's not returned
	if err := indexer.HostsBlocklistAdd(context.Background(), []types.PublicKey{h.PublicKey()}, t.Name()); err != nil {
		t.Fatal(err)
	} else if contracts, err := indexer.Contracts(context.Background(), api.WithGood(true)); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatal("expected no contract", len(contracts))
	} else if err := indexer.HostsBlocklistRemove(context.Background(), h.PublicKey()); err != nil {
		t.Fatal(err)
	}

	// figure out the renew height
	cs, err := indexer.SettingsContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	renewHeight := contract.ProofHeight - cs.RenewWindow + 1

	// mine until contracts get renewed
	ci, err := indexer.Tip()
	if err != nil {
		t.Fatal(err)
	} else if ci.Height > renewHeight {
		t.Fatal("unexpected")
	}
	c.MineBlocks(t, types.Address{}, renewHeight-ci.Height)
	time.Sleep(time.Second)

	// assert contract was renewed - we don't pass the option here to asserts
	// the contracts API returns only revisable contracts by default
	if contracts, err := indexer.Contracts(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	} else if contracts[0].RenewedFrom != contract.ID {
		t.Fatal("expected contract to be renewed", contracts[0].RenewedFrom, contract.ID)
	}
}

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
	} else if !h1.Usability.Usable() {
		t.Fatal("expected h1 to be usable", h1.Usability)
	} else if h2, err := indexer.Host(context.Background(), h2.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h2.LastSuccessfulScan.IsZero() {
		t.Fatal("expected h2 to be scanned successfully")
	} else if !h2.LastFailedScan.IsZero() {
		t.Fatal("expected h2 to not have failed scans")
	} else if !h2.Usability.Usable() {
		t.Fatal("expected h2 to be usable", h2.Usability)
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
	if err := indexer.HostsBlocklistAdd(context.Background(), []types.PublicKey{h1.PublicKey(), h2.PublicKey()}, t.Name()); err != nil {
		t.Fatal(err)
	} else if blocklist, err := indexer.HostsBlocklist(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(blocklist) != 2 {
		t.Fatal("expected 2 blocklisted hosts", len(blocklist))
	} else if h1, err := indexer.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h1.Blocked {
		t.Fatal("expected host to be blocked", h1.Blocked)
	} else if h1.BlockedReason != t.Name() {
		t.Fatalf("expected host to be blocked with reason %s, got %s", t.Name(), h1.BlockedReason)
	} else if h2, err := indexer.Host(context.Background(), h2.PublicKey()); err != nil {
		t.Fatal(err)
	} else if !h2.Blocked {
		t.Fatal("expected host to be blocked", h2.Blocked)
	} else if h2.BlockedReason != t.Name() {
		t.Fatalf("expected host to be blocked with reason %s, got %s", t.Name(), h2.BlockedReason)
	}

	// unblock h1
	if err := indexer.HostsBlocklistRemove(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1, err := indexer.Host(context.Background(), h1.PublicKey()); err != nil {
		t.Fatal(err)
	} else if h1.Blocked {
		t.Fatal("expected host to be unblocked", h1.Blocked)
	}

	// filter by blocked hosts
	unblocked, err := indexer.Hosts(context.Background(), api.WithBlocked(false))
	if err != nil {
		t.Fatal(err)
	} else if len(unblocked) != 1 || unblocked[0].PublicKey != h1.PublicKey() {
		t.Fatalf("invalid hosts were returned (%d): %+v", len(unblocked), unblocked)
	}
	blocked, err := indexer.Hosts(context.Background(), api.WithBlocked(true))
	if err != nil {
		t.Fatal(err)
	} else if len(blocked) != 1 || blocked[0].PublicKey != h2.PublicKey() {
		t.Fatalf("invalid hosts were returned (%d): %+v", len(blocked), blocked)
	}

	// filter by usable hosts - all of them should be usable
	usable, err := indexer.Hosts(context.Background(), api.WithUsable(true))
	if err != nil {
		t.Fatal(err)
	} else if len(usable) != 2 {
		t.Fatalf("invalid number of hosts: %d", len(usable))
	}
	unusable, err := indexer.Hosts(context.Background(), api.WithUsable(false))
	if err != nil {
		t.Fatal(err)
	} else if len(unusable) != 0 {
		t.Fatalf("invalid number of hosts: %d", len(unusable))
	}

	// filter for hosts with contracts - none should have contracts
	contracted, err := indexer.Hosts(context.Background(), api.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(contracted) != 0 {
		t.Fatalf("invalid number of hosts: %d", len(contracted))
	}
	notContracted, err := indexer.Hosts(context.Background(), api.WithActiveContracts(false))
	if err != nil {
		t.Fatal(err)
	} else if len(notContracted) != 2 {
		t.Fatalf("invalid number of hosts: %d", len(notContracted))
	}
}

func TestSettingsAPI(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())

	// assert contract settings can be fetched and updated
	cs, err := indexer.SettingsContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs.Enabled = frand.Uint64n(2) == 0
	cs.Period = frand.Uint64n(100) + 2
	cs.RenewWindow = cs.Period / 2
	cs.WantedContracts = frand.Uint64n(1e3)

	err = indexer.SettingsContractsUpdate(context.Background(), cs)
	if err != nil {
		t.Fatal(err)
	}

	csUpdate, err := indexer.SettingsContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(cs, csUpdate) {
		t.Fatal("unexpected", csUpdate)
	}

	// assert host settings can be fetched and updated
	hs, err := indexer.SettingsHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	hs.MaxEgressPrice = types.NewCurrency64(frand.Uint64n(1e3))
	hs.MaxIngressPrice = types.NewCurrency64(frand.Uint64n(1e3))
	hs.MaxStoragePrice = types.NewCurrency64(frand.Uint64n(1e3))
	hs.MinCollateral = types.NewCurrency64(frand.Uint64n(1e3))
	frand.Read(hs.MinProtocolVersion[:])

	err = indexer.SettingsHostsUpdate(context.Background(), hs)
	if err != nil {
		t.Fatal(err)
	}

	hsUpdate, err := indexer.SettingsHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hs, hsUpdate) {
		t.Fatal("unexpected", hsUpdate)
	}

	// assert price pinning settings can be fetched and updated
	ps, err := indexer.SettingsPricePinning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ps.Currency = "eur"
	ps.MaxEgressPrice = pins.Pin(frand.Float64())
	ps.MaxIngressPrice = pins.Pin(frand.Float64())
	ps.MaxStoragePrice = pins.Pin(frand.Float64())
	ps.MinCollateral = pins.Pin(frand.Float64())

	err = indexer.SettingsPricePinningUpdate(context.Background(), ps)
	if err != nil {
		t.Fatal(err)
	}

	psUpdate, err := indexer.SettingsPricePinning(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ps, psUpdate) {
		t.Fatal("unexpected", psUpdate)
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
	w := testutils.NewWallet(t, c, types.GeneratePrivateKey())

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

// newTestLogger creates a console logger used for testing.
func newTestLogger(enable bool) *zap.Logger {
	if !enable {
		return zap.NewNop()
	}
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zap.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zap.DebugLevel),
	)
}
