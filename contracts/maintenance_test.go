package contracts_test

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
)

func TestWalletMaintenance(t *testing.T) {
	log := zaptest.NewLogger(t)
	ms := contracts.MaintenanceSettings{
		Enabled:         false,
		Period:          100,
		RenewWindow:     10,
		WantedContracts: 100,
	}
	store := testutils.NewDB(t, ms, log)

	sk := types.GeneratePrivateKey()

	network, genesis := testutil.V2Network()
	network.MaturityDelay = 0

	dbstore, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis, chain.NewZapMigrationLogger(log.Named("chaindb")))
	if err != nil {
		t.Fatalf("failed to create chain store: %v", err)
	}
	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))

	s := testutils.NewSyncer(t, genesis.ID(), cm)
	defer s.Close()

	w, err := wallet.NewSingleAddressWallet(sk, cm, store, s, wallet.WithLogger(log.Named("wallet")), wallet.WithDefragThreshold(250))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	hm, err := hosts.NewManager(s, nil, nil, store, alerts.NewManager(), hosts.WithLogger(log.Named("hosts")))
	if err != nil {
		t.Fatal(err)
	}
	defer hm.Close()

	contracts, err := contracts.NewManager(sk, nil, nil, cm, store, nil, hm, s, w, contracts.WithLogger(log.Named("contracts")), contracts.WithSyncPollInterval(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer contracts.Close()

	sub, err := subscriber.New(cm, hm, contracts, w, store, subscriber.WithLogger(log.Named("subscriber")))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// fund wallet with a single large UTXO
	testutil.MineBlocks(t, cm, w.Address(), 1)
	// ensure all components are synced
	if err := sub.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	contracts.TriggerMaintenance()
	time.Sleep(time.Second) // wait for maintenance to run

	events, err := w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected an unconfirmed event, got %d", len(events))
	} else if txn := events[0].Data.(wallet.EventV2Transaction); len(txn.SiacoinOutputs) != 100 {
		t.Fatalf("expected 100 outputs, got %d", len(txn.SiacoinOutputs))
	}

	// mine block to confirm transaction
	testutil.MineBlocks(t, cm, types.VoidAddress, 1)
	time.Sleep(time.Second) // wait for confirmation to be processed

	utxos, err := w.SpendableOutputs()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 100 {
		t.Fatalf("expected 100 UTXOs, got %d", len(utxos))
	}

	// trigger maintenance again and ensure no new split is created
	contracts.TriggerMaintenance()
	time.Sleep(time.Second)

	events, err = w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 0 {
		t.Fatalf("expected no unconfirmed events, got %d", len(events))
	}
}
