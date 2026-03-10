package subscriber_test

import (
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

func TestSyncBatching(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := testutils.NewDB(t, contracts.MaintenanceSettings{
		Period:          100,
		RenewWindow:     10,
		WantedContracts: 1,
	}, log)

	network, genesis := testutil.V2Network()
	network.MaturityDelay = 0

	dbstore, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis, chain.NewZapMigrationLogger(log.Named("chaindb")))
	if err != nil {
		t.Fatalf("failed to create chain store: %v", err)
	}
	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))

	s := testutils.NewSyncer(t, genesis.ID(), cm)
	defer s.Close()

	sk := types.GeneratePrivateKey()
	w, err := wallet.NewSingleAddressWallet(sk, cm, store, s, wallet.WithLogger(log.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	hm, err := hosts.NewManager(s, nil, nil, store, alerts.NewManager(), hosts.WithLogger(log.Named("hosts")))
	if err != nil {
		t.Fatal(err)
	}
	defer hm.Close()

	cmm, err := contracts.NewManager(sk, nil, nil, cm, store, nil, nil, nil, contracts.NewContractLocker(), hm, s, w, contracts.WithLogger(log.Named("contracts")), contracts.WithSyncPollInterval(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cmm.Close()

	// use batch size of 1
	sub, err := subscriber.New(cm, hm, cmm, w, store, subscriber.WithLogger(log.Named("subscriber")), subscriber.WithBatchSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// mine more blocks than the batch size
	testutil.MineBlocks(t, cm, types.VoidAddress, 10)

	// trigger a sync
	if err := sub.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// assert subscriber syncs to the tip
	tip := cm.Tip()
	index, err := store.LastScannedIndex()
	if err != nil {
		t.Fatal(err)
	} else if index != tip {
		t.Fatalf("expected subscriber to sync to tip %v, got %v", tip, index)
	}
}
