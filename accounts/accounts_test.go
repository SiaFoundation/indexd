package accounts_test

import (
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAccountFunding(t *testing.T) {
	// create cluster
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer

	// create an app
	app, sk := cluster.App(t)

	// assert we have one usable host
	time.Sleep(time.Second)
	available, err := indexer.Hosts().Hosts(context.Background(), 0, 10, hosts.WithUsable(true))
	if err != nil {
		t.Fatal(err)
	} else if len(available) != 1 {
		t.Fatalf("expected 1 host, got %d", len(available))
	}

	// convenience variables
	hk := available[0].PublicKey

	client := client.New(client.NewProvider(hosts.NewHostStore(indexer.Store())))
	defer client.Close()

	// assert we have at least one active contract
	contracts, err := indexer.Contracts().Contracts(context.Background(), 0, 10, contracts.WithRevisable(true), contracts.WithGood(true))
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) < 1 {
		t.Fatalf("expected at least 1 contract, got %d", len(contracts))
	}

	// mine a few blocks to ensure the contract is confirmed
	cluster.ConsensusNode.MineBlocks(t, types.VoidAddress, 5)
	time.Sleep(time.Second)

	// fetch account
	acc, err := app.Account(t.Context(), sk)
	if err != nil {
		t.Fatal(err)
	}

	hp, err := client.Prices(t.Context(), hk)
	if err != nil {
		t.Fatal(err)
	}

	// assert the account is funded
	balance, err := client.AccountBalance(t.Context(), hk, acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if balance.IsZero() {
		t.Fatal("expected account to be funded")
	}

	// spend some money
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	_, err = client.WriteSector(t.Context(), sk, hk, sector[:])
	if err != nil {
		t.Fatal(err)
	}

	// assert it was spent
	updated, err := client.AccountBalance(t.Context(), hk, acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !updated.Add(hp.RPCWriteSectorCost(proto.SectorSize).RenterCost()).Equals(balance) {
		t.Fatalf("expected account balance to be slightly less than %v", balance)
	}

	// trigger funding
	err = indexer.Contracts().TriggerAccountFunding(true)
	if err != nil {
		t.Fatal(err)
	}

	// assert it was refilled
	time.Sleep(time.Second)
	updated, err = client.AccountBalance(t.Context(), hk, acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !updated.Equals(balance) {
		t.Fatalf("expected account balance to be funded to %v, instead it was %v", balance, updated)
	}
}
