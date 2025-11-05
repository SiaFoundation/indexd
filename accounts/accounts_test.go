package accounts_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
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
	app := cluster.App(t)

	// assert we have one usable host
	time.Sleep(time.Second)
	hosts, err := indexer.Hosts().Hosts(context.Background(), 0, 10, hosts.WithUsable(true))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	}

	// convenience variables
	hk := hosts[0].PublicKey
	hp := hosts[0].Settings.Prices
	hc := indexer.HostClient(t, hk)

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
	acc, err := app.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// assert the account is funded
	balance, err := hc.AccountBalance(context.Background(), acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if balance.IsZero() {
		t.Fatal("expected account to be funded")
	}

	// spend some money
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	_, err = hc.WriteSector(context.Background(), hp, app.AccountToken(hk), bytes.NewReader(sector[:]), proto.SectorSize)
	if err != nil {
		t.Fatal(err)
	}

	// assert it was spent
	updated, err := hc.AccountBalance(context.Background(), acc.AccountKey)
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
	updated, err = hc.AccountBalance(context.Background(), acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !updated.Equals(balance) {
		t.Fatalf("expected account balance to be funded to %v, instead it was %v", balance, updated)
	}
}
