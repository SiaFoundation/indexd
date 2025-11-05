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
	"lukechampine.com/frand"
)

func TestAccountFunding(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer

	// create an app
	app := cluster.App(t)

	// fetch account
	acc, err := app.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}

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
	target := types.Siacoins(2)

	// assert we have one active contract
	time.Sleep(time.Second)
	contracts, err := indexer.Contracts().Contracts(context.Background(), 0, 10, contracts.WithRevisable(true), contracts.WithGood(true))
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contracts))
	}

	// assert the account is funded
	balance, err := hc.AccountBalance(context.Background(), acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(target) {
		t.Fatalf("expected account to be funded to %v, got %v", target, balance)
	}

	// spend some money
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	_, err = hc.WriteSector(context.Background(), hp, app.AccountToken(hk), bytes.NewReader(sector[:]), proto.SectorSize)
	if err != nil {
		t.Fatal(err)
	}

	// assert it was spent
	balance, err = hc.AccountBalance(context.Background(), acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Add(hp.RPCWriteSectorCost(proto.SectorSize).RenterCost()).Equals(target) {
		t.Fatal("expected account balance to be slightly less than 1SC")
	}

	// trigger funding
	err = indexer.Contracts().TriggerAccountFunding(true)
	if err != nil {
		t.Fatal(err)
	}

	// assert it was refilled
	time.Sleep(time.Second)
	balance, err = hc.AccountBalance(context.Background(), acc.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(target) {
		t.Fatal("expected account balance to be funded to 1SC", balance)
	}
}
