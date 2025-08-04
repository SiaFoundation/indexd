package accounts_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/internal/testutils"
	"lukechampine.com/frand"
)

func TestAccountFunding(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer

	// add an account
	a1 := types.GeneratePrivateKey()
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert we have one usable host
	time.Sleep(time.Second)
	hosts, err := indexer.Hosts(context.Background(), admin.WithUsable(true))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	}

	// convenience variables
	acc := proto.Account(a1.PublicKey())
	hk := hosts[0].PublicKey
	hp := hosts[0].Settings.Prices
	hc := indexer.HostClient(t, hk)
	token := proto.NewAccountToken(a1, hk)
	target := types.Siacoins(1)

	// assert we have one active contract
	time.Sleep(time.Second)
	contracts, err := indexer.Contracts(context.Background(), admin.WithRevisable(true), admin.WithGood(true))
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contracts))
	}

	// assert the account is funded
	balance, err := hc.AccountBalance(context.Background(), acc)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(target) {
		t.Fatal("expected account balance to be funded to 1SC")
	}

	// spend some money
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	_, err = hc.WriteSector(context.Background(), hp, token, bytes.NewReader(sector[:]), proto.SectorSize)
	if err != nil {
		t.Fatal(err)
	}

	// assert it was spent
	balance, err = hc.AccountBalance(context.Background(), acc)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Add(hp.RPCWriteSectorCost(proto.SectorSize).RenterCost()).Equals(target) {
		t.Fatal("expected account balance to be slightly less than 1SC")
	}

	// trigger funding
	err = indexer.TriggerAction(context.Background(), "funding")
	if err != nil {
		t.Fatal(err)
	}

	// assert it was refilled
	time.Sleep(time.Second)
	balance, err = hc.AccountBalance(context.Background(), acc)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(target) {
		t.Fatal("expected account balance to be funded to 1SC", balance)
	}
}
