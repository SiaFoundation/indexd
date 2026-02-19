package accounts_test

import (
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAccountFunding(t *testing.T) {
	// create cluster
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(1))
	indexer := cluster.Indexer
	store := indexer.Store()

	// create two quotas with different fund targets
	var (
		quota1Name       = "small"
		quota1FundTarget = uint64(1 << 30) // 1 GiB
		quota2Name       = "large"
		quota2FundTarget = uint64(2 << 30) // 2 GiB
		connectKey1      = "key1"
		connectKey2      = "key2"
	)

	if err := store.PutQuota(quota1Name, accounts.PutQuotaRequest{
		Description:     "Small quota",
		MaxPinnedData:   uint64(1e12),
		TotalUses:       10000,
		FundTargetBytes: &quota1FundTarget,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutQuota(quota2Name, accounts.PutQuotaRequest{
		Description:     "Large quota",
		MaxPinnedData:   uint64(1e12),
		TotalUses:       10000,
		FundTargetBytes: &quota2FundTarget,
	}); err != nil {
		t.Fatal(err)
	}

	// create connect keys for each quota
	if _, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:   connectKey1,
		Quota: quota1Name,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:   connectKey2,
		Quota: quota2Name,
	}); err != nil {
		t.Fatal(err)
	}

	// register two accounts under different quotas
	sk1 := types.GeneratePrivateKey()
	if err := store.RegisterAppKey(connectKey1, sk1.PublicKey(), accounts.AppMeta{}); err != nil {
		t.Fatal(err)
	}

	sk2 := types.GeneratePrivateKey()
	if err := store.RegisterAppKey(connectKey2, sk2.PublicKey(), accounts.AppMeta{}); err != nil {
		t.Fatal(err)
	}

	// assert we have one usable host
	time.Sleep(time.Second)
	available, err := indexer.Hosts().Hosts(context.Background(), 0, 10, hosts.WithUsable(true))
	if err != nil {
		t.Fatal(err)
	} else if len(available) != 1 {
		t.Fatalf("expected 1 host, got %d", len(available))
	}
	host := available[0]
	hk := host.PublicKey

	c := client.New(client.NewProvider(hosts.NewHostStore(store)))
	defer c.Close()

	// assert we have at least one active contract
	activeContracts, err := indexer.Contracts().Contracts(context.Background(), 0, 10, contracts.WithRevisable(true), contracts.WithGood(true))
	if err != nil {
		t.Fatal(err)
	} else if len(activeContracts) < 1 {
		t.Fatalf("expected at least 1 contract, got %d", len(activeContracts))
	}

	// mine a few blocks to ensure the contract is confirmed
	cluster.ConsensusNode.MineBlocks(t, types.VoidAddress, 5)
	time.Sleep(time.Second)

	// fetch accounts
	acc1, err := indexer.App.Account(t.Context(), sk1)
	if err != nil {
		t.Fatal(err)
	}
	acc2, err := indexer.App.Account(t.Context(), sk2)
	if err != nil {
		t.Fatal(err)
	}

	// assert both accounts are funded
	balance1, err := c.AccountBalance(t.Context(), hk, acc1.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if balance1.IsZero() {
		t.Fatal("expected account 1 to be funded")
	}
	balance2, err := c.AccountBalance(t.Context(), hk, acc2.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if balance2.IsZero() {
		t.Fatal("expected account 2 to be funded")
	}

	// assert the balances match their respective fund targets
	expectedTarget1 := accounts.HostFundTarget(host, quota1FundTarget)
	expectedTarget2 := accounts.HostFundTarget(host, quota2FundTarget)
	if !balance1.Equals(expectedTarget1) {
		t.Fatalf("expected account 1 balance %v, got %v", expectedTarget1, balance1)
	}
	if !balance2.Equals(expectedTarget2) {
		t.Fatalf("expected account 2 balance %v, got %v", expectedTarget2, balance2)
	}

	// spend some money on account 1
	var sector [proto.SectorSize]byte
	frand.Read(sector[:])
	_, err = c.WriteSector(t.Context(), sk1, hk, sector[:])
	if err != nil {
		t.Fatal(err)
	}

	// trigger funding
	err = indexer.Contracts().TriggerAccountFunding(true)
	if err != nil {
		t.Fatal(err)
	}

	// assert account 1 was refilled to its original balance
	time.Sleep(time.Second)
	updated1, err := c.AccountBalance(t.Context(), hk, acc1.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !updated1.Equals(balance1) {
		t.Fatalf("expected account 1 to be refilled to %v, got %v", balance1, updated1)
	}

	// assert account 2 is still at its original balance
	updated2, err := c.AccountBalance(t.Context(), hk, acc2.AccountKey)
	if err != nil {
		t.Fatal(err)
	} else if !updated2.Equals(balance2) {
		t.Fatalf("expected account 2 balance to remain %v, got %v", balance2, updated2)
	}
}
