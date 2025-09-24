package slabs_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
)

func TestMigrations(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(7), testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithHealthCheckInterval(time.Second))))

	// create some more utxos
	indexer := cluster.Indexer
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 10)

	// add an account
	a1 := types.GeneratePrivateKey()
	err := indexer.Store().AddAccount(context.Background(), a1.PublicKey(), accounts.AccountMeta{})
	if err != nil {
		t.Fatal(err)
	}

	// convenience variables
	app := indexer.App(a1)

	// fetch hosts
	hosts := testutils.WaitForHosts(t, app, 7)
	if err != nil {
		t.Fatal(err)
	}

	// upload sectors to hosts
	encryptionKey, shards, roots := slabs.NewTestShards(t, 2, 4)
	for i := range shards {
		client := indexer.HostClient(t, hosts[i].PublicKey)
		hs, err := client.Settings(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.WriteSector(context.Background(), hs.Prices, proto.NewAccountToken(a1, hosts[i].PublicKey), bytes.NewReader(shards[i]), proto.SectorSize); err != nil {
			t.Fatal(err)
		}
	}

	// pin the slab
	slabID, err := app.PinSlab(context.Background(), slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     2,
		Sectors: []slabs.PinnedSector{
			{Root: roots[0], HostKey: hosts[0].PublicKey},
			{Root: roots[1], HostKey: hosts[1].PublicKey},
			{Root: roots[2], HostKey: hosts[2].PublicKey},
			{Root: roots[3], HostKey: hosts[3].PublicKey},
			{Root: roots[4], HostKey: hosts[4].PublicKey},
			{Root: roots[5], HostKey: hosts[5].PublicKey},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert sectors were pinned
	time.Sleep(time.Second)
	pinned, err := app.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	} else if len(pinned.Sectors) != 6 {
		t.Fatalf("expected 6 pinned sectors, got %d", len(pinned.Sectors))
	} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != hosts[0].PublicKey {
		t.Fatalf("expected sector %s on host %s, got %s on host %s", roots[0], hosts[0].PublicKey, pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
	}

	// add h1 to blocklist
	err = indexer.Hosts().BlockHosts(context.Background(), []types.PublicKey{hosts[0].PublicKey}, "test blocklist reason")
	if err != nil {
		t.Fatal(err)
	}

	// assert sectors are still pinned and the first shard was migrated to the free host
	time.Sleep(3 * time.Second)
	pinned, err = app.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	} else if len(pinned.Sectors) != 6 {
		t.Fatalf("expected 6 pinned sectors, got %d", len(pinned.Sectors))
	} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != hosts[6].PublicKey {
		t.Fatalf("expected sector %s on host %s, got %s on host %s", roots[0], hosts[6].PublicKey, pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
	}
}

func TestUpdateLastUsed(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(3), testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithHealthCheckInterval(time.Second))))

	// create some more utxos
	indexer := cluster.Indexer
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 10)

	// add an account
	a1 := types.GeneratePrivateKey()
	err := indexer.Store().AddAccount(context.Background(), a1.PublicKey(), accounts.AccountMeta{})
	if err != nil {
		t.Fatal(err)
	}

	// convenience variables
	app := indexer.App(a1)

	// fetch hosts
	hosts := testutils.WaitForHosts(t, app, 3)
	if err != nil {
		t.Fatal(err)
	}

	// upload sectors to hosts
	encryptionKey, shards, roots := slabs.NewTestShards(t, 1, 2)
	for i := range shards {
		client := indexer.HostClient(t, hosts[i].PublicKey)
		hs, err := client.Settings(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.WriteSector(context.Background(), hs.Prices, proto.NewAccountToken(a1, hosts[i].PublicKey), bytes.NewReader(shards[i]), proto.SectorSize); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()

	// last used time should be around account creation and thus should predate
	// `now` timestamp
	account, err := app.Account(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.Before(now) {
		t.Fatal("LastUsed time later than expected")
	}

	// pin the slab
	// pin the slab
	slabID, err := app.PinSlab(context.Background(), slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: roots[0], HostKey: hosts[0].PublicKey},
			{Root: roots[1], HostKey: hosts[1].PublicKey},
			{Root: roots[2], HostKey: hosts[2].PublicKey},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// last used time should be time of PinSlab invocation and thus should be
	// after `now` timestamp
	account, err = app.Account(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.After(now) {
		t.Fatal("LastUsed time earlier than expected")
	}
	now = account.LastUsed

	// assert sectors were pinned
	time.Sleep(time.Second)
	if _, err := app.Slab(context.Background(), slabID); err != nil {
		t.Fatal(err)
	}

	// last used time should be time of Slab invocation (which causes the server
	// to call PinnedSlab) and thus should be after the timestamp from when
	// PinSlab was invoked
	account, err = app.Account(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.After(now) {
		t.Fatal("LastUsed time earlier than expected")
	}
	now = account.LastUsed

	if err := app.UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal(err)
	}

	// last used time should not have changed as a result of UnpinSlab
	account, err = app.Account(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !account.LastUsed.Equal(now) {
		t.Fatal("LastUsed time different than expected")
	}
}
