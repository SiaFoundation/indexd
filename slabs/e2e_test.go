package slabs_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
)

func TestMigrations(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(11), testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithHealthCheckInterval(500*time.Millisecond))))

	// create some more utxos
	indexer := cluster.Indexer
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 11)

	// add an account
	a1 := types.GeneratePrivateKey()
	indexer.AddAccount(t, a1.PublicKey())

	// convenience variables
	app := indexer.App(a1)

	// fetch hosts
	hosts := testutils.WaitForHosts(t, app, 11)

	// upload sectors to hosts
	encryptionKey, shards, roots := slabs.NewTestShards(t, 1, 10)
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
	slabIDs, err := app.PinSlabs(context.Background(), slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: roots[0], HostKey: hosts[0].PublicKey},
			{Root: roots[1], HostKey: hosts[1].PublicKey},
			{Root: roots[2], HostKey: hosts[2].PublicKey},
			{Root: roots[3], HostKey: hosts[3].PublicKey},
			{Root: roots[4], HostKey: hosts[4].PublicKey},
			{Root: roots[5], HostKey: hosts[5].PublicKey},
			{Root: roots[6], HostKey: hosts[6].PublicKey},
			{Root: roots[7], HostKey: hosts[7].PublicKey},
			{Root: roots[8], HostKey: hosts[8].PublicKey},
			{Root: roots[9], HostKey: hosts[9].PublicKey},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	// assert sectors were pinned
	time.Sleep(time.Second)
	pinned, err := app.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	} else if len(pinned.Sectors) != 10 {
		t.Fatalf("expected 10 pinned sectors, got %d", len(pinned.Sectors))
	} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != hosts[0].PublicKey {
		t.Fatalf("expected sector %s on host %s, got %s on host %s", roots[0], hosts[0].PublicKey, pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
	}

	// add first host to blocklist
	err = indexer.Hosts().BlockHosts(context.Background(), []types.PublicKey{hosts[0].PublicKey}, "test blocklist reason")
	if err != nil {
		t.Fatal(err)
	}

	// assert sectors are still pinned and the first shard was migrated to the free host
	time.Sleep(3 * time.Second)
	pinned, err = app.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	} else if len(pinned.Sectors) != 10 {
		t.Fatalf("expected 10 pinned sectors, got %d", len(pinned.Sectors))
	} else if pinned.Sectors[0].Root != roots[0] || pinned.Sectors[0].HostKey != hosts[10].PublicKey {
		t.Fatalf("expected sector %s on host %s, got %s on host %s", roots[0], hosts[10].PublicKey, pinned.Sectors[0].Root, pinned.Sectors[0].HostKey)
	}
}

func TestUpdateLastUsed(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(10), testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithHealthCheckInterval(time.Second))))

	// create some more utxos
	indexer := cluster.Indexer
	cluster.ConsensusNode.MineBlocks(t, indexer.WalletAddr(), 10)

	// add an account
	a1 := types.GeneratePrivateKey()
	indexer.AddAccount(t, a1.PublicKey())

	// convenience variables
	app := indexer.App(a1)

	// fetch hosts
	hosts := testutils.WaitForHosts(t, app, 10)

	// upload sectors to hosts
	encryptionKey, shards, roots := slabs.NewTestShards(t, 1, 9)
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
	slabIDs, err := app.PinSlabs(context.Background(), slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     1,
		Sectors: func() (s []slabs.PinnedSector) {
			for i, h := range hosts {
				s = append(s, slabs.PinnedSector{Root: roots[i], HostKey: h.PublicKey})
			}
			return s
		}(),
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

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
