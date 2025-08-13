package slabs_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
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
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
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
	shards, roots := newTestShards(t, 2, 4)
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
		EncryptionKey: frand.Entropy256(),
		MinShards:     2,
		Sectors: []slabs.SectorPinParams{
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
	err = indexer.HostsBlocklistAdd(context.Background(), []types.PublicKey{hosts[0].PublicKey}, "test blocklist reason")
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

func newTestShards(t *testing.T, dataShards, parityShards int) ([][]byte, []types.Hash256) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		t.Fatal(err)
	}

	shards := make([][]byte, dataShards+parityShards)
	for i := range shards {
		shards[i] = make([]byte, proto.SectorSize)
	}

	buf := make([]byte, proto.SectorSize*dataShards)
	frand.Read(buf)

	stripedSplit(buf, shards[:dataShards])
	err = enc.Encode(shards)
	if err != nil {
		t.Fatalf("failed to encode shards: %v", err)
	}

	var roots []types.Hash256
	for _, shard := range shards {
		roots = append(roots, proto.SectorRoot((*[proto.SectorSize]byte)(shard)))
	}

	return shards, roots
}

func stripedSplit(data []byte, dataShards [][]byte) {
	buf := bytes.NewBuffer(data)
	for off := 0; buf.Len() > 0; off += proto.LeafSize {
		for _, shard := range dataShards {
			copy(shard[off:], buf.Next(proto.LeafSize))
		}
	}
}
