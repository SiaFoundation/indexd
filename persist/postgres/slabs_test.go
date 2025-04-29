package postgres

import (
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestPruneSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	assertCount := func(expected int, table string) {
		t.Helper()
		var got int
		if err := store.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&got); err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d rows in %s, got %d", expected, table, got)
		}
	}

	// add two accounts
	a1 := proto.Account{1}
	a2 := proto.Account{2}
	if err := store.AddAccount(context.Background(), types.PublicKey(a1)); err != nil {
		t.Fatal("failed to add account:", err)
	} else if err := store.AddAccount(context.Background(), types.PublicKey(a2)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add host
	hk := types.PublicKey{1}
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// pin 2 slabs that share a sector
	_, err := store.PinSlab(context.Background(), a1, time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    types.Hash256{1},
				HostKey: hk,
			},
			{
				Root:    types.Hash256{2},
				HostKey: hk,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID, err := store.PinSlab(context.Background(), a2, time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    types.Hash256{2},
				HostKey: hk,
			},
			{
				Root:    types.Hash256{3},
				HostKey: hk,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert table counts
	assertCount(2, "accounts")
	assertCount(2, "slabs")
	assertCount(3, "sectors")
	assertCount(4, "slab_sectors")

	// assert no slabs get pruned
	pruned, err := store.PruneSlabs(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if pruned != 0 {
		t.Fatal("expected no slabs to be pruned")
	}

	// unpin the second slab
	err = store.UnpinSlab(context.Background(), a2, slabID)
	if err != nil {
		t.Fatal(err)
	}

	// assert one slabs got pruned
	pruned, err = store.PruneSlabs(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if pruned != 1 {
		t.Fatal("expected one slabs to get pruned")
	}

	// assert table counts
	assertCount(2, "accounts")
	assertCount(1, "slabs")
	assertCount(2, "sectors")
	assertCount(2, "slab_sectors")
}

// BenchmarkPruneSlabs benchmarks PruneSlabs.
//
// Hardware   |   ms/op   |  Throughput  |
// M1 Max     |   1.09ms  |  1.34 TB/s    |
func BenchmarkPruneSlabs(b *testing.B) {
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// 30 hosts to simulate default redundancy
	var hks []types.PublicKey
	for i := byte(0); i < 30; i++ {
		hk := types.PublicKey{i}
		ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
		}); err != nil {
			b.Fatal(err)
		}
		hks = append(hks, hk)
	}

	// helper to create slabs
	newSlab := func() slabs.SlabPinParams {
		var sectors []slabs.SectorPinParams
		for i := range hks {
			sectors = append(sectors, slabs.SectorPinParams{
				Root:    frand.Entropy256(),
				HostKey: hks[i],
			})
		}
		slab := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     10,
			Sectors:       sectors,
		}
		return slab
	}

	const dbBaseSize = 1 << 40    // 1TiB
	const slabSize = 40 * 1 << 20 // 40MiB

	// prepare base db
	var initialSlabIDs []slabs.SlabID
	for range dbBaseSize / slabSize {
		slabID, err := store.PinSlab(context.Background(), account, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}
		initialSlabIDs = append(initialSlabIDs, slabID)
	}

	// define helper to unpin next batch of slabs
	unpinBatch := func() bool {
		b.Helper()
		if len(initialSlabIDs) < slabPruneBatchSize {
			return false
		}
		for i := range slabPruneBatchSize {
			if err := store.UnpinSlab(context.Background(), account, initialSlabIDs[i]); err != nil {
				b.Fatal(err)
			}
		}
		initialSlabIDs = initialSlabIDs[slabPruneBatchSize:]
		return true
	}

	b.SetBytes(slabPruneBatchSize * slabSize)
	for b.Loop() {
		b.StopTimer()
		if ok := unpinBatch(); !ok {
			break
		}
		b.StartTimer()

		pruned, err := store.PruneSlabs(context.Background())
		if err != nil {
			b.Fatal(err)
		} else if pruned != slabPruneBatchSize {
			b.Fatalf("expected %d slabs to get pruned but got %d", slabPruneBatchSize, pruned)
		}
	}
}
