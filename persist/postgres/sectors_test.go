package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestPinSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}
	account2 := proto.Account{2}

	// pin without an account
	slabIDs, err := store.PinSlabs(context.Background(), account, []SlabPinParams{{}})
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected ErrNotFound, got", err)
	}

	// add accounts
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	} else if err := store.AddAccount(context.Background(), types.PublicKey(account2)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add hosts
	addHost := func(i byte) types.PublicKey {
		t.Helper()
		hk := types.PublicKey{i}

		ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
		}); err != nil {
			t.Fatal(err)
		}
		return hk
	}
	hk1 := addHost(1)
	hk2 := addHost(2)

	// helper to create slabs
	newSlab := func(i byte) (SlabID, SlabPinParams) {
		slab := SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     10,
			Sectors: []SectorPinParams{
				{
					Root:    frand.Entropy256(),
					HostKey: hk1,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: hk2,
				},
			},
		}
		hasher := types.NewHasher()
		for _, sector := range slab.Sectors {
			hasher.E.Write(sector.Root[:])
		}
		return SlabID(hasher.Sum()), slab
	}

	// pins slabs
	slab1ID, slab1 := newSlab(1)
	slab2ID, slab2 := newSlab(2)
	slabs := []SlabPinParams{slab1, slab2}
	expectedIDs := []SlabID{slab1ID, slab2ID}
	slabIDs, err = store.PinSlabs(context.Background(), proto.Account{1}, slabs)
	if err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != len(slabs) {
		t.Fatalf("expected %d slab IDs, got %d", len(slabs), len(slabIDs))
	} else if slabIDs[0] != expectedIDs[0] || slabIDs[1] != expectedIDs[1] {
		t.Fatalf("expected slab IDs %v, got %v", expectedIDs, slabIDs)
	}

	assertSlab := func(slabID SlabID, params SlabPinParams, slab Slab) {
		t.Helper()
		if slab.ID != slabID {
			t.Fatalf("expected slab ID %v, got %v", slabID, slab.ID)
		} else if slab.EncryptionKey != params.EncryptionKey {
			t.Fatalf("expected encryption key %x, got %x", params.EncryptionKey, slab.EncryptionKey)
		} else if slab.MinShards != params.MinShards {
			t.Fatalf("expected min shards %d, got %d", params.MinShards, slab.MinShards)
		}
		if len(slab.Sectors) != len(params.Sectors) {
			t.Fatalf("expected %d sectors, got %d", len(params.Sectors), len(slab.Sectors))
		}
		for i, sector := range slab.Sectors {
			if sector.Root != params.Sectors[i].Root {
				t.Fatalf("expected sector root %x, got %x", params.Sectors[i].Root, sector.Root)
			} else if *sector.HostKey != params.Sectors[i].HostKey {
				t.Fatalf("expected host key %x, got %x", params.Sectors[i].HostKey, sector.HostKey)
			} else if sector.ContractID != nil {
				t.Fatalf("expected nil contract ID, got %v", sector.ContractID)
			}
		}
	}

	// fetch inserted slabs
	fetched, err := store.Slabs(context.Background(), slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(fetched) != len(slabs) {
		t.Fatalf("expected %d slabs, got %d", len(slabs), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// pin same slabs again which should return an error
	slabIDs, err = store.PinSlabs(context.Background(), proto.Account{1}, slabs)
	if !errors.Is(err, ErrSlabExists) {
		t.Fatal("expected ErrSlabExists, got", err)
	}

	// pinning them under a different account id should work
	_, err = store.PinSlabs(context.Background(), proto.Account{2}, slabs)
	if err != nil {
		t.Fatal(err)
	}
}

// BenchmarkPinSlabs benchmarks PinSlabs in various batch sizes
// Hardware | BatchSize |  ms/op  | Throughput    |
// M2 Pro   |  40MiB    |   4.1ms | 10213.82 MB/s |
// M2 Pro   | 400MiB    |  45.2ms |   927.61 MB/s |
// M2 Pro   |   4GiB    | 460.8ms |    91.01 MB/s |
func BenchmarkPinSlabs(b *testing.B) {
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
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
	newSlab := func() SlabPinParams {
		var sectors []SectorPinParams
		for i := range hks {
			sectors = append(sectors, SectorPinParams{
				Root:    frand.Entropy256(),
				HostKey: hks[i],
			})
		}
		slab := SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     10,
			Sectors:       sectors,
		}
		return slab
	}

	const dbBaseSize = 1 << 40    // 1TiB
	const slabSize = 40 * 1 << 20 // 40MiB

	// prepare base db
	var initialSlabs []SlabPinParams
	for range dbBaseSize / slabSize {
		initialSlabs = append(initialSlabs, newSlab())
	}
	_, err := store.PinSlabs(context.Background(), account, []SlabPinParams{newSlab()})
	if err != nil {
		b.Fatal(err)
	}

	runBenchmark := func(b *testing.B, nSlabs int) {
		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {

			b.StopTimer()
			var slabs []SlabPinParams
			for range nSlabs {
				slabs = append(slabs, newSlab())
			}
			b.StartTimer()

			_, err := store.PinSlabs(context.Background(), proto.Account{1}, slabs)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	// insert 40MiB of data at a time
	b.Run("PinSlabs-40MiB", func(b *testing.B) {
		runBenchmark(b, 1)
	})

	// insert 400MiB of data at a time
	b.Run("PinSlabs-400MiB", func(b *testing.B) {
		runBenchmark(b, 10)
	})

	// insert 4GiB of data at a time
	b.Run("PinSlabs-4GiB", func(b *testing.B) {
		runBenchmark(b, 100)
	})
}
