package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSectorsForIntegrityCheck(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
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

	// pin a slab to add a few sectors to the database
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	root4 := frand.Entropy256()
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, []slabs.SlabPinParams{
		{
			EncryptionKey: [32]byte{},
			MinShards:     10,
			Sectors: []slabs.SectorPinParams{
				{
					Root:    root1,
					HostKey: hk,
				},
				{
					Root:    root2,
					HostKey: hk,
				},
				{
					Root:    root3,
					HostKey: hk,
				},
				{
					Root:    root4,
					HostKey: hk,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// update next integrity check time for roots and assert the ordering works
	updateNextCheck := func(root types.Hash256, nextCheck time.Time) {
		t.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET next_integrity_check = $1 WHERE sector_root = $2`, nextCheck, sqlHash256(root))
		if err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().Round(time.Microsecond)
	updateNextCheck(root2, now.Add(-2*time.Hour))      // requires check
	updateNextCheck(root4, now.Add(-1*time.Hour))      // requires check
	updateNextCheck(root3, now.Add(-time.Millisecond)) // requires check
	updateNextCheck(root1, now.Add(1*time.Hour))       // doesn't require check

	// make sure limit is applied
	assertSectors := func(limit int) {
		t.Helper()
		expected := []types.Hash256{root2, root4, root3}
		sectors, err := store.SectorsForIntegrityCheck(context.Background(), hk, limit)
		if err != nil {
			t.Fatal(err)
		} else if len(sectors) != min(limit, len(expected)) {
			t.Fatalf("expected 3 sectors, got %d", len(sectors))
		} else if !reflect.DeepEqual(sectors, expected[:min(limit, len(expected))]) {
			t.Fatal("sectors don't match expected roots")
		}
	}
	assertSectors(10)
	assertSectors(3)
	assertSectors(2)
	assertSectors(1)

	// no sectors for unknown host
	sectors, err := store.SectorsForIntegrityCheck(context.Background(), types.PublicKey{2}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(sectors) != 0 {
		t.Fatalf("expected 0 sectors, got %d", len(sectors))
	}
}

func TestPinSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}
	account2 := proto.Account{2}

	// pin without an account
	nextCheck := time.Now().Round(time.Microsecond).Add(time.Hour)
	slabIDs, err := store.PinSlabs(context.Background(), account, nextCheck, []slabs.SlabPinParams{{}})
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
	newSlab := func(i byte) (slabs.SlabID, slabs.SlabPinParams) {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     10,
			Sectors: []slabs.SectorPinParams{
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
		return slabs.SlabID(hasher.Sum()), slab
	}

	// pins slabs
	slab1ID, slab1 := newSlab(1)
	slab2ID, slab2 := newSlab(2)
	params := []slabs.SlabPinParams{slab1, slab2}
	expectedIDs := []slabs.SlabID{slab1ID, slab2ID}
	slabIDs, err = store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, params)
	if err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != len(params) {
		t.Fatalf("expected %d slab IDs, got %d", len(params), len(slabIDs))
	} else if slabIDs[0] != expectedIDs[0] || slabIDs[1] != expectedIDs[1] {
		t.Fatalf("expected slab IDs %v, got %v", expectedIDs, slabIDs)
	}

	assertSlab := func(slabID slabs.SlabID, params slabs.SlabPinParams, slab slabs.Slab) {
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
	fetched, err := store.Slabs(context.Background(), account, slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(fetched) != len(params) {
		t.Fatalf("expected %d slabs, got %d", len(params), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// again but for wrong account
	_, err = store.Slabs(context.Background(), account2, slabIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// pin same slabs again which should return an error
	slabIDs, err = store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, params)
	if !errors.Is(err, slabs.ErrSlabExists) {
		t.Fatal("expected ErrSlabExists, got", err)
	}

	// pinning them under a different account id should work
	_, err = store.PinSlabs(context.Background(), proto.Account{2}, nextCheck, params)
	if err != nil {
		t.Fatal(err)
	}
}

// BenchmarkSlabs benchmarks Slabs and PinSlabs in various batch sizes. The
// results are expressed in time per operation as well as equivalent
// upload/download throughput.
//
// Hardware |     Benchmark   |  ms/op  | Throughput   |
// M2 Pro   | PinSlabs-40MiB  |  1.1ms | 36115.26 MB/s |
// M2 Pro   | PinSlabs-400MiB |  7.8ms |  5363.69 MB/s |
// M2 Pro   | PinSlabs-4GiB   | 79.8ms |   528.11 MB/s |
//
// M2 Pro   | Slabs-40MiB  |  0.6ms |    61408.13 MB/s |
// M2 Pro   | Slabs-400MiB |  3.2ms |    13007.09 MB/s |
// M2 Pro   | Slabs-4GiB   | 29.4ms |     1426.31 MB/s |
func BenchmarkSlabs(b *testing.B) {
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
	var initialSlabs []slabs.SlabPinParams
	for range dbBaseSize / slabSize {
		initialSlabs = append(initialSlabs, newSlab())
	}
	initialSlabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, initialSlabs)
	if err != nil {
		b.Fatal(err)
	}

	runPinBenchmark := func(b *testing.B, nSlabs int) {
		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			var slabs []slabs.SlabPinParams
			for range nSlabs {
				slabs = append(slabs, newSlab())
			}
			b.StartTimer()

			_, err := store.PinSlabs(context.Background(), proto.Account{1}, time.Time{}, slabs)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	runSlabsBenchmark := func(b *testing.B, nSlabs int) {
		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			frand.Shuffle(len(initialSlabIDs), func(i, j int) {
				initialSlabIDs[i], initialSlabIDs[j] = initialSlabIDs[j], initialSlabIDs[i]
			})
			slabIDs := initialSlabIDs[:nSlabs]
			b.StartTimer()

			_, err := store.Slabs(context.Background(), proto.Account{1}, slabIDs)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	// insert 40MiB of data at a time
	b.Run("PinSlabs-40MiB", func(b *testing.B) {
		runPinBenchmark(b, 1)
	})

	// insert 400MiB of data at a time
	b.Run("PinSlabs-400MiB", func(b *testing.B) {
		runPinBenchmark(b, 10)
	})

	// insert 4GiB of data at a time
	b.Run("PinSlabs-4GiB", func(b *testing.B) {
		runPinBenchmark(b, 100)
	})

	// fetch 40MiB of data at a time
	b.Run("Slabs-40MiB", func(b *testing.B) {
		runSlabsBenchmark(b, 1)
	})

	// fetch 400MiB of data at a time
	b.Run("Slabs-400MiB", func(b *testing.B) {
		runSlabsBenchmark(b, 10)
	})

	// fetch 4GiB of data at a time
	b.Run("Slabs-4GiB", func(b *testing.B) {
		runSlabsBenchmark(b, 100)
	})
}
