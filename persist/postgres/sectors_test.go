package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestPinSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}
	account2 := proto.Account{2}

	// pin without an account
	slabIDs, err := store.PinSlabs(context.Background(), account, []slabs.SlabPinParams{{}})
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
		hasher.E.WriteUint64(uint64(slab.MinShards))
		hasher.E.Write(slab.EncryptionKey[:])
		for _, sector := range slab.Sectors {
			hasher.E.Write(sector.Root[:])
		}
		return slabs.SlabID(hasher.Sum()), slab
	}

	// pin slabs
	slab1ID, slab1 := newSlab(1)
	slab2ID, slab2 := newSlab(2)
	toPin := []slabs.SlabPinParams{slab1, slab2}
	expectedIDs := []slabs.SlabID{slab1ID, slab2ID}
	slabIDs, err = store.PinSlabs(context.Background(), proto.Account{1}, toPin)
	if err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != len(toPin) {
		t.Fatalf("expected %d slab IDs, got %d", len(toPin), len(slabIDs))
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
	} else if len(fetched) != len(toPin) {
		t.Fatalf("expected %d slabs, got %d", len(toPin), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// again but for wrong account
	_, err = store.Slabs(context.Background(), account2, slabIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// pin same slabs for account 2 again which should add links to the join
	// table
	slabIDs, err = store.PinSlabs(context.Background(), account2, toPin)
	if err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != len(toPin) {
		t.Fatalf("expected %d slab IDs, got %d", len(toPin), len(slabIDs))
	} else if slabIDs[0] != expectedIDs[0] || slabIDs[1] != expectedIDs[1] {
		t.Fatalf("expected slab IDs %v, got %v", expectedIDs, slabIDs)
	}

	// fetch slabs for account 2
	fetched, err = store.Slabs(context.Background(), account2, slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(fetched) != len(toPin) {
		t.Fatalf("expected %d slabs, got %d", len(toPin), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// assert database has the right number of sectors, slabs and accounts to
	// make sure deduplication works
	assertCount := func(table string, rows int64) {
		t.Helper()
		var count int64
		err := store.pool.QueryRow(context.Background(), fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count)
		if err != nil {
			t.Fatal(err)
		} else if count != rows {
			t.Fatalf("expected %d rows in %s, got %d", rows, table, count)
		}
	}
	assertCount("accounts", 2)      // 2 accounts
	assertCount("account_slabs", 4) // 2 slabs for each account
	assertCount("slabs", 2)         // 2 slabs
	assertCount("sectors", 4)       // 2 sectors per slab
}

func TestUnpinnedSectors(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create account, host and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}
	hk := types.PublicKey{1}
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.AddFormedContract(context.Background(), types.FileContractID(hk), hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		t.Fatal(err)
	}

	// create 4 sectors
	_, err := store.PinSlabs(context.Background(), account, []slabs.SlabPinParams{
		{
			EncryptionKey: frand.Entropy256(),
			MinShards:     10,
			Sectors: []slabs.SectorPinParams{
				{
					HostKey: hk,
					Root:    types.Hash256{1},
				},
				{
					HostKey: hk,
					Root:    types.Hash256{2},
				},
				{
					HostKey: hk,
					Root:    types.Hash256{3},
				},
				{
					HostKey: hk,
					Root:    types.Hash256{4},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// helper to update sector's pinned state
	updateSector := func(sid int64, hostID, contractID *int64, uploadedAt time.Time) {
		t.Helper()
		res, err := store.pool.Exec(context.Background(), `UPDATE sectors SET contract_id=$1, host_id=$2, uploaded_at=$3 WHERE id=$4`, contractID, hostID, uploadedAt, sid)
		if err != nil {
			t.Fatal(err)
		} else if res.RowsAffected() != 1 {
			t.Fatal("no rows updated")
		}
	}

	// prepare sectors
	one := int64(1)
	now := time.Now().Round(time.Microsecond)
	updateSector(1, &one, &one, now)                // has host and is pinned to contract
	updateSector(2, &one, nil, now)                 // has host but is not pinned to contract
	updateSector(3, nil, nil, now)                  // has neither host nor is it pinned
	updateSector(4, &one, nil, now.Add(-time.Hour)) // also not pinned but older than 2

	// check unpinned sectors
	unpinned, err := store.UnpinnedSectors(context.Background(), hk, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(unpinned) != 2 {
		t.Fatalf("expected 2 unpinned sector, got %d", len(unpinned))
	} else if unpinned[0] != (types.Hash256{4}) {
		t.Fatalf("expected root %v, got %v", types.Hash256{4}, unpinned[0])
	} else if unpinned[1] != (types.Hash256{2}) {
		t.Fatalf("expected root %v, got %v", types.Hash256{2}, unpinned[1])
	}

	// again with lower limit
	unpinned, err = store.UnpinnedSectors(context.Background(), hk, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unpinned) != 1 {
		t.Fatalf("expected 1 unpinned sector, got %d", len(unpinned))
	} else if unpinned[0] != (types.Hash256{4}) {
		t.Fatalf("expected root %v, got %v", types.Hash256{4}, unpinned[0])
	}
}

// BenchmarkSlabs benchmarks Slabs and PinSlabs in various batch sizes. The
// results are expressed in time per operation as well as equivalent
// upload/download throughput.
//
// Hardware |     Benchmark   |  ms/op  | Throughput   |
// M2 Pro   | PinSlabs-40MiB  |  1.4ms | 28472.56 MB/s |
// M2 Pro   | PinSlabs-400MiB |  8.9ms |  4698.53 MB/s |
// M2 Pro   | PinSlabs-4GiB   | 89.6ms |   467.67 MB/s |
//
// M2 Pro   | Slabs-40MiB  |  0.6ms |    63029.04 MB/s |
// M2 Pro   | Slabs-400MiB |  3.1ms |    13181.86 MB/s |
// M2 Pro   | Slabs-4GiB   | 29.8ms |     1404.40 MB/s |
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
	initialSlabIDs, err := store.PinSlabs(context.Background(), account, initialSlabs)
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

			_, err := store.PinSlabs(context.Background(), proto.Account{1}, slabs)
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

// BenchmarkUnpinnedSectors benchmarks UnpinnedSectors in various batch sizes.
//
// CPU    | BatchSize |	 Count  |    Time/op    |    Throughput
// M2 Pro |     100   |   1407  |  0.847440 ms  |   494938.30 MB/s
// M2 Pro |    1000   |    657  |  1.805719 ms  |  2322788.39 MB/s
// M2 Pro |   10000   |    123  |  8.913824 ms  |  4705392.65 MB/s
func BenchmarkUnpinnedSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}
	hk := types.PublicKey{1}
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		b.Fatal(err)
	}
	if err := store.AddFormedContract(context.Background(), types.FileContractID(hk), hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		b.Fatal(err)
	}

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.SectorPinParams
		for range batchSize {
			sectors = append(sectors, slabs.SectorPinParams{
				Root:    frand.Entropy256(),
				HostKey: hk,
			})
		}
		slabIDs, err := store.PinSlabs(context.Background(), account, []slabs.SlabPinParams{{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}})
		if err != nil {
			b.Fatal(err)
		} else if len(slabIDs) != 1 {
			b.Fatal("expected 1 slab id")
		}
	}

	// helper to pin sector
	pinSectors := func(roots []types.Hash256) {
		b.Helper()
		sqlRoots := make([]sqlHash256, len(roots))
		for i, root := range roots {
			sqlRoots[i] = sqlHash256(root)
		}
		res, err := store.pool.Exec(context.Background(), `UPDATE sectors SET contract_id = 1 WHERE sector_root = ANY($1)`, sqlRoots)
		if err != nil {
			b.Fatal(err)
		} else if res.RowsAffected() != int64(len(roots)) {
			b.Fatalf("expected %d rows updated, got %d", len(roots), res.RowsAffected())
		}
	}

	// helper to unpin all sectors
	unpinSectors := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), "UPDATE sectors set contract_id = NULL WHERE contract_id IS NOT NULL")
		if err != nil {
			b.Fatal(err)
		}
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			unpinSectors()
			unpinnedSectors := nSectors
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				unpinned, err := store.UnpinnedSectors(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(unpinned) != batchSize {
					b.Fatalf("expected %d unpinned sector, got %d (%d unpinned)", batchSize, len(unpinned), unpinnedSectors)
				}
				unpinnedSectors -= batchSize

				b.StopTimer()
				if unpinnedSectors < batchSize {
					// unpin all sectors to avoid running out
					unpinSectors()
					unpinnedSectors = nSectors
				} else {
					// pin fetched sectors to fetch different ones next
					pinSectors(unpinned)
				}
				b.StartTimer()
			}
		})
	}
}
