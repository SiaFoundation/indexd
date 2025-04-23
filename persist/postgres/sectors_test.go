package postgres

import (
	"context"
	"errors"
	"fmt"
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
	"go.uber.org/zap"
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
	_, err := store.PinSlabs(context.Background(), account, nextCheck, []slabs.SlabPinParams{{}})
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
		slabID, err := slab.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return slabID, slab
	}

	// pin slabs
	slab1ID, slab1 := newSlab(1)
	slab2ID, slab2 := newSlab(2)
	toPin := []slabs.SlabPinParams{slab1, slab2}
	expectedIDs := []slabs.SlabID{slab1ID, slab2ID}
	slabIDs, err := store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, toPin)
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
	slabIDs, err = store.PinSlabs(context.Background(), account2, nextCheck, toPin)
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

func TestUnhealthySlabs(t *testing.T) {
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

	// add contract
	err := store.AddFormedContract(context.Background(), types.FileContractID(hk), hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(3))
	if err != nil {
		t.Fatal(err)
	}

	// pin a slab to add a few sectors to the database
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, []slabs.SlabPinParams{
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
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// make sure some time passes since the default time that is set when the
	// slab is pinned
	time.Sleep(100 * time.Millisecond)

	// after pinning, no slab should be unhealthy since their sectors aren't
	// pinned to contracts yet.
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// pin one sector to the contract - we should still not have any unhealthy sectors
	if _, err := store.pool.Exec(context.Background(), "UPDATE sectors SET contract_sectors_map_id = 1 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// update the contract to be bad
	if _, err := store.pool.Exec(context.Background(), "UPDATE contracts SET good = FALSE"); err != nil {
		t.Fatal(err)
	}

	// fetch unhealthy slabs which haven't had a repair attempted in at least 1 hour - should not have any
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(-time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// try again with the current time - this should return the slab since it has 1 bad sector
	unhealthyID, err := store.UnhealthySlab(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	} else if slabIDs[0] != unhealthyID {
		t.Fatalf("expected slab ID %v, got %v", slabIDs[0], unhealthyID)
	}

	// run again for 50ms - shouldn't return the same slab twice since the last_repair_attempt was updated
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(-50*time.Millisecond))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// fix the contract again
	if _, err := store.pool.Exec(context.Background(), "UPDATE contracts SET good = TRUE"); err != nil {
		t.Fatal(err)
	}
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(-time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// remove a sector from its host - the unhealthy slab should be back
	if _, err := store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	unhealthyID, err = store.UnhealthySlab(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	} else if slabIDs[0] != unhealthyID {
		t.Fatalf("expected slab ID %v, got %v", slabIDs[0], unhealthyID)
	}
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
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, []slabs.SlabPinParams{
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
		res, err := store.pool.Exec(context.Background(), `UPDATE sectors SET contract_sectors_map_id=$1, host_id=$2, uploaded_at=$3 WHERE id=$4`, contractID, hostID, uploadedAt, sid)
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
// M2 Pro   | PinSlabs-40MiB  |  1.2ms | 33885.23 MB/s |
// M2 Pro   | PinSlabs-400MiB |  8.6ms |  4875.42 MB/s |
// M2 Pro   | PinSlabs-4GiB   | 86.7ms |   483.63 MB/s |
//
// M2 Pro   | Slabs-40MiB  |  0.5ms |    70382.04 MB/s |
// M2 Pro   | Slabs-400MiB |  0.8ms |    52154.24 MB/s |
// M2 Pro   | Slabs-4GiB   |  3.3ms |    12668.21 MB/s |
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

// BenchmarkUnpinnedSectors benchmarks UnpinnedSectors in various batch sizes.
//
// CPU    | BatchSize |	 Count  |    Time/op    |    Throughput
// M2 Pro |     100   |   1357  |  0.847729 ms  |   494769.40 MB/s
// M2 Pro |    1000   |    434  |  3.023359 ms  |  1387299.30 MB/s
// M2 Pro |   10000   |     84  | 21.598813 ms  |  1941914.13 MB/s
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
		slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, []slabs.SlabPinParams{{
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
		_, err := store.pool.Exec(context.Background(), `
			UPDATE sectors
			SET contract_id = NULL,
			uploaded_at = NOW() - interval '1 week' * random()
			WHERE contract_id IS NOT NULL`)
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

// BenchmarkSectorsForIntegrityCheck benchmarks SectorsForIntegrityCheck
//
//	CPU  | BatchSize |	  Count  |     Time/op     |   Throughput
//
// M2 Pro |    10     |   2857   |    0.380024 ms  |  110369.50 MB/s
// M2 Pro |   100     |   2780   |    0.428167 ms  |  979595.01 MB/s
// M2 Pro |  1000     |   1497   |    0.790556 ms  | 5305513.99 MB/s
func BenchmarkSectorsForIntegrityCheck(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add a host
	hk := types.PublicKey{1}
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
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
		slabIDs, err := store.PinSlabs(context.Background(), account, time.Now().Add(time.Hour), []slabs.SlabPinParams{{
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

	// update next_integrity_check to random value in the past
	_, err := store.pool.Exec(context.Background(), `
		UPDATE sectors SET next_integrity_check = NOW() - interval '1 week' * random()
	`)
	if err != nil {
		b.Fatal(err)
	}

	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				batch, err := store.SectorsForIntegrityCheck(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(batch) != batchSize {
					b.Fatalf("no full batch was returned: %d", len(batch))
				}
			}
		})
	}
}

// BenchmarkUnhealthySlab benchmarks UnhealthySlab
//
//	CPU   |	  Count  |   Time/op
//
// M1 Max |   782   |    3.5 ms
func BenchmarkUnhealthySlab(b *testing.B) {
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

	// add 1 good and 1 bad contract
	err := store.AddFormedContract(context.Background(), types.FileContractID(hks[0]), hks[0], 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(3))
	if err != nil {
		b.Fatal(err)
	}
	err = store.AddFormedContract(context.Background(), types.FileContractID(hks[1]), hks[1], 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(3))
	if err != nil {
		b.Fatal(err)
	}
	res, err := store.pool.Exec(context.Background(), "UPDATE contracts SET good = FALSE WHERE id = 2") // id 2 is bad
	if err != nil {
		b.Fatal(err)
	} else if res.RowsAffected() != 1 {
		b.Fatal("expected to update 1 row")
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
	_, err = store.PinSlabs(context.Background(), account, time.Time{}, initialSlabs)
	if err != nil {
		b.Fatal(err)
	}

	// make sure the slabs have a last_repair_attempt time between 1 and 7 days
	// in the past
	_, err = store.pool.Exec(context.Background(), "UPDATE slabs SET last_repair_attempt = NOW() - interval '1 day' - interval '1 week' * random()")
	if err != nil {
		b.Fatal(err)
	}

	// default to the good contract for all sectors
	_, err = store.pool.Exec(context.Background(), `UPDATE sectors SET contract_sectors_map_id = 1`)
	if err != nil {
		b.Fatal(err)
	}

	// 25% of the sectors are stored on a bad contract
	_, err = store.pool.Exec(context.Background(), `UPDATE sectors SET contract_sectors_map_id = 2 WHERE id % 4 = 0`)
	if err != nil {
		b.Fatal(err)
	}

	// 25% of the sectors don't have a host at all
	_, err = store.pool.Exec(context.Background(), `UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id % 4 = 1`)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(b.N), "slabs")
	b.ResetTimer()

	seenSlabs := make(map[slabs.SlabID]struct{})
	for b.Loop() {
		slabID, err := store.UnhealthySlab(context.Background(), time.Now().Add(-time.Hour))
		if err != nil {
			b.Fatal(err)
		} else if _, exists := seenSlabs[slabID]; exists {
			b.Fatal("known slab was returned")
		}
		seenSlabs[slabID] = struct{}{}
	}
}
