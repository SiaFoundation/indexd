package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestMigrateSector(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add contract for first host
	fcid1 := store.addTestContract(t, hk1)

	// pin a slab to add 2 sectors which are both stored on the first host
	pinTime := time.Now().Round(time.Microsecond)
	root1 := types.Hash256{1}
	root2 := types.Hash256{2}
	_, err := store.PinSlab(context.Background(), account, pinTime, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    root1,
				HostKey: hk1,
			},
			{
				Root:    root2,
				HostKey: hk1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// pin sectors to contract
	if err := store.PinSectors(context.Background(), fcid1, []types.Hash256{root1, root2}); err != nil {
		t.Fatal(err)
	}

	// mark all sectors as having failed once
	if _, err := store.pool.Exec(context.Background(), `
		UPDATE sectors
		SET consecutive_failed_checks = 1
	`); err != nil {
		t.Fatal(err)
	}

	// helper to assert sector state
	assertSector := func(root types.Hash256, expectedHostKey types.PublicKey, expectedContractID types.FileContractID, expectedFailures int) {
		t.Helper()

		var hostKey types.PublicKey
		var contractID types.FileContractID
		var failures int
		err := store.pool.QueryRow(context.Background(), `
			SELECT hosts.public_key, contract_sectors_map.contract_id, consecutive_failed_checks
			FROM sectors
			INNER JOIN hosts ON sectors.host_id = hosts.id
			LEFT JOIN contract_sectors_map ON sectors.contract_sectors_map_id = contract_sectors_map.id
			WHERE sector_root = $1
		`, sqlHash256(root)).Scan(asNullable((*sqlPublicKey)(&hostKey)), asNullable((*sqlHash256)(&contractID)), &failures)
		if err != nil {
			t.Fatal(err)
		} else if hostKey != expectedHostKey {
			t.Fatalf("expected host key %v, got %v", expectedHostKey, hostKey)
		} else if contractID != expectedContractID {
			t.Fatalf("expected contract ID %v, got %v", expectedContractID, contractID)
		} else if failures != expectedFailures {
			t.Fatalf("expected %d consecutive failures, got %d", expectedFailures, failures)
		}
	}

	migrate := func(root types.Hash256, hostKey types.PublicKey, expectedMigrated bool) {
		t.Helper()
		if migrated, err := store.MigrateSector(context.Background(), root, hostKey); err != nil {
			t.Fatal(err)
		} else if migrated != expectedMigrated {
			t.Fatalf("expected migrated %v, got %v", expectedMigrated, migrated)
		}
	}

	// assert initial state
	assertSector(root1, hk1, fcid1, 1)
	assertSector(root2, hk1, fcid1, 1)

	// migrate sector 1 to host 2
	migrate(root1, hk2, true)
	assertSector(root1, hk2, types.FileContractID{}, 0)
	assertSector(root2, hk1, fcid1, 1)

	// migrate sector 2 to unknown host, this should be a no-op
	migrate(root2, types.PublicKey{10}, false)
	assertSector(root1, hk2, types.FileContractID{}, 0)
	assertSector(root2, hk1, fcid1, 1)

	// migrate sector 2 to host 2
	migrate(root2, hk2, true)
	assertSector(root1, hk2, types.FileContractID{}, 0)
	assertSector(root2, hk2, types.FileContractID{}, 0)
}

func TestRecordIntegrityCheck(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add host
	hk := store.addTestHost(t)

	// pin a slab to add 2 sectors
	pinTime := time.Now().Round(time.Microsecond)
	root1 := types.Hash256{1}
	root2 := types.Hash256{2}
	_, err := store.PinSlab(context.Background(), account, pinTime, slabs.SlabPinParams{
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
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// helper to assert sector state
	assertSectors := func(root types.Hash256, expectedNextCheck time.Time, expectedConsecutiveFailures int) {
		t.Helper()
		var nextCheck time.Time
		var consecutiveFailures int
		err := store.pool.QueryRow(context.Background(), "SELECT next_integrity_check, consecutive_failed_checks FROM sectors WHERE sector_root = $1", sqlHash256(root)).Scan(&nextCheck, &consecutiveFailures)
		if err != nil {
			t.Fatal(err)
		} else if expectedNextCheck != nextCheck {
			t.Fatalf("expected next check %v, got %v", expectedNextCheck, nextCheck)
		} else if consecutiveFailures != expectedConsecutiveFailures {
			t.Fatalf("expected %d consecutive failures, got %d", expectedConsecutiveFailures, consecutiveFailures)
		}
	}

	assertFailingSectors := func(expectedRoots []types.Hash256, minChecks int) {
		t.Helper()
		rows, err := store.pool.Query(context.Background(), `
			SELECT sector_root
			FROM sectors
   			WHERE
                host_id = (SELECT id FROM hosts WHERE public_key = $1)
                AND consecutive_failed_checks >= $2
		`, sqlPublicKey(hk), minChecks)
		if err != nil {
			t.Fatal(err)
		}
		var roots []types.Hash256
		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				t.Fatal(err)
			}
			roots = append(roots, root)
		}
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expectedRoots) {
			t.Fatalf("expected %d failing sectors, got %d", len(expectedRoots), len(roots))
		} else if len(roots) > 0 && !reflect.DeepEqual(roots, expectedRoots) {
			t.Fatalf("expected failing sectors %v, got %v", expectedRoots, roots)
		}
	}

	assertLostSectors := func(expected int) {
		t.Helper()
		var lostSectors int
		if err := store.pool.QueryRow(context.Background(), "SELECT lost_sectors FROM hosts WHERE public_key = $1", sqlPublicKey(hk)).
			Scan(&lostSectors); err != nil {
			t.Fatal(err)
		} else if lostSectors != expected {
			t.Fatalf("expected %d lost sectors, got %d", expected, lostSectors)
		}
	}

	record := func(success bool, nextCheck time.Time, roots []types.Hash256) {
		t.Helper()
		err := store.RecordIntegrityCheck(context.Background(), success, nextCheck, hk, roots)
		if err != nil {
			t.Fatal(err)
		}
	}

	// check initial state - 0 failures
	assertSectors(root1, pinTime, 0)
	assertSectors(root2, pinTime, 0)
	assertFailingSectors([]types.Hash256{}, 1)

	// record success for both
	now := time.Now().Round(time.Microsecond)
	record(true, now, []types.Hash256{root1, root2})
	assertSectors(root1, now, 0)
	assertSectors(root2, now, 0)
	assertFailingSectors([]types.Hash256{}, 1)

	// record failure for both
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1, root2})
	assertSectors(root1, now, 1)
	assertSectors(root2, now, 1)
	assertFailingSectors([]types.Hash256{root1, root2}, 1)

	// one more failure for root1 and success for root2
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1})
	record(true, now, []types.Hash256{root2})
	assertSectors(root1, now, 2)
	assertSectors(root2, now, 0)
	assertFailingSectors([]types.Hash256{root1}, 1)

	// host should not have any lost sectors
	assertLostSectors(0)

	// mark sectors lost with a threshold of 2 which is too high to mark
	// root1 as lost
	if err := store.MarkFailingSectorsLost(context.Background(), hk, 3); err != nil {
		t.Fatal(err)
	}
	assertFailingSectors([]types.Hash256{root1}, 2)

	// one more time with threshold of 1
	if err := store.MarkFailingSectorsLost(context.Background(), hk, 2); err != nil {
		t.Fatal(err)
	}
	assertFailingSectors([]types.Hash256{}, 2)

	// host should have lost sector
	assertLostSectors(1)
}

func TestSectorsForIntegrityCheck(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add host
	hk := store.addTestHost(t)

	// pin a slab to add a few sectors to the database
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	root4 := frand.Entropy256()
	_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
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
	_, err := store.PinSlab(context.Background(), account, nextCheck, slabs.SlabPinParams{})
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected ErrNotFound, got", err)
	}

	// add accounts
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	} else if err := store.AddAccount(context.Background(), types.PublicKey(account2)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

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
	for i := range toPin {
		slabID, err := store.PinSlab(context.Background(), proto.Account{1}, nextCheck, toPin[i])
		if err != nil {
			t.Fatal(err)
		} else if slabID != expectedIDs[i] {
			t.Fatalf("expected slab ID %v, got %v", expectedIDs[i], slabID)
		}
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
	fetched, err := store.Slabs(context.Background(), account, expectedIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(fetched) != len(toPin) {
		t.Fatalf("expected %d slabs, got %d", len(toPin), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// again but for wrong account
	_, err = store.Slabs(context.Background(), account2, expectedIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// pin same slabs for account 2 again which should add links to the join
	// table
	for i := range toPin {
		slabID, err := store.PinSlab(context.Background(), account2, nextCheck, toPin[i])
		if err != nil {
			t.Fatal(err)
		} else if slabID != expectedIDs[i] {
			t.Fatalf("expected slab IDs %v, got %v", expectedIDs[i], slabID)
		}
	}

	// fetch slabs for account 2
	fetched, err = store.Slabs(context.Background(), account2, expectedIDs)
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

	// swap roots of slab 2 and re-pin on account 2
	slab2.Sectors[0].Root, slab2.Sectors[1].Root = slab2.Sectors[1].Root, slab2.Sectors[0].Root
	slabID, err := store.PinSlab(context.Background(), account2, nextCheck, slab2)
	if err != nil {
		t.Fatal(err)
	} else if slabID == expectedIDs[0] || slabID == expectedIDs[1] {
		t.Fatalf("expected new slab ID, got %v (%v)", slabID, expectedIDs)
	}

	// assert we still have 3 slabs now, but still only have 4 sectors
	assertCount("account_slabs", 5) // 2 slabs for each account + the new one
	assertCount("slabs", 3)         // 3 slabs
	assertCount("sectors", 4)       // 2 sectors per slab + 0 new ones
}

func TestUnpinSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	assertAccountSlabs := func(acc proto.Account, expected int64) {
		t.Helper()
		var got int64
		query := `SELECT COUNT(*) FROM account_slabs INNER JOIN accounts ON account_slabs.account_id = accounts.id WHERE accounts.public_key = $1`
		err := store.pool.QueryRow(context.Background(), query, sqlHash256(acc)).Scan(&got)
		if err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d slabs for account %v, got %d", expected, acc, got)
		}
	}

	assertCount := func(name string, expected int64) {
		t.Helper()
		var got int64
		if err := store.pool.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&got); err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d rows in %s, got %d", expected, name, got)
		}
	}

	// add host
	hk := store.addTestHost(t)

	// precreate 3 slabs, 2 sectors each
	var params []slabs.SlabPinParams
	for range 3 {
		params = append(params, slabs.SlabPinParams{
			EncryptionKey: [32]byte{},
			MinShards:     2,
			Sectors: []slabs.SectorPinParams{
				{Root: frand.Entropy256(), HostKey: hk},
				{Root: frand.Entropy256(), HostKey: hk},
			},
		})
	}
	slab1, _ := params[0].Digest()
	slab2, _ := params[1].Digest()
	slab3, _ := params[2].Digest()

	// add an account with 2 slabs, 2 sectors each
	acc1 := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(acc1)); err != nil {
		t.Fatal("failed to add account:", err)
	}
	if _, err := store.PinSlab(context.Background(), acc1, time.Time{}, params[0]); err != nil {
		t.Fatal(err)
	} else if _, err := store.PinSlab(context.Background(), acc1, time.Time{}, params[1]); err != nil {
		t.Fatal(err)
	}

	// add another account with 2 slabs, the first one is shared with acc1
	acc2 := proto.Account{2}
	if err := store.AddAccount(context.Background(), types.PublicKey(acc2)); err != nil {
		t.Fatal("failed to add account:", err)
	}
	if _, err := store.PinSlab(context.Background(), acc2, time.Time{}, params[1]); err != nil {
		t.Fatal(err)
	} else if _, err := store.PinSlab(context.Background(), acc2, time.Time{}, params[2]); err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 2)
	assertAccountSlabs(acc2, 2)
	assertCount("slabs", 3)
	assertCount("sectors", 6)
	assertCount("account_slabs", 4)

	// unpinning a slab that's not pinned to an account should return [slabs.ErrNotFound]
	err := store.UnpinSlab(context.Background(), acc2, slab1)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal("unexpected error:", err)
	}

	// unpin first slab
	err = store.UnpinSlab(context.Background(), acc1, slab1)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 1)
	assertAccountSlabs(acc2, 2)
	assertCount("slabs", 2)
	assertCount("sectors", 4)
	assertCount("account_slabs", 3)

	// unpin second slab
	err = store.UnpinSlab(context.Background(), acc1, slab2)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 0)
	assertAccountSlabs(acc2, 2)
	assertCount("slabs", 2)
	assertCount("sectors", 4)
	assertCount("account_slabs", 2)

	// unpin second slab on second account
	err = store.UnpinSlab(context.Background(), acc2, slab2)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 0)
	assertAccountSlabs(acc2, 1)
	assertCount("slabs", 1)
	assertCount("sectors", 2)
	assertCount("account_slabs", 1)

	// unpin third slab on second account
	err = store.UnpinSlab(context.Background(), acc2, slab3)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 0)
	assertAccountSlabs(acc2, 0)
	assertCount("slabs", 0)
	assertCount("sectors", 0)
	assertCount("account_slabs", 0)
}

func TestPinSectors(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create host and account
	hk := store.addTestHost(t)
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// create 2 contracts
	contractID1 := store.addTestContract(t, hk, types.FileContractID{1})
	contractID2 := store.addTestContract(t, hk, types.FileContractID{2})

	// create 4 sectors
	_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
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
	})
	if err != nil {
		t.Fatal(err)
	}

	// set the sectors' host IDs to NULL to make sure PinSectors also sets
	// those
	res, err := store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL")
	if err != nil {
		t.Fatal(err)
	} else if res.RowsAffected() != 4 {
		t.Fatalf("expected 4 rows affected, got %d", res.RowsAffected())
	}

	// helper to assert sector is pinned
	assertPinned := func(sid int64, contractID *int64) {
		t.Helper()
		var selectedContractID, selectedHostID sql.NullInt64
		err := store.pool.QueryRow(context.Background(), "SELECT contract_sectors_map_id, host_id FROM sectors WHERE id = $1", sid).
			Scan(&selectedContractID, &selectedHostID)
		if err != nil {
			t.Fatal(err)
		} else if contractID != nil && !selectedContractID.Valid {
			t.Fatalf("expected contract ID %v, got nil", contractID)
		} else if contractID == nil && selectedContractID.Valid {
			t.Fatalf("expected nil contract ID, got %v", selectedContractID)
		} else if contractID != nil && selectedContractID.Int64 != *contractID {
			t.Fatalf("expected contract ID %v, got %v", *contractID, selectedContractID.Int64)
		} else if contractID != nil && (!selectedHostID.Valid || selectedHostID.Int64 != 1) {
			t.Fatal("expected host ID to be set to 1 if sector is pinned", selectedHostID.Int64)
		}
	}

	// none are pinned
	assertPinned(1, nil)
	assertPinned(2, nil)
	assertPinned(3, nil)
	assertPinned(4, nil)

	// pin sectors 1 and 3 to contract 1
	err = store.PinSectors(context.Background(), contractID1, []types.Hash256{{1}, {3}})
	if err != nil {
		t.Fatal(err)
	}
	one := int64(1)
	assertPinned(1, &one)
	assertPinned(2, nil)
	assertPinned(3, &one)
	assertPinned(4, nil)

	// pin sectors 2 and 4 to contract 2
	err = store.PinSectors(context.Background(), contractID2, []types.Hash256{{2}, {4}})
	if err != nil {
		t.Fatal(err)
	}
	two := int64(2)
	assertPinned(1, &one)
	assertPinned(2, &two)
	assertPinned(3, &one)
	assertPinned(4, &two)

	// pin to contract that doesn't exist
	err = store.PinSectors(context.Background(), types.FileContractID{9}, []types.Hash256{{2}})
	if !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal("expected ErrNotFound, got", err)
	}
}

func TestUnhealthySlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add host with a contract
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin a slab to add a few sectors to the database
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	slabID, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
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
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET contract_sectors_map_id = 1 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// update the contract to be bad
	_, err = store.pool.Exec(context.Background(), "UPDATE contracts SET good = FALSE")
	if err != nil {
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
	} else if slabID != unhealthyID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthyID)
	}

	// run again for 50ms - shouldn't return the same slab twice since the last_repair_attempt was updated
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(-50*time.Millisecond))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// fix the contract again
	_, err = store.pool.Exec(context.Background(), "UPDATE contracts SET good = TRUE")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(-time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// remove a sector from its host - the unhealthy slab should be back
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	unhealthyID, err = store.UnhealthySlab(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	} else if slabID != unhealthyID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthyID)
	}
}

func TestUnpinnedSectors(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create host with account and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// create 4 sectors
	_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
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
func BenchmarkSlabs(b *testing.B) {
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	account := proto.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// 30 hosts to simulate default redundancy
	var hks []types.PublicKey
	for i := byte(0); i < 30; i++ {
		hks = append(hks, store.addTestHost(b, types.PublicKey{i}))
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

	const dbBaseSize = 1 << 40         // 1TiB of sectors
	const slabSize = 40 * int64(1<<20) // 40MiB

	// prepare base db
	var initialSlabIDs []slabs.SlabID
	for range dbBaseSize / slabSize {
		slabID, err := store.PinSlab(context.Background(), account, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}
		initialSlabIDs = append(initialSlabIDs, slabID)
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

	// insert 40MiB of slab data
	b.Run("PinSlab", func(b *testing.B) {
		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			_, err := store.PinSlab(context.Background(), proto.Account{1}, time.Time{}, newSlab())
			if err != nil {
				b.Fatal(err)
			}
		}
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
func BenchmarkUnpinnedSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

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
		_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// randomize the uploaded_at time for all sectors
	_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET uploaded_at = NOW() - interval '1 week' * random()`)
	if err != nil {
		b.Fatal(err)
	}

	// define a helper to unpin all sectors between runs
	unpinSectors := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET contract_sectors_map_id = NULL`)
		if err != nil {
			b.Fatal(err)
		}
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			unpinSectors()
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				// fetch unpinned sectors
				unpinned, err := store.UnpinnedSectors(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				}

				// check if unpinned sectors are exhausted
				b.StopTimer()
				if len(unpinned) < batchSize {
					unpinSectors()
				}

				// pin sectors to ensure we fetch different ones next time
				err = store.PinSectors(context.Background(), types.FileContractID(hk), unpinned)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkSectorsForIntegrityCheck benchmarks SectorsForIntegrityCheck
func BenchmarkSectorsForIntegrityCheck(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add a host
	hk := store.addTestHost(b)

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
		if _, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
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

// BenchmarkPinSectors benchmarks PinSectors in various batch sizes.
func BenchmarkPinSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

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
		_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// helper to unpin all sectors
	unpinSectors := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `
			UPDATE sectors
			SET contract_sectors_map_id = NULL,
			uploaded_at = NOW() - interval '1 week' * random()
			WHERE contract_sectors_map_id IS NOT NULL`)
		if err != nil {
			b.Fatal(err)
		}
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			unpinSectors()
			unpinnedSectors := nSectors
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				b.StopTimer()

				// make sure there are enough unpinned sectors left
				if unpinnedSectors < batchSize {
					// unpin all sectors to avoid running out
					unpinSectors()
					unpinnedSectors = nSectors
				}

				// fetch sectors to pin
				unpinned, err := store.UnpinnedSectors(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(unpinned) != batchSize {
					b.Fatalf("expected %d unpinned sector, got %d (%d unpinned)", batchSize, len(unpinned), unpinnedSectors)
				}
				unpinnedSectors -= batchSize

				// pin fetched sectors to fetch different ones next
				b.StartTimer()
				err = store.PinSectors(context.Background(), types.FileContractID(hk), unpinned)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnhealthySlab benchmarks UnhealthySlab
func BenchmarkUnhealthySlab(b *testing.B) {
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	account := proto.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// 30 hosts to simulate default redundancy
	var hks []types.PublicKey
	for i := byte(0); i < 30; i++ {
		hks = append(hks, store.addTestHost(b, types.PublicKey{i}))
	}

	// add 2 contracts
	store.addTestContract(b, hks[0])
	store.addTestContract(b, hks[1])

	// mark the second contract as bad
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
	for range dbBaseSize / slabSize {
		_, err = store.PinSlab(context.Background(), account, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}
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

// BenchmarkUnpinSlab precreates slabs and benchmarks the performance of
// UnpinSlab. All slabs are referenced by the same account, every slab is only
// referenced once, that means that unpinning the slab will delete the
// reference, as well as the slab itself.
func BenchmarkUnpinSlab(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add host with one contract
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSlabs     = dbBaseSize / (30 * proto.SectorSize)
	)

	// insert slabs
	slabIDs := make([]slabs.SlabID, nSlabs)
	sectors := make([]slabs.SectorPinParams, 30)
	for i := range nSlabs {
		for j := range 30 {
			sectors[j] = slabs.SectorPinParams{
				Root:    frand.Entropy256(),
				HostKey: hk,
			}
		}
		slabID, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
		slabIDs[i] = slabID
	}

	var iter int
	for b.Loop() {
		rIdx := frand.Intn(len(slabIDs))
		if err := store.UnpinSlab(context.Background(), account, slabIDs[rIdx]); err != nil {
			b.Fatal(err)
		}

		slabIDs = slices.Delete(slabIDs, rIdx, rIdx+1)
		if len(slabIDs) == 0 {
			if iter < 100 {
				b.Fatalf("expected at least 100 iterations, got %d", iter) // sanity check
			}
			b.StopTimer()
			break // exhausted
		}
		iter++
	}
}

// BenchmarkRecordIntegrityChecks benchmarks RecordIntegrityCheck
func BenchmarkRecordIntegrityChecks(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add a host
	hk := store.addTestHost(b)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	sectorRoots := make([]types.Hash256, 0, nSectors)
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.SectorPinParams
		for range batchSize {
			root := frand.Entropy256()
			sectors = append(sectors, slabs.SectorPinParams{
				Root:    root,
				HostKey: hk,
			})
			sectorRoots = append(sectorRoots, root)
		}
		if _, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				b.StopTimer()
				frand.Shuffle(len(sectorRoots), func(i, j int) {
					sectorRoots[i], sectorRoots[j] = sectorRoots[j], sectorRoots[i]
				})
				batch := sectorRoots[:batchSize]
				success := frand.Intn(2) == 0
				b.StartTimer()

				err := store.RecordIntegrityCheck(context.Background(), success, time.Now(), hk, batch)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarkFailingSectorsLost benchmarks MarkFailingSectorsLost
func BenchmarkMarkFailingSectorsLost(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add a host
	hk := store.addTestHost(b)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	sectorRoots := make([]types.Hash256, 0, nSectors)
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.SectorPinParams
		for range batchSize {
			root := frand.Entropy256()
			sectors = append(sectors, slabs.SectorPinParams{
				Root:    root,
				HostKey: hk,
			})
			sectorRoots = append(sectorRoots, root)
		}
		if _, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	// 10% of the sectors are bad
	reset := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET consecutive_failed_checks = 1 WHERE consecutive_failed_checks = 0 AND id % 10 = 0`)
		if err != nil {
			b.Fatal(err)
		}
	}
	reset()

	for b.Loop() {
		b.SetBytes(proto.SectorSize * nSectors / 10)

		err := store.MarkFailingSectorsLost(context.Background(), hk, 1)
		if err != nil {
			b.Fatal(err)
		}

		// reset db
		b.StopTimer()
		reset()
		b.StartTimer()
	}
}

func TestMarkSectorsLost(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add a contract for each host
	store.addTestContract(t, hk1)
	store.addTestContract(t, hk2)

	// pin a slab that adds 2 sectors to each host
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	root4 := frand.Entropy256()
	_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    root1,
				HostKey: hk1,
			},
			{
				Root:    root2,
				HostKey: hk1,
			},
			{
				Root:    root3,
				HostKey: hk2,
			},
			{
				Root:    root4,
				HostKey: hk2,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertSectorLost := func(root types.Hash256, lost bool) {
		t.Helper()
		var isLost bool
		err := store.pool.QueryRow(context.Background(), `SELECT host_id IS NULL FROM sectors WHERE sector_root = $1`, sqlHash256(root)).Scan(&isLost)
		if err != nil {
			t.Fatal(err)
		} else if isLost != lost {
			t.Fatalf("expected sector %x to be lost: %v, got %v", root, lost, isLost)
		}
	}

	assertLostSectors := func(hostKey types.PublicKey, numLost int) {
		t.Helper()
		var count int
		err := store.pool.QueryRow(context.Background(), `SELECT lost_sectors FROM hosts WHERE public_key = $1`, sqlHash256(hostKey)).Scan(&count)
		if err != nil {
			t.Fatal(err)
		} else if count != numLost {
			t.Fatalf("expected %d lost sectors for host %x, got %d", numLost, hostKey, count)
		}
	}

	markSectorLost := func(hk types.PublicKey, roots []types.Hash256) {
		t.Helper()
		if err := store.MarkSectorsLost(context.Background(), hk, roots); err != nil {
			t.Fatal(err)
		}
	}

	// check that no sectors are lost
	assertLostSectors(hk1, 0)
	assertLostSectors(hk2, 0)
	assertSectorLost(root1, false)
	assertSectorLost(root2, false)
	assertSectorLost(root3, false)
	assertSectorLost(root4, false)

	// mark sectors 1 to 3 lost
	markSectorLost(hk1, []types.Hash256{root1, root2})
	markSectorLost(hk2, []types.Hash256{root3})

	assertLostSectors(hk1, 2)
	assertLostSectors(hk2, 1)
	assertSectorLost(root1, true)
	assertSectorLost(root2, true)
	assertSectorLost(root3, true)
	assertSectorLost(root4, false)

	// mark last sector lost as well
	markSectorLost(hk2, []types.Hash256{root4})

	assertLostSectors(hk1, 2)
	assertLostSectors(hk2, 2)
	assertSectorLost(root1, true)
	assertSectorLost(root2, true)
	assertSectorLost(root3, true)
	assertSectorLost(root4, true)
}

// BenchmarkMarkSectorsLost benchmarks MarkSectorsLost in various batch sizes.
func BenchmarkMarkSectorsLost(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

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
		_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// helper to mark sectors "unlost".
	markNotLost := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `
			UPDATE sectors
			SET contract_sectors_map_id = 1, host_id = 1
			WHERE contract_sectors_map_id IS NULL AND host_id IS NULL
		`)
		if err != nil {
			b.Fatal(err)
		}
	}

	// helper to find sectors to mark as lost
	sectorsToMark := func(batchSize int) []types.Hash256 {
		b.Helper()
		rows, err := store.pool.Query(context.Background(), `
			SELECT sector_root
			FROM sectors
			WHERE host_id IS NOT NULL
			LIMIT $1
		`, batchSize)
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()
		var roots []types.Hash256
		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				b.Fatal(err)
			}
			roots = append(roots, root)
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		return roots
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			markNotLost()
			goodSectors := nSectors
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				b.StopTimer()
				toMark := sectorsToMark(batchSize)
				if len(toMark) != batchSize {
					b.Fatalf("expected %d sectors to mark, got %d", batchSize, len(toMark))
				}
				goodSectors -= batchSize
				b.StartTimer()

				if goodSectors < batchSize {
					// reset all sectors to avoid running out
					b.StopTimer()
					markNotLost()
					goodSectors = nSectors
					b.StartTimer()
				} else {
					// pin fetched sectors to fetch different ones next
					err := store.MarkSectorsLost(context.Background(), hk, toMark)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkMigrateSector benchmarks MigrateSector.
func BenchmarkMigrateSector(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add 100 hosts and contracts
	var hks []types.PublicKey
	for range 100 {
		hk := store.addTestHost(b)
		store.addTestContract(b, hk)
		hks = append(hks, hk)
	}

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	hostIdx := 0
	var roots []types.Hash256
	rootsByContract := make(map[types.FileContractID][]types.Hash256)
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.SectorPinParams
		for range batchSize {
			hk := hks[hostIdx]
			root := frand.Entropy256()
			sectors = append(sectors, slabs.SectorPinParams{
				Root:    root,
				HostKey: hks[hostIdx],
			})
			rootsByContract[types.FileContractID(hk)] = append(rootsByContract[types.FileContractID(hk)], root)
			roots = append(roots, root)
			hostIdx = (hostIdx + 1) % len(hks)
		}

		// pin slab
		_, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// pin all sectors to contracts
	for contractID, roots := range rootsByContract {
		err := store.PinSectors(context.Background(), contractID, roots)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(proto.SectorSize)
	for b.Loop() {
		// migrate random sector to random host
		root := roots[frand.Intn(len(roots))]
		hostKey := hks[frand.Intn(len(hks))]
		_, err := store.MigrateSector(context.Background(), root, hostKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}
