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
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestRecordIntegrityCheck(t *testing.T) {
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

	assertFailingSectors := func(expectedRoots []types.Hash256, minChecks, limit int) {
		t.Helper()
		roots, err := store.FailingSectors(context.Background(), hk, minChecks, limit)
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expectedRoots) {
			t.Fatalf("expected %d failing sectors, got %d", len(expectedRoots), len(roots))
		} else if len(roots) > 0 && !reflect.DeepEqual(roots, expectedRoots) {
			t.Fatalf("expected failing sectors %v, got %v", expectedRoots, roots)
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
	assertFailingSectors([]types.Hash256{}, 1, 10)

	// record success for both
	now := time.Now().Round(time.Microsecond)
	record(true, now, []types.Hash256{root1, root2})
	assertSectors(root1, now, 0)
	assertSectors(root2, now, 0)
	assertFailingSectors([]types.Hash256{}, 1, 10)

	// record failure for both
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1, root2})
	assertSectors(root1, now, 1)
	assertSectors(root2, now, 1)
	assertFailingSectors([]types.Hash256{root1, root2}, 1, 10)
	assertFailingSectors([]types.Hash256{root1}, 1, 1)

	// one more failure for root1 and success for root2
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1})
	record(true, now, []types.Hash256{root2})
	assertSectors(root1, now, 2)
	assertSectors(root2, now, 0)
	assertFailingSectors([]types.Hash256{root1}, 1, 10)
}

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
	hk := types.PublicKey{1}
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

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

	// unpinning for a non existing account should return [accounts.ErrNotFound]
	err = store.UnpinSlab(context.Background(), proto.Account{3}, slab3)
	if !errors.Is(err, accounts.ErrNotFound) {
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

	// create account and host
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

	// create 2 contracts
	contractID1, contractID2 := types.FileContractID{1}, types.FileContractID{2}
	if err := store.AddFormedContract(context.Background(), contractID1, hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		t.Fatal(err)
	} else if err := store.AddFormedContract(context.Background(), contractID2, hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		t.Fatal(err)
	}

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

	// exec is a helper to execute a query and refresh unhealthy slabs
	exec := func(query string) {
		t.Helper()
		_, err := store.pool.Exec(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.pool.Exec(context.Background(), `REFRESH MATERIALIZED VIEW CONCURRENTLY unhealthy_slabs`)
		if err != nil {
			t.Fatal(err)
		}
	}

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
	exec("UPDATE sectors SET contract_sectors_map_id = 1 WHERE id = 1")

	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// update the contract to be bad
	exec("UPDATE contracts SET good = FALSE")

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
	exec("UPDATE contracts SET good = TRUE")
	_, err = store.UnhealthySlab(context.Background(), time.Now().Add(-time.Hour))
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// remove a sector from its host - the unhealthy slab should be back
	exec("UPDATE sectors SET host_id = NULL WHERE id = 2")
	unhealthyID, err = store.UnhealthySlab(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	} else if slabID != unhealthyID {
		t.Fatalf("expected slab ID %v, got %v", slabID, unhealthyID)
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
//
// Hardware |     Benchmark   |  ms/op  |  Throughput   |
// M2 Pro   |     PinSlab     |  1.4ms  | 26590.43 MB/s |
//
// M2 Pro   |   Slabs-40MiB   |   0.5ms | 64021.45 MB/s |
// M2 Pro   |   Slabs-400MiB  |   0.8ms | 40447.43 MB/s |
// M2 Pro   |   Slabs-4GiB    |   3.3ms |  9930.88 MB/s |
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
					err := store.PinSectors(context.Background(), types.FileContractID(hk), unpinned)
					if err != nil {
						b.Fatal(err)
					}
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
		slabIDs, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
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

// BenchmarkPinSectors benchmarks PinSectors in various batch sizes.
//
// CPU    | BatchSize |	 Count  |   Time/op     |   Throughput
// M2 Pro |     10    |   1335  |    0.860 ms   |     48721.98 MB/s
// M2 Pro |    100    |   400   |    2.637 ms   |     159044.64 MB/s
// M2 Pro |   1000    |   56    |   19.966 ms   |     210065.00 MB/s
func BenchmarkPinSectors(b *testing.B) {
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
//
//	CPU    |  Count  |   Time/op
//	M1 Max |   100   |   1.63 ms
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

	// populate the materialized view
	_, err = store.pool.Exec(context.Background(), `REFRESH MATERIALIZED VIEW CONCURRENTLY unhealthy_slabs`)
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
//
//	CPU    |   Count  |    Time/op
//	M1 Max |    100   |    2.197 ms
func BenchmarkUnpinSlab(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add host
	hk := types.PublicKey{1}
	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		b.Fatal(err)
	}

	// add contract
	if err := store.AddFormedContract(context.Background(), types.FileContractID(hk), hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		b.Fatal(err)
	}

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
//
//	CPU  | BatchSize |	  Count  |     Time/op     |   Throughput
//
// M2 Pro |    10     |    957   |     3.80024 ms  |   19291.60 MB/s
// M2 Pro |   100     |    280   |    4.057123 ms  |  103381.24 MB/s
// M2 Pro |  1000     |     37   |   28.196233 ms  |  148754.05 MB/s
func BenchmarkRecordIntegrityChecks(b *testing.B) {
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
		slabIDs, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		} else if len(slabIDs) != 1 {
			b.Fatal("expected 1 slab id")
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

// BenchmarkFailingSectors benchmarks FailingSectors.
//
//	CPU  | BatchSize |	  Count  |     Time/op     |   Throughput
//
// M2 Pro |   100     |    496   |    2.260241 ms  |  185568.92 MB/s
// M2 Pro |  1000     |    100   |   13.477551 ms  |  311206.68 MB/s
// M2 Pro | 10000     |     44   |   73.282087 ms  |  572350.51 MB/s
func BenchmarkFailingSectors(b *testing.B) {
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
		slabIDs, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		} else if len(slabIDs) != 1 {
			b.Fatal("expected 1 slab id")
		}
	}

	// 50% of the sectors are bad
	reset := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET consecutive_failed_checks = 1 WHERE id % 2 = 0`)
		if err != nil {
			b.Fatal(err)
		}
	}
	reset()
	remainingSectors := nSectors / 2

	// run benchmark for various batch sizes
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				// reset if necessary
				if remainingSectors < batchSize {
					b.StopTimer()
					reset()
					b.StartTimer()
					remainingSectors = nSectors / 2
				}

				// fetch batch
				batch, err := store.FailingSectors(context.Background(), hk, 1, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(batch) != batchSize {
					b.Fatalf("no full batch was returned: %d", len(batch))
				}
				b.StopTimer()

				// mark the batch as good
				err = store.RecordIntegrityCheck(context.Background(), true, time.Now(), hk, batch)
				if err != nil {
					b.Fatal(err)
				}
				remainingSectors -= batchSize
				b.StartTimer()
			}
		})
	}
}

func TestMarkSectorsLost(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add hosts and contracts
	hk1 := types.PublicKey{1}
	hk2 := types.PublicKey{2}
	for _, hk := range []types.PublicKey{hk1, hk2} {
		ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
		}); err != nil {
			t.Fatal(err)
		}
		err := store.AddFormedContract(context.Background(), types.FileContractID(hk), hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(3))
		if err != nil {
			t.Fatal(err)
		}
	}

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
//
// CPU    | BatchSize |	 Count  |     Time/op    |    Throughput
// M2 Pro |     100   |    638  |   1.903132 ms  |   220389.59 MB/s
// M2 Pro |    1000   |     94  |  12.430024 ms  |   337433.29 MB/s
// M2 Pro |   10000   |     10  | 112.704779 ms  |   372149.61 MB/s
func BenchmarkMarkSectorsLost(b *testing.B) {
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
