package postgres

import (
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSectorStatsNumSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account and host
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))
	hk := store.addTestHost(t)

	// helper to create slabs
	newSlab := func(i byte) slabs.SlabPinParams {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     10,
			Sectors: []slabs.PinnedSector{
				{
					Root:    frand.Entropy256(),
					HostKey: hk,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: hk,
				},
			},
		}
		return slab
	}

	assertStats := func(numSlabs int64) {
		t.Helper()
		stats, err := store.SectorStats(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if stats.Slabs != numSlabs {
			t.Fatalf("expected %d slabs, got %d", numSlabs, stats.Slabs)
		}
	}

	// we start with 0 slabs
	assertStats(0)

	// pin some slabs
	var pinned []slabs.SlabID
	for i := range byte(10) {
		slabIDs, err := store.PinSlabs(context.Background(), account, time.Now(), newSlab(i))
		if err != nil {
			t.Fatal(err)
		}
		pinned = append(pinned, slabIDs[0])
		assertStats(int64(len(pinned)))
	}

	// unpin them again
	for len(pinned) > 0 {
		slabID := pinned[0]
		if err := store.UnpinSlab(context.Background(), account, slabID); err != nil {
			t.Fatal(err)
		}
		pinned = pinned[1:]
		assertStats(int64(len(pinned)))
	}
}

// TestSectorStats is a regression test that verifies that the sector stats are
// updated correctly when sectors are pinned, unpinned, migrated and pruned.
func TestSectorStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	fcid := store.addTestContract(t, hk1, types.FileContractID(hk1))

	root := types.Hash256(frand.Entropy256())
	slab := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{{
			Root:    root,
			HostKey: hk1,
		}},
	}
	if _, err := store.PinSlabs(t.Context(), account, time.Time{}, slab); err != nil {
		t.Fatal(err)
	}

	assertStats := func(pinned, unpinned, unpinnable, migrated int64) {
		t.Helper()
		stats, err := store.SectorStats(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if stats.Pinned != pinned || stats.Unpinned != unpinned || stats.Unpinnable != unpinnable || stats.Migrated != migrated {
			t.Fatalf("unexpected sector stats: pinned=%d unpinned=%d unpinnable=%d migrated=%d", stats.Pinned, stats.Unpinned, stats.Unpinnable, stats.Migrated)
		}
	}

	assertStats(0, 1, 0, 0)

	if err := store.PinSectors(t.Context(), fcid, []types.Hash256{root}); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 0, 0, 0)

	if migrated, err := store.MigrateSector(t.Context(), root, hk2); err != nil {
		t.Fatal(err)
	} else if !migrated {
		t.Fatal("expected sector to be migrated")
	}
	assertStats(0, 1, 0, 1)

	var uploadedAt time.Time
	if err := store.pool.QueryRow(t.Context(), `
		SELECT uploaded_at
		FROM sectors
		WHERE sector_root = $1
	`, sqlHash256(root)).Scan(&uploadedAt); err != nil {
		t.Fatal(err)
	}
	if err := store.PruneUnpinnableSectors(t.Context(), uploadedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	assertStats(0, 0, 1, 1)

	if migrated, err := store.MigrateSector(t.Context(), root, hk1); err != nil {
		t.Fatal(err)
	} else if !migrated {
		t.Fatal("expected sector to be migrated")
	}
	assertStats(0, 1, 0, 2)

	if err := store.PinSectors(t.Context(), fcid, []types.Hash256{root}); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 0, 0, 2)
}

func TestAccountStatsRegistered(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	var accs []types.PublicKey
	for i := range 5 {
		if stats, err := store.AccountStats(t.Context()); err != nil {
			t.Fatal(err)
		} else if stats.Registered != uint64(i) {
			t.Fatalf("expected %d accounts, got %d", i, stats.Registered)
		}

		acc := types.GeneratePrivateKey().PublicKey()
		store.addTestAccount(t, acc)
		accs = append(accs, acc)
	}

	for i := range accs {
		if err := store.DeleteAccount(t.Context(), accs[i]); err != nil {
			t.Fatal(err)
		}

		if stats, err := store.AccountStats(t.Context()); err != nil {
			t.Fatal(err)
		} else if expected := uint64(len(accs)) - uint64(i) - 1; stats.Registered != expected {
			t.Fatalf("expected %d accounts, got %d", expected, stats.Registered)
		}
	}
}

func TestHostStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	updateUsageTotalSpent := func(hk types.PublicKey, spent types.Currency) {
		t.Helper()
		if _, err := store.pool.Exec(
			t.Context(),
			"UPDATE hosts SET usage_total_spent = $1 WHERE public_key = $2", sqlCurrency(spent), sqlPublicKey(hk)); err != nil {
			t.Fatal(err)
		}
	}

	// add three hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	hk3 := store.addTestHost(t)

	// assert empty stats
	stats, err := store.HostStats(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(stats))
	}

	// add test contracts
	fcid1 := store.addTestContract(t, hk1, types.FileContractID(hk1))
	store.addTestContract(t, hk2, types.FileContractID(hk2))
	store.addTestContract(t, hk3, types.FileContractID(hk3))

	// assert empty stats - no usage
	stats, err = store.HostStats(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(stats))
	}

	// update usage spent
	updateUsageTotalSpent(hk1, types.NewCurrency64(10))
	updateUsageTotalSpent(hk2, types.NewCurrency64(20))
	// hk3 remains at 0

	testRevision := newTestRevision(types.PublicKey{})

	// assert updated stats

	stats, err = store.HostStats(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(stats))
	} else if stats[0].PublicKey != hk2 {
		t.Fatalf("expected first host to be hk2, got %s", stats[0].PublicKey.String())
	} else if stats[1].PublicKey != hk1 {
		t.Fatalf("expected second host to be hk1, got %s", stats[1].PublicKey.String())
	} else if stats[0].ActiveContractsSize != int64(testRevision.Filesize) {
		t.Fatalf("expected first host to have %d active contract size, got %d", testRevision.Filesize, stats[0].ActiveContractsSize)
	} else if stats[1].ActiveContractsSize != int64(testRevision.Filesize) {
		t.Fatalf("expected second host to have %d active contract size, got %d", testRevision.Filesize, stats[1].ActiveContractsSize)
	}

	// resolve first contract manually - should exclude it from total_contract_size
	_, err = store.pool.Exec(t.Context(), "UPDATE contracts SET state = $1 WHERE contract_id = $2", sqlContractState(2), sqlHash256(fcid1))
	if err != nil {
		t.Fatal(err)
	}

	// assert updated stats
	stats, err = store.HostStats(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(stats))
	} else if stats[0].PublicKey != hk2 {
		t.Fatalf("expected first host to be hk2, got %s", stats[0].PublicKey.String())
	} else if stats[1].PublicKey != hk1 {
		t.Fatalf("expected second host to be hk1, got %s", stats[1].PublicKey.String())
	} else if stats[0].ActiveContractsSize != int64(testRevision.Filesize) {
		t.Fatalf("expected first host to have %d active contract size, got %d", testRevision.Filesize, stats[0].ActiveContractsSize)
	} else if stats[1].ActiveContractsSize != 0 {
		t.Fatalf("expected second host to have 0 active contract size, got %d", stats[1].ActiveContractsSize)
	}

	// set scanned height to the proof height - should exclude it
	proofHeight := testRevision.ProofHeight
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(context.Background(), types.ChainIndex{Height: proofHeight})
	}); err != nil {
		t.Fatal(err)
	}

	// assert updated stats
	stats, err = store.HostStats(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(stats))
	} else if stats[0].ActiveContractsSize != 0 {
		t.Fatalf("expected first host to have 0 active contract size, got %d", stats[0].ActiveContractsSize)
	} else if stats[1].ActiveContractsSize != 0 {
		t.Fatalf("expected second host to have 0 active contract size, got %d", stats[1].ActiveContractsSize)
	}

	// assert limit and offset are applied
	if stats, err := store.HostStats(t.Context(), 1, 1); err != nil {
		t.Fatal(err)
	} else if len(stats) != 1 {
		t.Fatalf("expected 1 host, got %d", len(stats))
	} else if stats[0].PublicKey != hk1 {
		t.Fatalf("expected host to be hk1, got %s", stats[0].PublicKey.String())
	} else if stats, err := store.HostStats(t.Context(), 2, 1); err != nil {
		t.Fatal(err)
	} else if len(stats) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(stats))
	}
}
