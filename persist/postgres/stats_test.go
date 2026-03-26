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
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
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
	store.addTestContract(t, hk)

	// helper to create slabs
	newSlab := func(i byte) slabs.SlabPinParams {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     1,
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
		stats, err := store.SectorStats()
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
		slabIDs, err := store.PinSlabs(account, time.Now(), newSlab(i))
		if err != nil {
			t.Fatal(err)
		}
		pinned = append(pinned, slabIDs[0])
		assertStats(int64(len(pinned)))
	}

	// unpin them again
	for len(pinned) > 0 {
		slabID := pinned[0]
		if err := store.UnpinSlab(account, slabID); err != nil {
			t.Fatal(err)
		}
		pinned = pinned[1:]
		assertStats(int64(len(pinned)))
	}
}

func TestSectorStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	assertStats := func(pinned, unpinned, unpinnable, migrated int64) {
		t.Helper()
		stats, err := store.SectorStats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.Pinned != pinned || stats.Unpinned != unpinned || stats.Unpinnable != unpinnable || stats.Migrated != migrated {
			t.Fatalf("unexpected sector stats: pinned=%d unpinned=%d unpinnable=%d migrated=%d", stats.Pinned, stats.Unpinned, stats.Unpinnable, stats.Migrated)
		}
	}

	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	hk3 := store.addTestHost(t)
	hk4 := store.addTestHost(t)
	store.addTestContract(t, hk2)
	store.addTestContract(t, hk3)
	fcidHK1 := store.addTestContract(t, hk1, types.FileContractID{1})
	fcidHK4 := store.addTestContract(t, hk4, types.FileContractID{2})

	roots := []types.Hash256{
		frand.Entropy256(),
		frand.Entropy256(),
		frand.Entropy256(),
	}

	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{HostKey: hk1, Root: roots[0]},
			{HostKey: hk2, Root: roots[1]},
			{HostKey: hk3, Root: roots[2]},
		},
	}
	if _, err := store.PinSlabs(account, time.Time{}, params); err != nil {
		t.Fatal(err)
	}
	assertStats(0, 3, 0, 0)

	if err := store.PinSectors(fcidHK1, []types.Hash256{roots[0]}); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 2, 0, 0) // r0 is pinned

	var uploadedAt time.Time
	if err := store.pool.QueryRow(t.Context(), `
		SELECT uploaded_at
		FROM sectors
		WHERE sector_root = $1
	`, sqlHash256(roots[0])).Scan(&uploadedAt); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSectorsUnpinnable(uploadedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 0, 2, 0) // r0 still pinned, others unpinnable

	// migrate sectors to h2
	_, err1 := store.MigrateSector(roots[1], hk4)
	_, err2 := store.MigrateSector(roots[2], hk4)
	if err := errors.Join(err1, err2); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 2, 0, 2) // r0 still pinned, others unpinned and 2 migrated

	if err := store.PinSectors(fcidHK4, []types.Hash256{roots[1], roots[2]}); err != nil {
		t.Fatal(err)
	}
	assertStats(3, 0, 0, 2) // all roots pinned

	// h1 lost the sector
	if err := store.MarkSectorsLost(hk1, []types.Hash256{roots[0]}); err != nil {
		t.Fatal(err)
	}
	assertStats(2, 0, 1, 2) // r0 is unpinnable

	if _, err := store.pool.Exec(t.Context(), `
		UPDATE sectors
		SET consecutive_failed_checks = 10
		WHERE sector_root = $1
	`, sqlHash256(roots[1])); err != nil {
		t.Fatal(err)
	}

	if err := store.MarkFailingSectorsLost(hk4, 10); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 0, 2, 2) // r0 and r1 are unpinnable

	_, err1 = store.MigrateSector(roots[0], hk4)
	_, err2 = store.MigrateSector(roots[1], hk4)
	if err := errors.Join(err1, err2); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 2, 0, 4) // r2 is still pinned, r0 and r1 migrated and unpinned

	if err := store.PinSectors(fcidHK4, []types.Hash256{roots[0], roots[1]}); err != nil {
		t.Fatal(err)
	}

	assertStats(3, 0, 0, 4) // all sectors are pinned

	// the following section verifies MarkSectorsLost properly tracks both
	// pinned and unpinned sectors, moving them to unpinnable but more
	// importantly correctly decrementing from pinned/unpinned stats
	if err := store.MarkSectorsLost(hk4, []types.Hash256{roots[0]}); err != nil {
		t.Fatal(err)
	}
	assertStats(2, 0, 1, 4)

	if _, err := store.MigrateSector(roots[0], hk4); err != nil {
		t.Fatal(err)
	}
	assertStats(2, 1, 0, 5)

	if err := store.MarkSectorsLost(hk4, roots); err != nil {
		t.Fatal(err)
	}
	assertStats(0, 0, 3, 5)

	// the following section verifies BlockHosts properly unpins the sectors and
	// updates the stats accordingly
	if _, err := store.MigrateSector(roots[0], hk1); err != nil {
		t.Fatal(err)
	}
	assertStats(0, 1, 2, 6)

	err := store.BlockHosts([]types.PublicKey{hk1}, []string{t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	assertStats(0, 0, 3, 6)

	if unpinned, err := store.UnpinnedSectors(hk1, 1); err != nil {
		t.Fatal(err)
	} else if len(unpinned) != 0 {
		t.Fatalf("expected 0 unpinned sectors, got %d", len(unpinned))
	}
}

func TestIntegrityCheckStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add host
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin a slab to add 2 sectors
	pinTime := time.Now().Round(time.Microsecond)
	root1 := types.Hash256{1}
	root2 := types.Hash256{2}
	_, err := store.PinSlabs(account, pinTime, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
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

	assertSectorStats := func(expectedLost, expectedChecked, expectedCheckFailed int64) {
		t.Helper()
		var lost, checked, checkFailed int64
		err := store.pool.QueryRow(context.Background(), `
			SELECT
				(SELECT stat_value FROM stats WHERE stat_name = $1),
				(SELECT stat_value FROM stats WHERE stat_name = $2),
				(SELECT stat_value FROM stats WHERE stat_name = $3)`,
			statSectorsLost, statSectorsChecked, statSectorsCheckFailed,
		).Scan(&lost, &checked, &checkFailed)
		if err != nil {
			t.Fatal(err)
		}
		if lost != expectedLost || checked != expectedChecked || checkFailed != expectedCheckFailed {
			t.Fatalf("unexpected sector stats: lost=%d (want %d) checked=%d (want %d) checkFailed=%d (want %d)", lost, expectedLost, checked, expectedChecked, checkFailed, expectedCheckFailed)
		}
	}

	record := func(success bool, nextCheck time.Time, roots []types.Hash256) {
		t.Helper()
		err := store.RecordIntegrityCheck(success, nextCheck, hk, roots)
		if err != nil {
			t.Fatal(err)
		}
	}

	// check initial state - 0 failures
	assertSectorStats(0, 0, 0)

	// record success for both
	now := time.Now().Round(time.Microsecond)
	record(true, now, []types.Hash256{root1, root2})
	assertSectorStats(0, 2, 0)

	// record failure for both
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1, root2})
	assertSectorStats(0, 4, 2)

	// one more failure for root1 and success for root2
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1})
	record(true, now, []types.Hash256{root2})
	assertSectorStats(0, 6, 3)

	// mark sectors lost with a threshold of 3 which is too high to mark
	// root1 as lost
	if err := store.MarkFailingSectorsLost(hk, 3); err != nil {
		t.Fatal(err)
	}
	assertSectorStats(0, 6, 3)

	// one more time with threshold of 2
	if err := store.MarkFailingSectorsLost(hk, 2); err != nil {
		t.Fatal(err)
	}

	// host should have lost sector
	assertSectorStats(1, 6, 3)

	// marking both lost should result in lost=2 because root1 is already lost
	if err := store.MarkSectorsLost(hk, []types.Hash256{root1, root2}); err != nil {
		t.Fatal(err)
	}
	assertSectorStats(2, 6, 3)
}

func TestAccountStatsRegistered(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	var accs []types.PublicKey
	for i := range 5 {
		if stats, err := store.AccountStats(); err != nil {
			t.Fatal(err)
		} else if stats.Registered != uint64(i) {
			t.Fatalf("expected %d accounts, got %d", i, stats.Registered)
		}

		acc := types.GeneratePrivateKey().PublicKey()
		store.addTestAccount(t, acc)
		accs = append(accs, acc)
	}

	for i := range accs {
		if err := store.DeleteAccount(proto.Account(accs[i])); err != nil {
			t.Fatal(err)
		} else if err := store.PruneAccounts(1); err != nil {
			t.Fatal(err)
		}

		if stats, err := store.AccountStats(); err != nil {
			t.Fatal(err)
		} else if expected := uint64(len(accs)) - uint64(i) - 1; stats.Registered != expected {
			t.Fatalf("expected %d accounts, got %d", expected, stats.Registered)
		}
	}
}

func TestAppStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	appID1 := types.Hash256{1}
	appID2 := types.Hash256{2}

	addAccountWithApp := func(ak types.PublicKey, appID types.Hash256, name string) {
		t.Helper()
		connectKey := fmt.Sprintf("test-connect-key-%x", frand.Bytes(8))
		_, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
			Key:         connectKey,
			Description: "test connect key",
			Quota:       "default",
		})
		if err != nil {
			t.Fatal(err)
		}
		err = store.transaction(func(ctx context.Context, tx *txn) error {
			return addAccount(ctx, tx, connectKey, ak, accounts.AppMeta{ID: appID, Name: name})
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertStats := func(appID types.Hash256, expectedName string, expectedAccounts, expectedActive, expectedPinnedData uint64) {
		t.Helper()
		all, err := store.AppStats(0, 100)
		if err != nil {
			t.Fatal(err)
		}
		var stats admin.AppStats
		var found bool
		for _, s := range all {
			if s.AppID == appID {
				stats = s
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("app %s not found in stats", appID)
		}
		if stats.Name != expectedName {
			t.Fatalf("expected name %q, got %q", expectedName, stats.Name)
		} else if stats.Accounts != expectedAccounts {
			t.Fatalf("expected %d accounts, got %d", expectedAccounts, stats.Accounts)
		} else if stats.Active != expectedActive {
			t.Fatalf("expected %d active, got %d", expectedActive, stats.Active)
		} else if stats.PinnedData != expectedPinnedData {
			t.Fatalf("expected %d pinned data, got %d", expectedPinnedData, stats.PinnedData)
		}
	}

	// add accounts to app1
	acc1 := types.GeneratePrivateKey().PublicKey()
	acc2 := types.GeneratePrivateKey().PublicKey()
	addAccountWithApp(acc1, appID1, "App One")
	addAccountWithApp(acc2, appID1, "App One")

	// add account to app2
	acc3 := types.GeneratePrivateKey().PublicKey()
	addAccountWithApp(acc3, appID2, "App Two")

	// all accounts are active (recently created)
	assertStats(appID1, "App One", 2, 2, 0)
	assertStats(appID2, "App Two", 1, 1, 0)

	// set pinned_data for some accounts
	if _, err := store.pool.Exec(t.Context(), `UPDATE accounts SET pinned_data = 100 WHERE public_key = $1`, sqlPublicKey(acc1)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(t.Context(), `UPDATE accounts SET pinned_data = 200 WHERE public_key = $1`, sqlPublicKey(acc2)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(t.Context(), `UPDATE accounts SET pinned_data = 500 WHERE public_key = $1`, sqlPublicKey(acc3)); err != nil {
		t.Fatal(err)
	}
	assertStats(appID1, "App One", 2, 2, 300)
	assertStats(appID2, "App Two", 1, 1, 500)

	// make acc1 inactive by setting last_used sufficiently before the inactivity threshold
	inactiveTime := time.Now().Add(-accounts.AccountActivityThreshold - time.Hour)
	if _, err := store.pool.Exec(t.Context(), `UPDATE accounts SET last_used = $1 WHERE public_key = $2`, inactiveTime, sqlPublicKey(acc1)); err != nil {
		t.Fatal(err)
	}
	assertStats(appID1, "App One", 2, 1, 300)

	// soft-delete acc2 — should be excluded entirely
	if err := store.DeleteAccount(proto.Account(acc2)); err != nil {
		t.Fatal(err)
	}
	assertStats(appID1, "App One", 1, 0, 100) // only acc1 remains (inactive)
	assertStats(appID2, "App Two", 1, 1, 500) // app2 unaffected
}

func TestHostStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	updateUsageTotalSpent := func(hk types.PublicKey, spent types.Currency) {
		t.Helper()
		if _, err := store.pool.Exec(t.Context(), "UPDATE hosts SET usage_total_spent = $1 WHERE public_key = $2", sqlCurrency(spent), sqlPublicKey(hk)); err != nil {
			t.Fatal(err)
		}
	}

	// add three hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	hk3 := store.addTestHost(t)

	// assert empty stats
	stats, err := store.HostStats(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(stats))
	}

	// add test contracts
	fcid1 := store.addTestContract(t, hk1)
	store.addTestContract(t, hk2)
	store.addTestContract(t, hk3)

	// assert empty stats - no usage
	stats, err = store.HostStats(0, 10)
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

	stats, err = store.HostStats(0, 10)
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
	if stats[0].Blocked || stats[1].Blocked {
		t.Fatal("expected both hosts to be unblocked")
	}

	reason := t.Name()
	if err := store.BlockHosts([]types.PublicKey{hk1}, []string{reason}); err != nil {
		t.Fatal(err)
	}

	stats, err = store.HostStats(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(stats) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(stats))
	} else if stats[0].Blocked {
		t.Fatal("expected first host to remain unblocked")
	} else if !stats[1].Blocked {
		t.Fatal("expected second host to be blocked")
	} else if !reflect.DeepEqual(stats[1].BlockedReasons, []string{reason}) {
		t.Fatalf("expected blocked reasons %v, got %v", []string{reason}, stats[1].BlockedReasons)
	}

	// resolve first contract manually - should exclude it from total_contract_size
	_, err = store.pool.Exec(t.Context(), "UPDATE contracts SET state = $1 WHERE contract_id = $2", sqlContractState(2), sqlHash256(fcid1))
	if err != nil {
		t.Fatal(err)
	}

	// set protocol and release version for hk2
	protocolVersion := rhp.ProtocolVersion502
	const release = "hostd v2.4.1"
	_, err = store.pool.Exec(t.Context(), "UPDATE hosts SET settings_protocol_version = $1, settings_release = $2 WHERE public_key = $3", sqlProtocolVersion(protocolVersion), release, sqlPublicKey(hk2))
	if err != nil {
		t.Fatal(err)
	}

	// assert updated stats
	stats, err = store.HostStats(0, 10)
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
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(types.ChainIndex{Height: proofHeight})
	}); err != nil {
		t.Fatal(err)
	}

	// assert updated stats
	stats, err = store.HostStats(0, 10)
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
	if stats, err := store.HostStats(1, 1); err != nil {
		t.Fatal(err)
	} else if len(stats) != 1 {
		t.Fatalf("expected 1 host, got %d", len(stats))
	} else if stats[0].PublicKey != hk1 {
		t.Fatalf("expected host to be hk1, got %s", stats[0].PublicKey.String())
	} else if stats, err := store.HostStats(2, 1); err != nil {
		t.Fatal(err)
	} else if len(stats) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(stats))
	}

	// neither host should be usable (no scans)
	stats, err = store.HostStats(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if stats[0].Usable || stats[1].Usable {
		t.Fatal("expected neither host to be usable")
	} else if stats[0].GoodForUpload || stats[1].GoodForUpload {
		t.Fatal("expected neither host to be good for upload")
	}

	// lower global settings so test host settings pass usability checks, reset
	// scanned height so contracts are active, and scan hk2 so it has settings
	// and a last_successful_scan that let it pass usability checks
	if _, err := store.pool.Exec(t.Context(), `UPDATE global_settings SET
		hosts_min_protocol_version = $1, contracts_period = $2,
		contracts_renew_window = $3, hosts_min_collateral = $4`,
		sqlProtocolVersion(rhp.ProtocolVersion400), 100, 50, sqlCurrency(types.ZeroCurrency)); err != nil {
		t.Fatal(err)
	} else if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(types.ChainIndex{Height: 0})
	}); err != nil {
		t.Fatal(err)
	} else if err := store.UpdateHostScan(hk2, newTestHostSettings(hk2), geoip.Location{}, true, time.Now()); err != nil {
		t.Fatal(err)
	}

	// hk2 should be usable and good for upload; hk1 should not (resolved contract)
	stats, err = store.HostStats(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if stats[0].PublicKey != hk2 {
		t.Fatalf("expected stats[0] to be hk2, got %v", stats[0].PublicKey)
	} else if !stats[0].Usable {
		t.Fatal("expected hk2 to be usable")
	} else if !stats[0].GoodForUpload {
		t.Fatal("expected hk2 to be good for upload")
	} else if stats[1].PublicKey != hk1 {
		t.Fatalf("expected stats[1] to be hk1, got %v", stats[1].PublicKey)
	} else if stats[1].Usable {
		t.Fatal("expected hk1 to not be usable")
	} else if stats[1].GoodForUpload {
		t.Fatal("expected hk1 to not be good for upload")
	}

	// mark hk2 as stuck — still usable but not good for upload
	if err := store.UpdateStuckHosts([]types.PublicKey{hk2}); err != nil {
		t.Fatal(err)
	}
	stats, err = store.HostStats(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if stats[0].PublicKey != hk2 {
		t.Fatalf("expected stats[0] to be hk2, got %v", stats[0].PublicKey)
	} else if !stats[0].Usable {
		t.Fatal("expected hk2 to still be usable when stuck")
	} else if stats[0].GoodForUpload {
		t.Fatal("expected hk2 to not be good for upload when stuck")
	}

	// block hk2 — should no longer be usable or good for upload
	if err := store.BlockHosts([]types.PublicKey{hk2}, []string{"test"}); err != nil {
		t.Fatal(err)
	}
	stats, err = store.HostStats(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if stats[0].PublicKey != hk2 {
		t.Fatalf("expected stats[0] to be hk2, got %v", stats[0].PublicKey)
	} else if stats[0].Usable {
		t.Fatal("expected hk2 to not be usable when blocked")
	} else if stats[0].GoodForUpload {
		t.Fatal("expected hk2 to not be good for upload when blocked")
	}
}

func TestAggregatedHostStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	store.addTestContract(t, hk1)
	store.addTestContract(t, hk2)
	hs := newTestHostSettings(hk1)

	updateUsageTotalSpent := func(hk types.PublicKey, spent types.Currency) {
		t.Helper()
		if _, err := store.pool.Exec(t.Context(), "UPDATE hosts SET usage_total_spent = $1 WHERE public_key = $2", sqlCurrency(spent), sqlPublicKey(hk)); err != nil {
			t.Fatal(err)
		}
	}
	// set this so hosts show up in HostStats
	updateUsageTotalSpent(hk1, types.Siacoins(1))
	updateUsageTotalSpent(hk2, types.Siacoins(1))

	assertStats := func(expectedActiveHosts, expectedGoodForUpload uint64, expectedScans, expectedFailed int64) {
		t.Helper()

		stats, err := store.AggregatedHostStats()
		if err != nil {
			t.Fatal(err)
		} else if expectedActiveHosts != stats.Active {
			t.Fatalf("expected %d active hosts, got %d", expectedActiveHosts, stats.Active)
		} else if expectedGoodForUpload != stats.GoodForUpload {
			t.Fatalf("expected %d good for upload hosts, got %d", expectedGoodForUpload, stats.GoodForUpload)
		} else if expectedScans != stats.TotalScans {
			t.Fatalf("expected %d scans, got %d", expectedScans, stats.TotalScans)
		} else if expectedFailed != stats.FailedScans {
			t.Fatalf("expected %d failed scans, got %d", expectedFailed, stats.FailedScans)
		}
	}
	assertHost := func(hk types.PublicKey, expectedScans, expectedFailed int64) {
		t.Helper()

		hosts, err := store.HostStats(0, 500)
		if err != nil {
			t.Fatal(err)
		}
		for _, host := range hosts {
			if host.PublicKey != hk {
				continue
			}

			if expectedScans != host.Scans {
				t.Fatalf("expected %d scans, got %d", expectedScans, host.Scans)
			} else if expectedFailed != host.ScansFailed {
				t.Fatalf("expected %d scans, got %d", expectedFailed, host.ScansFailed)
			}
			return
		}
		t.Fatal("host missing from HostStats", hosts)
	}

	// both hosts have good contracts, so both should be active
	// neither has been scanned yet so remaining_storage is 0 — none good for upload
	assertStats(2, 0, 0, 0)
	assertHost(hk1, 0, 0)
	assertHost(hk2, 0, 0)

	// add successful scan - hk1 now has remaining_storage > 0
	if err := store.UpdateHostScan(hk1, hs, geoip.Location{}, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertStats(2, 1, 1, 0)
	assertHost(hk1, 1, 0)
	assertHost(hk2, 0, 0)

	// add failed scan
	if err := store.UpdateHostScan(hk1, hs, geoip.Location{}, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertStats(2, 1, 2, 1)
	assertHost(hk1, 2, 1)
	assertHost(hk2, 0, 0)

	// add another successful scan - hk2 now also has remaining_storage > 0
	if err := store.UpdateHostScan(hk2, hs, geoip.Location{}, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertStats(2, 2, 3, 1)
	assertHost(hk1, 2, 1)
	assertHost(hk2, 1, 0)

	// mark hk2 as stuck - should reduce both active and good for upload
	if err := store.UpdateStuckHosts([]types.PublicKey{hk2}); err != nil {
		t.Fatal(err)
	}
	assertStats(1, 1, 3, 1)

	// scans where host doesn't exist shouldn't affect stats
	if err := store.UpdateHostScan(types.GeneratePrivateKey().PublicKey(), hs, geoip.Location{}, true, time.Now()); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatalf("expected error %v, got %v", hosts.ErrNotFound, err)
	}
	assertStats(1, 1, 3, 1)
	assertHost(hk1, 2, 1)
	assertHost(hk2, 1, 0)

	if err := store.UpdateHostScan(types.GeneratePrivateKey().PublicKey(), hs, geoip.Location{}, false, time.Now()); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatalf("expected error %v, got %v", hosts.ErrNotFound, err)
	}
	assertStats(1, 1, 3, 1)
	assertHost(hk1, 2, 1)
	assertHost(hk2, 1, 0)
}

func TestConnectKeyStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// no connect keys yet
	stats, err := store.ConnectKeyStats()
	if err != nil {
		t.Fatal(err)
	} else if stats.Total != 0 {
		t.Fatalf("expected 0 total connect keys, got %d", stats.Total)
	} else if len(stats.Quotas) != 0 {
		t.Fatalf("expected 0 quotas, got %d", len(stats.Quotas))
	}

	// create a second quota
	fundTarget := uint64(1 << 30)
	if err := store.PutQuota("premium", accounts.PutQuotaRequest{
		Description:     "Premium quota",
		MaxPinnedData:   1e12,
		TotalUses:       100,
		FundTargetBytes: &fundTarget,
	}); err != nil {
		t.Fatal(err)
	}

	// add 3 keys on default, 2 on premium
	for i := range 3 {
		if _, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
			Key:         fmt.Sprintf("default-key-%d", i),
			Description: "default key",
			Quota:       "default",
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2 {
		if _, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
			Key:         fmt.Sprintf("premium-key-%d", i),
			Description: "premium key",
			Quota:       "premium",
		}); err != nil {
			t.Fatal(err)
		}
	}

	stats, err = store.ConnectKeyStats()
	if err != nil {
		t.Fatal(err)
	} else if stats.Total != 5 {
		t.Fatalf("expected 5 total connect keys, got %d", stats.Total)
	} else if len(stats.Quotas) != 2 {
		t.Fatalf("expected 2 quotas, got %d", len(stats.Quotas))
	}

	// quotas are ordered by name
	if stats.Quotas[0].Quota != "default" || stats.Quotas[0].Total != 3 {
		t.Fatalf("expected default quota with 3 keys, got %q with %d", stats.Quotas[0].Quota, stats.Quotas[0].Total)
	}
	if stats.Quotas[1].Quota != "premium" || stats.Quotas[1].Total != 2 {
		t.Fatalf("expected premium quota with 2 keys, got %q with %d", stats.Quotas[1].Quota, stats.Quotas[1].Total)
	}

	// delete a key and verify stats update
	if err := store.DeleteAppConnectKey("default-key-0"); err != nil {
		t.Fatal(err)
	}

	stats, err = store.ConnectKeyStats()
	if err != nil {
		t.Fatal(err)
	} else if stats.Total != 4 {
		t.Fatalf("expected 4 total connect keys, got %d", stats.Total)
	} else if stats.Quotas[0].Quota != "default" || stats.Quotas[0].Total != 2 {
		t.Fatalf("expected default quota with 2 keys, got %q with %d", stats.Quotas[0].Quota, stats.Quotas[0].Total)
	} else if stats.Quotas[1].Quota != "premium" || stats.Quotas[1].Total != 2 {
		t.Fatalf("expected premium quota with 2 keys, got %q with %d", stats.Quotas[1].Quota, stats.Quotas[1].Total)
	}
}
