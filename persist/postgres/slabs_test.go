package postgres

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	store.addTestAccount(t, types.PublicKey(account))

	// add hosts
	hosts := make([]types.PublicKey, 30)
	for i := range hosts {
		hosts[i] = store.addTestHost(t)
		store.addTestContract(t, hosts[i], frand.Entropy256())
	}

	// pin slab
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors:       make([]slabs.PinnedSector, 0, len(hosts)),
	}
	var expectedSectors []slabs.Sector
	for _, host := range hosts {
		root := frand.Entropy256()
		params.Sectors = append(params.Sectors, slabs.PinnedSector{
			Root:    root,
			HostKey: host,
		})
		expectedSectors = append(expectedSectors, slabs.Sector{
			Root:       root,
			HostKey:    &host,
			ContractID: nil, // not pinned to a contract
		})
	}

	// pin slab
	slabIDs, err := store.PinSlabs(account, time.Time{}, params)
	if err != nil {
		t.Fatal(err)
	}

	// fetch slab
	got, err := store.Slab(slabIDs[0])
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches the expected slab
	expected := slabs.Slab{
		ID:            params.Digest(),
		EncryptionKey: params.EncryptionKey,
		MinShards:     params.MinShards,
		Sectors:       expectedSectors,
		PinnedAt:      got.PinnedAt, // ignore pinned at
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected slab %v, got %v", expected, got)
	} else if expected.PinnedAt.IsZero() {
		t.Fatal("expected slab to be pinned at a non-zero time")
	}

	// pin the first sector to a contract
	hk := hosts[0]
	fcid := types.FileContractID(hk)
	if err := store.AddFormedContract(hk, fcid, newTestRevision(hk), types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(fcid, []types.Hash256{params.Sectors[0].Root}); err != nil {
		t.Fatal(err)
	}

	// fetch slab again
	got, err = store.Slab(slabIDs[0])
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches the expected slab with the pinned sector
	expected.Sectors[0].ContractID = &fcid
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected slab %v, got %v", expected, got)
	}
}

func TestMarkSlabRepaired(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add host
	host := store.addTestHost(t)
	store.addTestContract(t, host)

	// add slab
	slabIDs, err := store.PinSlabs(account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: frand.Entropy256(), HostKey: host},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertSlabState := func(expectedRepairs int, expectedNextAttempt time.Time) {
		t.Helper()

		roughlyEqual := func(a, b time.Time, slack time.Duration) bool {
			if diff := a.Sub(b); diff < -slack || diff > slack {
				return false
			}
			return true
		}

		var consecutiveFailedRepairs int
		var nextRepairAttempt time.Time
		if err := store.pool.QueryRow(t.Context(), `
			SELECT consecutive_failed_repairs, next_repair_attempt
			FROM slabs
			WHERE digest = $1`, sqlHash256(slabIDs[0])).Scan(&consecutiveFailedRepairs, &nextRepairAttempt); err != nil {
			t.Fatal(err)
		} else if consecutiveFailedRepairs != expectedRepairs {
			t.Fatalf("expected %d consecutive failed repairs, got %d", expectedRepairs, consecutiveFailedRepairs)
		} else if !roughlyEqual(nextRepairAttempt, expectedNextAttempt, time.Second) {
			t.Fatalf("expected next repair attempt %s, got %s", expectedNextAttempt, nextRepairAttempt)
		}
	}

	simulateFailedRepair := func() {
		t.Helper()
		if err = store.MarkSlabRepaired(slabIDs[0], false); err != nil {
			t.Fatal(err)
		}
	}

	simulateSuccessfulRepair := func() {
		t.Helper()
		if err = store.MarkSlabRepaired(slabIDs[0], true); err != nil {
			t.Fatal(err)
		}
	}

	// assert initial state
	assertSlabState(0, time.Now())

	// assert state after failed repair
	simulateFailedRepair()
	assertSlabState(1, time.Now().Add(minRepairBackoff))

	// assert backoff is capped at maxRepairBackoff (at 6 consec. failures we exceed it)
	for i := range 6 {
		simulateFailedRepair()
		if i < 4 {
			assertSlabState(i+2, time.Now().Add(minRepairBackoff*time.Duration(1<<(i+1))))
		} else {
			assertSlabState(i+2, time.Now().Add(maxRepairBackoff))
		}
	}

	// assert state after successful repair
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	_, err = store.pool.Exec(t.Context(), "UPDATE slabs SET next_repair_attempt = $1", oneHourAgo)
	if err != nil {
		t.Fatal(err)
	}
	simulateSuccessfulRepair()
	assertSlabState(0, oneHourAgo)
}

func TestPinnedSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	store.addTestAccount(t, types.PublicKey(account))

	// add hosts
	hosts := make([]types.PublicKey, 30)
	for i := range hosts {
		hosts[i] = store.addTestHost(t)
		store.addTestContract(t, hosts[i])
	}

	pinned := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors:       make([]slabs.PinnedSector, 0, len(hosts)),
	}
	for _, host := range hosts {
		pinned.Sectors = append(pinned.Sectors, slabs.PinnedSector{
			Root:    frand.Entropy256(),
			HostKey: host,
		})
	}
	expected := slabs.PinnedSlab{
		ID:            pinned.Digest(),
		EncryptionKey: pinned.EncryptionKey,
		MinShards:     pinned.MinShards,
		Sectors:       make([]slabs.PinnedSector, len(pinned.Sectors)),
	}
	for i, sector := range pinned.Sectors {
		expected.Sectors[i] = slabs.PinnedSector(sector)
	}

	slabIDs, err := store.PinSlabs(account, time.Time{}, pinned)
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	slab, err := store.PinnedSlab(account, slabID)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(slab, expected) {
		t.Fatalf("expected slab %v, got %v", expected, slab)
	}

	// mark some of the sectors as lost
	for i := range slab.Sectors[:10] {
		if err := store.MarkSectorsLost(slab.Sectors[i].HostKey, []types.Hash256{slab.Sectors[i].Root}); err != nil {
			t.Fatal(err)
		}
	}

	// assert the slab no longer contains the lost sectors
	slab, err = store.PinnedSlab(account, slabID)
	if err != nil {
		t.Fatal(err)
	}
	expected.Sectors = expected.Sectors[10:] // first 10 sectors are lost
	if !reflect.DeepEqual(slab, expected) {
		t.Fatalf("expected slab %v, got %v", expected, slab)
	}

	// mark the remaining sectors as lost
	for i := range slab.Sectors {
		if err := store.MarkSectorsLost(slab.Sectors[i].HostKey, []types.Hash256{slab.Sectors[i].Root}); err != nil {
			t.Fatal(err)
		}
	}

	_, err = store.PinnedSlab(account, slabID)
	if !errors.Is(err, slabs.ErrUnrecoverable) {
		t.Fatalf("expected ErrUnrecoverable, got %v", err)
	}
}

func TestPinnedSlabUnpinned(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc1, acc2 := proto.Account{1}, proto.Account{2}
	store.addTestAccount(t, types.PublicKey(acc1))
	store.addTestAccount(t, types.PublicKey(acc2))

	host := store.addTestHost(t)
	store.addTestContract(t, host)

	// both accounts pin the same slab
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors:       []slabs.PinnedSector{{Root: frand.Entropy256(), HostKey: host}},
	}
	slabID := store.pinTestSlabs(t, acc1, params)[0]
	store.pinTestSlabs(t, acc2, params)

	// both accounts can fetch it
	if _, err := store.PinnedSlab(acc1, slabID); err != nil {
		t.Fatal(err)
	} else if _, err := store.PinnedSlab(acc2, slabID); err != nil {
		t.Fatal(err)
	}

	// acc1 unpins the slab; the row is only queued for deletion, not removed yet
	if err := store.UnpinSlab(acc1, slabID); err != nil {
		t.Fatal(err)
	}

	// acc1 no longer pins the slab, so it must not be returned
	if _, err := store.PinnedSlab(acc1, slabID); !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatalf("expected ErrSlabNotFound for unpinned slab, got %v", err)
	}
	// acc2 still pins it, so it remains fetchable
	if _, err := store.PinnedSlab(acc2, slabID); err != nil {
		t.Fatalf("expected acc2 to still fetch the slab, got %v", err)
	}

	// a slab the account never pinned is also not found
	if _, err := store.PinnedSlab(proto.Account{3}, slabID); !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatalf("expected ErrSlabNotFound for unknown account, got %v", err)
	}
}

func TestSlabPruning(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin slab for both accounts
	slab1 := slabs.SlabPinParams{
		MinShards: 1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	for _, acc := range []proto.Account{acc1, acc2} {
		if _, err := store.PinSlabs(acc, time.Time{}, slab1); err != nil {
			t.Fatal(err)
		}
	}

	// pin second slab for first account
	slab2 := slabs.SlabPinParams{
		MinShards: 1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	if _, err := store.PinSlabs(acc1, time.Time{}, slab2); err != nil {
		t.Fatal(err)
	}

	// add objects for both accounts
	slab1ID := slab1.Digest()
	obj1 := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			slab1.Slice(10, 100),
			slab1.Slice(110, 200),
		},
		DataSignature:     types.Signature(frand.Bytes(64)),
		MetadataSignature: types.Signature(frand.Bytes(64)),
	}
	obj1Key := obj1.ID()
	for _, acc := range []proto.Account{acc1, acc2} {
		// note: unique key and signature are required per object. It does not change the object ID
		obj1.EncryptedDataKey = frand.Bytes(72)
		obj1.DataSignature = types.Signature(frand.Bytes(64))
		obj1.EncryptedMetadataKey = frand.Bytes(72)
		obj1.MetadataSignature = types.Signature(frand.Bytes(64))
		if err := store.PinObject(acc, obj1.PinRequest()); err != nil {
			t.Fatal(err)
		}
	}

	// pin this object to first account only
	slab2ID := slab2.Digest()
	obj2 := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			slab2.Slice(10, 100),
			slab2.Slice(110, 200),
		},
	}

	if err := store.PinObject(acc1, obj2.PinRequest()); err != nil {
		t.Fatal(err)
	}

	assertSlabs := func(acc proto.Account, expected ...slabs.SlabID) {
		t.Helper()

		got, err := store.SlabIDs(acc, 0, math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(expected, got) {
			t.Fatal("mismatched slab IDs")
		}
	}

	assertSlabs(acc1, slab2ID, slab1ID)
	assertSlabs(acc2, slab1ID)

	// delete object for acc1
	if err := store.DeleteObject(acc1, obj1Key); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID, slab1ID)
	assertSlabs(acc2, slab1ID)

	// prune slabs for acc1
	if err := store.PruneSlabs(acc1, time.Now()); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2, slab1ID)

	// delete object for acc2
	if err := store.DeleteObject(acc2, obj1Key); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2, slab1ID)

	// prune slabs for acc2
	if err := store.PruneSlabs(acc2, time.Now()); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2)
}

// pinTestSlabs pins params for account, failing the test on error.
func (s *Store) pinTestSlabs(t testing.TB, account proto.Account, params ...slabs.SlabPinParams) []slabs.SlabID {
	t.Helper()
	ids, err := s.PinSlabs(account, time.Time{}, params...)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// newTestSlab builds a slab on host hk with the given sectors, padded to two
// sectors with fresh unique roots.
func newTestSlab(hk types.PublicKey, sectors ...slabs.PinnedSector) slabs.SlabPinParams {
	for len(sectors) < 2 {
		sectors = append(sectors, slabs.PinnedSector{Root: frand.Entropy256(), HostKey: hk})
	}
	return slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors:       sectors,
	}
}

// newTestPinParams builds n slabs, each with two unique sectors, on host hk.
func newTestPinParams(n int, hk types.PublicKey) []slabs.SlabPinParams {
	params := make([]slabs.SlabPinParams, n)
	for i := range params {
		params[i] = newTestSlab(hk)
	}
	return params
}

// TestSlabPruningConcurrent runs two pruners in parallel that together remove
// the last reference to every slab and sector; nothing must be left behind.
func TestSlabPruningConcurrent(t *testing.T) {
	const (
		rounds = 5
		nSlabs = 50
	)
	cutoff := time.Now().Add(time.Hour) // all slabs eligible for pruning

	store := initPostgres(t, zap.NewNop())
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	acc1, acc2 := proto.Account{1}, proto.Account{2}
	store.addTestAccount(t, types.PublicKey(acc1))
	store.addTestAccount(t, types.PublicKey(acc2))

	// assertSlabsPruned asserts no slabs or sectors remain, the deletion queue is
	// empty and the stat counters have returned to zero.
	assertSlabsPruned := func(t testing.TB) {
		t.Helper()

		var slabCount, sectorCount, queued int64
		err := store.pool.QueryRow(t.Context(), `SELECT
			(SELECT COUNT(*) FROM slabs),
			(SELECT COUNT(*) FROM sectors),
			(SELECT COUNT(*) FROM slab_deletion_queue)`).Scan(&slabCount, &sectorCount, &queued)
		if err != nil {
			t.Fatal(err)
		}
		if slabCount != 0 || sectorCount != 0 || queued != 0 {
			t.Fatalf("after pruning: %d slabs, %d sectors, %d queued for deletion", slabCount, sectorCount, queued)
		}

		stats, err := store.SectorStats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.Slabs != 0 || stats.Pinned != 0 || stats.Unpinned != 0 || stats.Unpinnable != 0 {
			t.Fatalf("stats not zeroed after pruning: slabs=%d pinned=%d unpinned=%d unpinnable=%d", stats.Slabs, stats.Pinned, stats.Unpinned, stats.Unpinnable)
		}
	}

	pruneSlabs := func(account proto.Account) func() error {
		return func() error { return store.PruneSlabs(account, cutoff) }
	}

	// each case pins a round's worth of slabs and returns the prune funcs to run
	// concurrently
	tests := []struct {
		name    string
		prepare func(t testing.TB) []func() error
	}{
		{
			// both accounts pin the same slabs, so the last reference to each slab
			// is removed concurrently
			name: "shared slabs",
			prepare: func(t testing.TB) []func() error {
				params := newTestPinParams(nSlabs, hk)
				store.pinTestSlabs(t, acc1, params...)
				store.pinTestSlabs(t, acc2, params...)
				return []func() error{pruneSlabs(acc1), pruneSlabs(acc2)}
			},
		},
		{
			// each account pins its own slabs that share a sector, so the last slab
			// referencing each sector is removed concurrently
			name: "shared sectors",
			prepare: func(t testing.TB) []func() error {
				p1 := make([]slabs.SlabPinParams, nSlabs)
				p2 := make([]slabs.SlabPinParams, nSlabs)
				for i := range p1 {
					shared := slabs.PinnedSector{Root: frand.Entropy256(), HostKey: hk}
					p1[i] = newTestSlab(hk, shared)
					p2[i] = newTestSlab(hk, shared)
				}
				store.pinTestSlabs(t, acc1, p1...)
				store.pinTestSlabs(t, acc2, p2...)
				return []func() error{pruneSlabs(acc1), pruneSlabs(acc2)}
			},
		},
		{
			// a soft-deleted account is pruned (same staging path) while acc2 prunes
			name: "account pruning",
			prepare: func(t testing.TB) []func() error {
				del := proto.Account(types.GeneratePrivateKey().PublicKey())
				store.addTestAccount(t, types.PublicKey(del))
				params := newTestPinParams(nSlabs, hk)
				store.pinTestSlabs(t, del, params...)
				store.pinTestSlabs(t, acc2, params...)
				if err := store.DeleteAccount(del); err != nil {
					t.Fatal(err)
				}
				return []func() error{
					func() error {
						for {
							if err := store.PruneAccounts(2 * nSlabs); errors.Is(err, accounts.ErrNotFound) {
								return nil
							} else if err != nil {
								return err
							}
						}
					},
					pruneSlabs(acc2),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range rounds {
				store.runConcurrentPrune(t, 2*nSlabs, tt.prepare(t)...)
				assertSlabsPruned(t)
			}
		})
	}
}

// TestSlabRepinDuringPrune re-pins slabs while acc1 is being pruned. acc2 ends
// up pinning every slab, so the locks in deleteOrphanedSlabs must keep all slabs
// and sectors alive.
func TestSlabRepinDuringPrune(t *testing.T) {
	const (
		rounds         = 30
		numSlabs       = 100
		sectorsPerSlab = 2
		repinBatch     = 5 // small batches widen the interleaving window
	)
	cutoff := time.Now().Add(time.Hour) // all slabs eligible for pruning

	store := initPostgres(t, zap.NewNop())
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	acc1, acc2 := proto.Account{1}, proto.Account{2}
	store.addTestAccount(t, types.PublicKey(acc1))
	store.addTestAccount(t, types.PublicKey(acc2))

	// assertSlabsIntact asserts exactly wantSlabs slabs remain, each with
	// sectorsPerSlab sectors and no orphaned sectors.
	assertSlabsIntact := func(t testing.TB, wantSlabs, sectorsPerSlab int) {
		t.Helper()

		var slabCount, shortSlabs, orphanedSectors int64
		err := store.pool.QueryRow(t.Context(), `SELECT
			(SELECT COUNT(*) FROM slabs),
			(SELECT COUNT(*) FROM slabs s WHERE (SELECT COUNT(*) FROM slab_sectors ss WHERE ss.slab_id = s.id) <> $1),
			(SELECT COUNT(*) FROM sectors se WHERE NOT EXISTS (SELECT 1 FROM slab_sectors ss WHERE ss.sector_id = se.id))`,
			sectorsPerSlab).Scan(&slabCount, &shortSlabs, &orphanedSectors)
		if err != nil {
			t.Fatal(err)
		}
		if slabCount != int64(wantSlabs) || shortSlabs != 0 || orphanedSectors != 0 {
			t.Fatalf("want %d intact slabs with %d sectors each; got slabs=%d slabsMissingSectors=%d orphanedSectors=%d",
				wantSlabs, sectorsPerSlab, slabCount, shortSlabs, orphanedSectors)
		}
	}

	// each case returns acc1's slabs and the slabs acc2 re-pins concurrently
	tests := []struct {
		name   string
		params func() (p1, p2 []slabs.SlabPinParams)
	}{
		{
			// acc2 re-pins the exact same slabs; the slabs lock serializes the
			// prune against the re-pin
			name: "same digest",
			params: func() (p1, p2 []slabs.SlabPinParams) {
				p1 = newTestPinParams(numSlabs, hk)
				return p1, p1
			},
		},
		{
			// acc2 pins different-digest slabs that reuse acc1's sector roots; only
			// the sectors lock keeps the shared sectors alive
			name: "shared sector",
			params: func() (p1, p2 []slabs.SlabPinParams) {
				p1 = make([]slabs.SlabPinParams, numSlabs)
				p2 = make([]slabs.SlabPinParams, numSlabs)
				for i := range p1 {
					shared := slabs.PinnedSector{Root: frand.Entropy256(), HostKey: hk}
					p1[i] = newTestSlab(hk, shared)
					p2[i] = newTestSlab(hk, shared)
				}
				return p1, p2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range rounds {
				p1, p2 := tt.params()
				store.pinTestSlabs(t, acc1, p1...)

				store.runConcurrentPrune(t, 2*numSlabs,
					func() error { return store.PruneSlabs(acc1, cutoff) },
					func() error {
						// re-pin in small batches so the prune loop interleaves
						// with the inserts
						for i := 0; i < len(p2); i += repinBatch {
							if _, err := store.PinSlabs(acc2, time.Time{}, p2[i:min(i+repinBatch, len(p2))]...); err != nil {
								return err
							}
						}
						return nil
					},
				)

				assertSlabsIntact(t, numSlabs, sectorsPerSlab)

				// clean up for the next round
				if err := store.PruneSlabs(acc2, cutoff); err != nil {
					t.Fatal(err)
				}
				store.pruneAllDeletedSlabs(t)
			}
		})
	}
}

func BenchmarkPruneSlabs(b *testing.B) {
	const (
		numAccounts       = 1000
		objectsPerAccount = 500
		slabsPerObject    = 3
	)

	store := initPostgres(b, zap.NewNop())

	connectKey, err := store.AddAppConnectKey(accounts.AppConnectKeyRequest{
		Key:         "benchmark-prune-slabs-key",
		Description: "benchmark connect key",
		Quota:       "default",
	})
	if err != nil {
		b.Fatal(err)
	}

	var connectKeyID int64
	if err := store.pool.QueryRow(b.Context(), `SELECT id FROM app_connect_keys WHERE app_key = $1`, connectKey.Key).Scan(&connectKeyID); err != nil {
		b.Fatal(err)
	}

	batch := &pgx.Batch{}
	var accs []proto.Account
	var slabID, objectID int64
	pinnedPerAccount := int64(objectsPerAccount*slabsPerObject) * int64(proto.SectorSize)
	for i := range numAccounts {
		pk := types.GeneratePrivateKey().PublicKey()
		accs = append(accs, proto.Account(pk))

		batch.Queue(`INSERT INTO accounts(public_key, connect_key_id, max_pinned_data, pinned_data) VALUES ($1, $2, 1000000, $3);`, sqlPublicKey(pk), connectKeyID, pinnedPerAccount)
		for j := range objectsPerAccount {
			accountID := i + 1

			var encryptionKey [32]byte
			frand.Read(encryptionKey[:])

			objectKey := sqlHash256(frand.Entropy256())
			if j%2 == 0 {
				objectID++
				batch.Queue(`INSERT INTO objects(object_key, account_id, encrypted_data_key, encrypted_meta_key, data_signature, meta_signature) VALUES ($1, $2, $3, $4, $5, $6)`, objectKey, accountID, frand.Bytes(72), frand.Bytes(72), frand.Bytes(64), frand.Bytes(64))
			}
			for k := range slabsPerObject {
				slabID++
				slabDigest := sqlHash256(frand.Entropy256())

				batch.Queue(`INSERT INTO slabs(digest, encryption_key, min_shards) VALUES ($1, $2, 1);`, slabDigest, sqlHash256(encryptionKey))
				batch.Queue(`INSERT INTO account_slabs(account_id, slab_id) VALUES ($1, $2)`, accountID, slabID)
				if j%2 == 0 {
					batch.Queue(`INSERT INTO object_slabs(object_id, slab_digest, slab_index, slab_offset, slab_length) VALUES ($1, $2, $3, 0, 0)`, objectID, slabDigest, k)
				}
			}
		}
	}
	batch.Queue(`UPDATE app_connect_keys ack SET pinned_data = (
		SELECT COALESCE(SUM(a.pinned_data), 0)
		FROM accounts a
		WHERE a.connect_key_id = ack.id
	)`)
	batch.Queue(`UPDATE stats SET stat_value = $1 WHERE stat_name = $2`, slabID, statSlabs)
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		b.ReportMetric(float64(objectsPerAccount)*float64(slabsPerObject)/2.0, "slabs/op")

		if err := store.PruneSlabs(accs[frand.Intn(len(accs))], time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}

// pruneAllDeletedSlabs runs PruneDeletedSlabs until none remain, so tests can
// force the queued slabs and their orphaned sectors to actually be removed.
func (s *Store) pruneAllDeletedSlabs(t testing.TB) {
	t.Helper()
	for {
		n, err := s.PruneDeletedSlabs(100)
		if err != nil {
			t.Fatal(err)
		} else if n == 0 {
			break
		}
	}
}

// runConcurrentPrune runs fns concurrently while a background goroutine prunes
// the deletion queue, then stops the pruner and does a final synchronous prune.
func (s *Store) runConcurrentPrune(t testing.TB, pruneBatch int, fns ...func() error) {
	t.Helper()

	errCh := make(chan error, len(fns)+1)
	start := make(chan struct{})
	stopPrune := make(chan struct{})

	var pruneWG sync.WaitGroup
	pruneWG.Go(func() {
		<-start
		for {
			select {
			case <-stopPrune:
				return
			default:
			}
			if _, err := s.PruneDeletedSlabs(pruneBatch); err != nil {
				errCh <- err
				return
			}
		}
	})

	var wg sync.WaitGroup
	for _, fn := range fns {
		wg.Go(func() {
			<-start
			if err := fn(); err != nil {
				errCh <- err
			}
		})
	}
	close(start)

	wg.Wait()
	close(stopPrune)
	pruneWG.Wait()

	// clear anything queued after the pruner last looped
	s.pruneAllDeletedSlabs(t)

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
