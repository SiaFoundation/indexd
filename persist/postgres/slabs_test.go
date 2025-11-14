package postgres

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
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
	expectedID, err := params.Digest()
	if err != nil {
		t.Fatal(err)
	}
	expected := slabs.Slab{
		ID:            expectedID,
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
	digest, err := pinned.Digest()
	if err != nil {
		t.Fatal(err)
	}
	expected := slabs.PinnedSlab{
		ID:            digest,
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
	slab1ID, _ := slab1.Digest()
	obj1 := slabs.SealedObject{
		EncryptedMasterKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slab1ID,
				Offset: 10,
				Length: 100,
			},
			{
				SlabID: slab1ID,
				Offset: 110,
				Length: 200,
			},
		},
		Signature: types.Signature(frand.Bytes(64)),
	}
	obj1Key := obj1.ID()
	for _, acc := range []proto.Account{acc1, acc2} {
		// note: unique key and signature are required per object. It does not change the object ID
		obj1.EncryptedMasterKey = frand.Bytes(72)
		obj1.Signature = types.Signature(frand.Bytes(64))
		if err := store.SaveObject(acc, obj1); err != nil {
			t.Fatal(err)
		}
	}

	// pin this object to first account only
	slab2ID, _ := slab2.Digest()
	obj2 := slabs.SealedObject{
		EncryptedMasterKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slab2ID,
				Offset: 10,
				Length: 100,
			},
			{
				SlabID: slab2ID,
				Offset: 110,
				Length: 200,
			},
		},
	}

	if err := store.SaveObject(acc1, obj2); err != nil {
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
	if err := store.PruneSlabs(acc1); err != nil {
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
	if err := store.PruneSlabs(acc2); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2)
}

func BenchmarkPruneSlabs(b *testing.B) {
	const (
		numAccounts       = 1000
		objectsPerAccount = 500
		slabsPerObject    = 3
	)

	store := initPostgres(b, zap.NewNop())

	batch := &pgx.Batch{}
	var accs []proto.Account
	var slabID, objectID int64
	for i := range numAccounts {
		pk := types.GeneratePrivateKey().PublicKey()
		accs = append(accs, proto.Account(pk))

		batch.Queue(`INSERT INTO accounts(public_key, max_pinned_data) VALUES ($1, 1000000);`, sqlPublicKey(pk))
		for j := range objectsPerAccount {
			accountID := i + 1

			var encryptionKey [32]byte
			frand.Read(encryptionKey[:])

			objectKey := sqlHash256(frand.Entropy256())
			if j%2 == 0 {
				objectID++
				batch.Queue(`INSERT INTO objects(object_key, account_id) VALUES ($1, $2)`, objectKey, accountID)
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
	batch.Queue(`UPDATE stats SET num_slabs = $1`, slabID)
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		b.ReportMetric(float64(objectsPerAccount)*float64(slabsPerObject)/2.0, "slabs/op")

		if err := store.PruneSlabs(accs[frand.Intn(len(accs))]); err != nil {
			b.Fatal(err)
		}
	}
}
