package postgres

import (
	"context"
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
	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, params)
	if err != nil {
		t.Fatal(err)
	}

	// fetch slab
	got, err := store.Slab(context.Background(), slabIDs[0])
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
	if err := store.AddFormedContract(context.Background(), hk, fcid, newTestRevision(hk), types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(context.Background(), fcid, []types.Hash256{params.Sectors[0].Root}); err != nil {
		t.Fatal(err)
	}

	// fetch slab again
	got, err = store.Slab(context.Background(), slabIDs[0])
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches the expected slab with the pinned sector
	expected.Sectors[0].ContractID = &fcid
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected slab %v, got %v", expected, got)
	}
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

	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, pinned)
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	slab, err := store.PinnedSlab(context.Background(), account, slabID)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(slab, expected) {
		t.Fatalf("expected slab %v, got %v", expected, slab)
	}

	// mark some of the sectors as lost
	for i := range slab.Sectors[:10] {
		if err := store.MarkSectorsLost(context.Background(), slab.Sectors[i].HostKey, []types.Hash256{slab.Sectors[i].Root}); err != nil {
			t.Fatal(err)
		}
	}

	// assert the slab no longer contains the lost sectors
	slab, err = store.PinnedSlab(context.Background(), account, slabID)
	if err != nil {
		t.Fatal(err)
	}
	expected.Sectors = expected.Sectors[10:] // first 10 sectors are lost
	if !reflect.DeepEqual(slab, expected) {
		t.Fatalf("expected slab %v, got %v", expected, slab)
	}

	// mark the remaining sectors as lost
	for i := range slab.Sectors {
		if err := store.MarkSectorsLost(context.Background(), slab.Sectors[i].HostKey, []types.Hash256{slab.Sectors[i].Root}); err != nil {
			t.Fatal(err)
		}
	}

	_, err = store.PinnedSlab(context.Background(), account, slabID)
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

	// pin slab for both accounts
	slab1 := slabs.SlabPinParams{MinShards: 1}
	for _, acc := range []proto.Account{acc1, acc2} {
		if _, err := store.PinSlabs(context.Background(), acc, time.Time{}, slab1); err != nil {
			t.Fatal(err)
		}
	}

	// pin second slab for first account
	slab2 := slabs.SlabPinParams{MinShards: 2}
	if _, err := store.PinSlabs(context.Background(), acc1, time.Time{}, slab2); err != nil {
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
		if err := store.SaveObject(context.Background(), acc, obj1); err != nil {
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

	if err := store.SaveObject(context.Background(), acc1, obj2); err != nil {
		t.Fatal(err)
	}

	assertSlabs := func(acc proto.Account, expected ...slabs.SlabID) {
		t.Helper()

		got, err := store.SlabIDs(context.Background(), acc, 0, math.MaxInt64)
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
	if err := store.DeleteObject(context.Background(), acc1, obj1Key); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID, slab1ID)
	assertSlabs(acc2, slab1ID)

	// prune slabs for acc1
	if err := store.PruneSlabs(context.Background(), acc1); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2, slab1ID)

	// delete object for acc2
	if err := store.DeleteObject(context.Background(), acc2, obj1Key); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2, slab1ID)

	// prune slabs for acc2
	if err := store.PruneSlabs(context.Background(), acc2); err != nil {
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

		if err := store.PruneSlabs(b.Context(), accs[frand.Intn(len(accs))]); err != nil {
			b.Fatal(err)
		}
	}
}
