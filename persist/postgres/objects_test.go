package postgres

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func (s *Store) pinRandomObject(t testing.TB, acc proto.Account, ss []slabs.SlabSlice) slabs.SealedObject {
	obj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs:                ss,
		EncryptedMetadata:    []byte("hello world"),
		DataSignature:        (types.Signature)(frand.Bytes(64)),
		MetadataSignature:    (types.Signature)(frand.Bytes(64)),
	}
	if err := s.SaveObject(acc, obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

func TestObject(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	acc := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	fcid := store.addTestContract(t, hk)

	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
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
			{
				Root:    frand.Entropy256(),
				HostKey: hk,
			},
		},
	}

	_, err := store.PinSlabs(acc, time.Time{}, params)
	if err != nil {
		t.Fatal(err)
	}

	// pin sector 1, keep sector 2 the way it is and mark sector 3 as lost
	if err := store.PinSectors(fcid, []types.Hash256{params.Sectors[0].Root}); err != nil {
		t.Fatal(err)
	} else if err := store.MarkSectorsLost(hk, []types.Hash256{params.Sectors[2].Root}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Round(time.Second)
	expected := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		EncryptedMetadata:    frand.Bytes(50),
		DataSignature:        types.Signature(frand.Bytes(64)),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
		CreatedAt:            now,
		UpdatedAt:            now,
		Slabs: []slabs.SlabSlice{
			params.Slice(0, 100),
		},
	}
	err = store.SaveObject(acc, expected)
	if err != nil {
		t.Fatal(err)
	}

	expected.CreatedAt = time.Time{}
	expected.UpdatedAt = time.Time{}
	expected.Slabs[0].Sectors[2].HostKey = types.PublicKey{}

	got, err := store.Object(acc, expected.ID())
	if err != nil {
		t.Fatal(err)
	} else if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatalf("expected non-zero timestamps, got %v and %v", got.CreatedAt, got.UpdatedAt)
	}

	got.CreatedAt = time.Time{}
	got.UpdatedAt = time.Time{}
	if !reflect.DeepEqual(expected, got) {
		t.Fatal("objects not equal", expected, got)
	}

	expectedShared := slabs.SharedObject{
		Slabs: expected.Slabs,
	}
	gotShared, err := store.SharedObject(expected.ID())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(expectedShared, gotShared) {
		t.Fatal("shared objects not equal", expectedShared, gotShared)
	}
}

func TestObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin slab for both accounts
	slab := slabs.SlabPinParams{
		MinShards: 1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	for _, acc := range []proto.Account{acc1, acc2} {
		_, err := store.PinSlabs(acc, time.Time{}, slab)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertObjects := func(acc proto.Account, expectedDeleted, expectedExist int) []slabs.ObjectEvent {
		t.Helper()
		objects, err := store.ListObjects(acc, slabs.Cursor{}, 10)
		if err != nil {
			t.Fatal(err)
		}

		var exist, deleted int
		for _, obj := range objects {
			if obj.Deleted {
				deleted++
			} else {
				exist++
			}
		}
		if expectedExist != exist {
			t.Fatalf("expected %d objects to exist, got %d", expectedExist, exist)
		}
		if expectedDeleted != deleted {
			t.Fatalf("expected %d objects to be deleted, got %d", expectedDeleted, deleted)
		}

		return objects
	}

	// no objects should exist
	assertObjects(acc1, 0, 0)
	assertObjects(acc2, 0, 0)

	// add objects for both accounts
	randomSlabs := func(n int) []slabs.SlabPinParams {
		s := make([]slabs.SlabPinParams, n)
		for i := range s {
			s[i] = slabs.SlabPinParams{
				EncryptionKey: frand.Entropy256(),
				MinShards:     1,
			}
			for range 10 {
				s[i].Sectors = append(s[i].Sectors, slabs.PinnedSector{
					Root:    types.Hash256(frand.Entropy256()),
					HostKey: hk,
				})
			}
		}
		return s
	}

	pinSlabs := func(acc proto.Account, params []slabs.SlabPinParams) []slabs.SlabSlice {
		t.Helper()

		var ss []slabs.SlabSlice
		for _, p := range params {
			_, err := store.PinSlabs(acc, time.Time{}, p)
			if err != nil {
				t.Fatal(err)
			}
			ss = append(ss, p.Slice(10, 120))
		}
		return ss
	}

	obj1Slabs := randomSlabs(3)
	pinSlabs(acc1, obj1Slabs)
	pinSlabs(acc2, obj1Slabs)
	obj1Acc1 := store.pinRandomObject(t, acc1, pinSlabs(acc1, obj1Slabs))

	// pin the same object for acc2 with different master key and sig to satisfy unique constraint
	obj1Acc2 := obj1Acc1
	obj1Acc2.EncryptedDataKey = frand.Bytes(72)
	obj1Acc2.DataSignature = (types.Signature)(frand.Bytes(64))
	obj1Acc2.EncryptedMetadataKey = frand.Bytes(72)
	obj1Acc2.MetadataSignature = (types.Signature)(frand.Bytes(64))
	if err := store.SaveObject(acc2, obj1Acc2); err != nil {
		t.Fatal(err)
	}

	if obj1Acc1.ID() != obj1Acc2.ID() {
		t.Fatal("expected object IDs to match")
	}

	assertObj := func(obj slabs.SealedObject, other slabs.ObjectEvent) {
		t.Helper()
		if other.Deleted {
			t.Fatal("object was unexpectedly deleted")
		}

		otherObj := *other.Object
		if otherObj.CreatedAt.IsZero() || otherObj.UpdatedAt.IsZero() {
			t.Fatalf("expected non-zero timestamps, got %v and %v", otherObj.CreatedAt, otherObj.UpdatedAt)
		}
		otherObj.CreatedAt = time.Time{}
		otherObj.UpdatedAt = time.Time{}
		if !reflect.DeepEqual(obj, otherObj) {
			t.Fatalf("objects not equal: expected %+v, got %+v", obj, otherObj)
		}
	}

	// 1 object should exist for both accounts
	objs := assertObjects(acc1, 0, 1)
	assertObj(obj1Acc1, objs[0])

	objs = assertObjects(acc2, 0, 1)
	assertObj(obj1Acc2, objs[0])

	// delete object for acc1
	if err := store.DeleteObject(acc1, obj1Acc1.ID()); err != nil {
		t.Fatal(err)
	}

	// no object should exist for acc1 (1 deleted), 1 for acc2
	assertObjects(acc1, 1, 0)

	objs = assertObjects(acc2, 0, 1)
	assertObj(obj1Acc2, objs[0])

	// add another object to acc2
	obj2 := store.pinRandomObject(t, acc2, pinSlabs(acc2, randomSlabs(2)))

	// listing the objects should return obj1 first since it was updated first
	assertObjects(acc1, 1, 0)
	objs = assertObjects(acc2, 0, 2)
	assertObj(obj1Acc2, objs[0])
	assertObj(obj2, objs[1])

	// save object 1 again to update its timestamp
	obj1Acc2.EncryptedMetadata = []byte("updated meta")
	if err := store.SaveObject(acc2, obj1Acc2); err != nil {
		t.Fatal(err)
	}

	// the order should be reversed now
	assertObjects(acc1, 1, 0)
	objs = assertObjects(acc2, 0, 2)
	assertObj(obj2, objs[0])
	assertObj(obj1Acc2, objs[1])

	// make sure the limit works
	objs, err := store.ListObjects(acc2, slabs.Cursor{}, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 {
		t.Fatalf("expected 1 objects, got %d", len(objs))
	}

	// increasing 'after' to now should not yield any results
	objs, err = store.ListObjects(acc2, slabs.Cursor{After: time.Now()}, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	// assert we can fetch a single object
	obj, err := store.Object(acc2, obj2.ID())
	if err != nil {
		t.Fatal(err)
	} else if obj.CreatedAt.IsZero() || obj.UpdatedAt.IsZero() {
		t.Fatalf("expected non-zero timestamps, got %v and %v", obj.CreatedAt, obj.UpdatedAt)
	}
	obj.CreatedAt = time.Time{}
	obj.UpdatedAt = time.Time{}
	if !reflect.DeepEqual(obj2, obj) {
		t.Fatalf("expected object %+v, got %+v", obj2, obj)
	}

	// assert account is taken into consideration when fetching an object
	_, err = store.Object(acc1, obj2.ID())
	if !errors.Is(err, slabs.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	// assert fetching a non-existent object returns the correct error
	_, err = store.Object(acc2, frand.Entropy256())
	if !errors.Is(err, slabs.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	// assert listing objects for accounts that include a deleted object works
	store.pinRandomObject(t, acc1, pinSlabs(acc1, randomSlabs(3)))
	assertObjects(acc1, 1, 1)
}

// TestListObjectsRegression is a small regression tests that asserts proper
// handling of cursor.key which was not casted as a sqlHash256 at one point.
func TestListObjectsRegression(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create account
	acc := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(acc))

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	randomSlabs := func() []slabs.SlabSlice {
		slab := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []slabs.PinnedSector{{
				Root:    frand.Entropy256(),
				HostKey: hk,
			}},
		}
		_, err := store.PinSlabs(acc, time.Time{}, slab)
		if err != nil {
			t.Fatal(err)
		}
		return []slabs.SlabSlice{
			slab.Slice(10, 100),
			slab.Slice(110, 200),
		}
	}

	// add multiple objects
	var objectIDs []types.Hash256
	for range 3 {
		obj := store.pinRandomObject(t, acc, randomSlabs())
		objectIDs = append(objectIDs, obj.ID())
	}
	// list objects returns objects in updated_at ASC then lexicographical order of ID
	sort.Slice(objectIDs, func(i, j int) bool {
		return bytes.Compare(objectIDs[i][:], objectIDs[j][:]) < 0
	})

	ts := time.Now().Round(time.Second)
	_, err := store.pool.Exec(t.Context(), "UPDATE objects SET updated_at = $1", ts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.pool.Exec(t.Context(), "UPDATE object_events SET updated_at = $1", ts)
	if err != nil {
		t.Fatal(err)
	}

	objs, err := store.ListObjects(acc, slabs.Cursor{After: ts}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != len(objectIDs) {
		t.Fatal("expected 3 objects, got", len(objs))
	}
	for i, obj := range objs {
		if obj.Object.ID() != objectIDs[i] {
			t.Fatalf("expected object ID %v, got %v", objectIDs[i], obj.Object.ID())
		}
	}
}

func TestSaveObject(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create account
	acc := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(acc))

	// add host and contract
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin a slab
	slab := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	if _, err := store.PinSlabs(acc, time.Time{}, slab); err != nil {
		t.Fatal(err)
	}

	// assert saving an object with metadata
	objWithMeta := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		EncryptedMetadata:    frand.Bytes(100),
		Slabs:                []slabs.SlabSlice{slab.Slice(0, 100)},
		DataSignature:        types.Signature(frand.Bytes(64)),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
	}

	if err := store.SaveObject(acc, objWithMeta); err != nil {
		t.Fatalf("failed to save object with metadata: %v", err)
	}

	// assert we can retrieve it correctly
	got, err := store.Object(acc, objWithMeta.ID())
	if err != nil {
		t.Fatalf("failed to get object: %v", err)
	} else if len(got.EncryptedMetadataKey) != 72 {
		t.Fatalf("unexpected key length, got %d bytes", len(got.EncryptedMetadataKey))
	} else if !bytes.Equal(got.EncryptedMetadataKey, objWithMeta.EncryptedMetadataKey) {
		t.Fatal("unexpected key")
	} else if !bytes.Equal(got.EncryptedMetadata, objWithMeta.EncryptedMetadata) {
		t.Fatal("unexpected metadata")
	}

	// assert saving an object without metadata
	objNoMeta := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: nil,
		EncryptedMetadata:    nil,
		Slabs:                []slabs.SlabSlice{slab.Slice(100, 100)},
		DataSignature:        types.Signature(frand.Bytes(64)),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
	}

	if err := store.SaveObject(acc, objNoMeta); err != nil {
		t.Fatalf("failed to save object without metadata: %v", err)
	}

	got, err = store.Object(acc, objNoMeta.ID())
	if err != nil {
		t.Fatalf("failed to get object: %v", err)
	} else if len(got.EncryptedMetadataKey) != 0 {
		t.Fatalf("unexpected key length, got %d bytes", len(got.EncryptedMetadataKey))
	} else if len(got.EncryptedMetadata) != 0 {
		t.Fatalf("unexpected metadata length, got %d bytes", len(got.EncryptedMetadata))
	}

	// assert saving an object with empty metadata slice
	objEmptyMeta := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: []byte{},
		EncryptedMetadata:    []byte{},
		Slabs:                []slabs.SlabSlice{slab.Slice(200, 100)},
		DataSignature:        types.Signature(frand.Bytes(64)),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
	}

	if err := store.SaveObject(acc, objEmptyMeta); err != nil {
		t.Fatalf("failed to save object with empty metadata slice: %v", err)
	}

	got, err = store.Object(acc, objEmptyMeta.ID())
	if err != nil {
		t.Fatalf("failed to get object: %v", err)
	} else if len(got.EncryptedMetadataKey) != 0 {
		t.Fatalf("unexpected key length, got %d bytes", len(got.EncryptedMetadataKey))
	} else if len(got.EncryptedMetadata) != 0 {
		t.Fatalf("unexpected metadata length, got %d bytes", len(got.EncryptedMetadata))
	}
}

func TestSharedObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hostKeys := make([]types.PublicKey, 30)
	for i := range hostKeys {
		hostKeys[i] = store.addTestHost(t)
		store.addTestContract(t, hostKeys[i])
	}

	pinRandomSlab := func(t *testing.T) slabs.SlabSlice {
		t.Helper()

		s := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			Sectors:       make([]slabs.PinnedSector, 30),
		}
		for i := range s.Sectors {
			s.Sectors[i].HostKey = hostKeys[i%len(hostKeys)]
			s.Sectors[i].Root = frand.Entropy256()
		}
		s.MinShards = uint(len(s.Sectors))

		slabIDs, err := store.PinSlabs(acc1, time.Time{}, s)
		if err != nil {
			t.Fatal(err)
		} else if id := s.Digest(); id != slabIDs[0] {
			t.Fatalf("expected slab ID %v, got %v", id, slabIDs[0])
		}

		return s.Slice(uint32(frand.Uint64n(math.MaxInt32)), uint32(frand.Uint64n(math.MaxInt32)))
	}

	// add an object with multiple slabs
	expectedSharedObj := slabs.SharedObject{
		Slabs: []slabs.SlabSlice{pinRandomSlab(t), pinRandomSlab(t), pinRandomSlab(t)},
	}
	obj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs:                make([]slabs.SlabSlice, len(expectedSharedObj.Slabs)),
		EncryptedMetadata:    []byte("hello world"),
	}
	obj.Slabs = slices.Clone(expectedSharedObj.Slabs)
	if err := store.SaveObject(acc1, obj); err != nil {
		t.Fatal(err)
	}

	sharedObj, err := store.SharedObject(obj.ID())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(expectedSharedObj, sharedObj) {
		t.Fatalf("shared objects not equal: expected %+v, got %+v", expectedSharedObj, sharedObj)
	}

	// pin the slabs to the second account
	for _, slab := range expectedSharedObj.Slabs {
		_, err := store.PinSlabs(acc2, time.Time{}, slabs.SlabPinParams{
			MinShards:     slab.MinShards,
			Sectors:       slab.Sectors,
			EncryptionKey: slab.EncryptionKey,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkSaveObject(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(b, types.PublicKey(acc))
	}

	hostKeys := make([]types.PublicKey, 30)
	for i := range hostKeys {
		hostKeys[i] = store.addTestHost(b)
		store.addTestContract(b, hostKeys[i])
	}

	var objs []slabs.SealedObject
	pinObject := func(b *testing.B) (obj slabs.SealedObject) {
		b.Helper()

		s := slabs.SlabPinParams{
			MinShards:     uint(frand.Intn(10)) + 1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       make([]slabs.PinnedSector, 30),
		}
		for i := range s.Sectors {
			s.Sectors[i].HostKey = hostKeys[i%len(hostKeys)]
			s.Sectors[i].Root = frand.Entropy256()
		}

		slabIDs, err := store.PinSlabs(acc1, time.Time{}, s)
		if err != nil {
			b.Fatal(err)
		}
		slabID := slabIDs[0]

		if id := s.Digest(); id != slabID {
			b.Fatalf("expected slab ID %v, got %v", id, slabID)
		}

		obj.Slabs = append(obj.Slabs, s.Slice(0, 256))
		for i := 0; i < 20 && i < len(objs); i++ {
			slab := objs[i].Slabs[0]
			slab.Offset, slab.Length = 0, 256
			obj.Slabs = append(obj.Slabs, slab)
		}
		obj.EncryptedMetadata = frand.Bytes(1024)
		obj.EncryptedDataKey = frand.Bytes(72)
		obj.DataSignature = types.Signature(frand.Bytes(64))
		obj.EncryptedMetadataKey = frand.Bytes(72)
		obj.MetadataSignature = types.Signature(frand.Bytes(64))

		return
	}

	for range 10000 {
		obj := pinObject(b)
		if err := store.SaveObject(acc1, obj); err != nil {
			b.Fatal(err)
		}
		objs = append(objs, obj)
	}

	obj := pinObject(b)
	for b.Loop() {
		if err := store.SaveObject(acc1, obj); err != nil {
			b.Fatal(err)
		}
	}
}
