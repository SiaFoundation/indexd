package postgres

import (
	"bytes"
	"errors"
	"fmt"
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
	if err := s.PinObject(acc, obj.PinRequest()); err != nil {
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

	expected := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		EncryptedMetadata:    frand.Bytes(50),
		DataSignature:        types.Signature(frand.Bytes(64)),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
		// note: created at and updated at are set by the server
		Slabs: []slabs.SlabSlice{
			params.Slice(0, 100),
		},
	}
	err = store.PinObject(acc, expected.PinRequest())
	if err != nil {
		t.Fatal(err)
	}

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

func TestObjectSlabVersion(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	acc := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	fcid := store.addTestContract(t, hk)

	params := slabs.SlabPinParams{
		Version:       1,
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: frand.Entropy256(), HostKey: hk},
		},
	}
	if _, err := store.PinSlabs(acc, time.Time{}, params); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(fcid, []types.Hash256{params.Sectors[0].Root}); err != nil {
		t.Fatal(err)
	}

	obj := slabs.SealedObject{
		EncryptedDataKey:  frand.Bytes(72),
		DataSignature:     types.Signature(frand.Bytes(64)),
		MetadataSignature: types.Signature(frand.Bytes(64)),
		Slabs:             []slabs.SlabSlice{params.Slice(0, 100)},
	}
	if obj.Slabs[0].Version != 1 {
		t.Fatalf("expected slice version 1 before save, got %d", obj.Slabs[0].Version)
	}
	if err := store.PinObject(acc, obj.PinRequest()); err != nil {
		t.Fatal(err)
	}

	got, err := store.Object(acc, obj.ID())
	if err != nil {
		t.Fatal(err)
	} else if got.Slabs[0].Version != 1 {
		t.Fatalf("Object: expected reloaded slice version 1, got %d", got.Slabs[0].Version)
	}

	shared, err := store.SharedObject(obj.ID())
	if err != nil {
		t.Fatal(err)
	} else if shared.Slabs[0].Version != 1 {
		t.Fatalf("SharedObject: expected reloaded slice version 1, got %d", shared.Slabs[0].Version)
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
	if err := store.PinObject(acc2, obj1Acc2.PinRequest()); err != nil {
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

	// add another object to acc2; sleep past the next second boundary since
	// updated_at has second precision
	time.Sleep(time.Second)
	obj2 := store.pinRandomObject(t, acc2, pinSlabs(acc2, randomSlabs(2)))

	// listing the objects should return obj1 first since it was updated first
	assertObjects(acc1, 1, 0)
	objs = assertObjects(acc2, 0, 2)
	assertObj(obj1Acc2, objs[0])
	assertObj(obj2, objs[1])

	// save object 1 again to update its timestamp; sleep past the next second
	// boundary since updated_at has second precision
	time.Sleep(time.Second)
	obj1Acc2.EncryptedMetadata = []byte("updated meta")
	if err := store.PinObject(acc2, obj1Acc2.PinRequest()); err != nil {
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

	ts := time.Now().Truncate(time.Second)
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

	if err := store.PinObject(acc, objWithMeta.PinRequest()); err != nil {
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

	if err := store.PinObject(acc, objNoMeta.PinRequest()); err != nil {
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

	if err := store.PinObject(acc, objEmptyMeta.PinRequest()); err != nil {
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
	if err := store.PinObject(acc1, obj.PinRequest()); err != nil {
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

func TestObjectsForSlab(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// create a shared slab pinned to both accounts
	sharedSlab := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	for _, acc := range []proto.Account{acc1, acc2} {
		if _, err := store.PinSlabs(acc, time.Time{}, sharedSlab); err != nil {
			t.Fatal(err)
		}
	}

	// create a second slab only pinned to acc1
	slab2 := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	if _, err := store.PinSlabs(acc1, time.Time{}, slab2); err != nil {
		t.Fatal(err)
	}

	// create an object referencing the shared slab on each account
	obj1 := store.pinRandomObject(t, acc1, []slabs.SlabSlice{sharedSlab.Slice(0, 100)})
	obj2 := store.pinRandomObject(t, acc2, []slabs.SlabSlice{sharedSlab.Slice(0, 100)})

	// create a second object on acc1 referencing only slab2
	obj3 := store.pinRandomObject(t, acc1, []slabs.SlabSlice{slab2.Slice(0, 100)})

	// ObjectsForSlab on the shared slab should return both objects
	objects, err := store.ObjectsForSlab(sharedSlab.Digest())
	if err != nil {
		t.Fatal(err)
	} else if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}

	// sort by account for deterministic comparison
	sort.Slice(objects, func(i, j int) bool {
		return bytes.Compare(objects[i].Account[:], objects[j].Account[:]) < 0
	})
	if objects[0].Account != acc1 || objects[0].ObjectID != obj1.ID() {
		t.Fatalf("unexpected object: account %v, id %v", objects[0].Account, objects[0].ObjectID)
	}
	if objects[1].Account != acc2 || objects[1].ObjectID != obj2.ID() {
		t.Fatalf("unexpected object: account %v, id %v", objects[1].Account, objects[1].ObjectID)
	}

	// assert ObjectsForSlab for slab2 returns the acc1 obj
	objects, err = store.ObjectsForSlab(slab2.Digest())
	if err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objects))
	} else if objects[0].Account != acc1 || objects[0].ObjectID != obj3.ID() {
		t.Fatalf("unexpected object: account %v, id %v", objects[0].Account, objects[0].ObjectID)
	}

	// assert ObjectsForSlab for a non-existent slab returns no objects
	objects, err = store.ObjectsForSlab(slabs.SlabID(frand.Entropy256()))
	if err != nil {
		t.Fatal(err)
	} else if len(objects) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objects))
	}
}

// TestDeleteObjectUnpinsSlabs asserts that deleting an object unpins the slabs
// that are no longer referenced by any of the account's objects and queues
// them for deletion, while slabs referenced by other objects or pinned by
// other accounts stay alive.
func TestDeleteObjectUnpinsSlabs(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// slabShared is referenced by both of acc1's objects, slabSole only by
	// acc1's first object and slabForeign by acc1's first object as well as
	// acc2's object
	slabShared := newTestSlab(hk)
	slabSole := newTestSlab(hk)
	slabForeign := newTestSlab(hk)
	store.pinTestSlabs(t, acc1, slabShared, slabSole, slabForeign)
	store.pinTestSlabs(t, acc2, slabForeign)

	// obj1 references slabSole twice to make sure duplicate references are
	// handled
	obj1 := store.pinRandomObject(t, acc1, []slabs.SlabSlice{
		slabShared.Slice(0, 100),
		slabSole.Slice(0, 100),
		slabSole.Slice(100, 200),
		slabForeign.Slice(0, 100),
	})
	obj2 := store.pinRandomObject(t, acc1, []slabs.SlabSlice{slabShared.Slice(0, 100)})
	obj3 := store.pinRandomObject(t, acc2, []slabs.SlabSlice{slabForeign.Slice(0, 100)})

	assertSlabs := func(acc proto.Account, expected ...slabs.SlabID) {
		t.Helper()

		got, err := store.SlabIDs(acc, 0, math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		sortIDs := func(ids []slabs.SlabID) {
			sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
		}
		sortIDs(expected)
		sortIDs(got)
		if !reflect.DeepEqual(expected, got) {
			t.Fatalf("mismatched slab IDs: expected %v, got %v", expected, got)
		}
	}

	assertSlabExists := func(id slabs.SlabID, exists bool) {
		t.Helper()

		_, err := store.Slab(id)
		if exists && err != nil {
			t.Fatalf("expected slab %v to exist, got %v", id, err)
		} else if !exists && !errors.Is(err, slabs.ErrSlabNotFound) {
			t.Fatalf("expected slab %v to be gone, got %v", id, err)
		}
	}

	assertSlabs(acc1, slabShared.Digest(), slabSole.Digest(), slabForeign.Digest())
	assertSlabs(acc2, slabForeign.Digest())

	// delete obj1; slabSole and slabForeign are no longer referenced by any of
	// acc1's objects and are unpinned, slabShared is still referenced by obj2
	if err := store.DeleteObject(acc1, obj1.ID()); err != nil {
		t.Fatal(err)
	}
	assertSlabs(acc1, slabShared.Digest())
	assertSlabs(acc2, slabForeign.Digest())

	// process the deletion queue; slabSole is deleted, slabForeign survives
	// since acc2 still pins it
	store.pruneAllDeletedSlabs(t)
	assertSlabExists(slabShared.Digest(), true)
	assertSlabExists(slabSole.Digest(), false)
	assertSlabExists(slabForeign.Digest(), true)

	// delete the remaining objects; all pins are removed
	if err := store.DeleteObject(acc1, obj2.ID()); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(acc2, obj3.ID()); err != nil {
		t.Fatal(err)
	}
	assertSlabs(acc1)
	assertSlabs(acc2)

	store.pruneAllDeletedSlabs(t)
	assertSlabExists(slabShared.Digest(), false)
	assertSlabExists(slabForeign.Digest(), false)

	// no slabs, sectors or queued deletions may remain and the accounts'
	// pinned totals must be back to zero
	var slabCount, sectorCount, queued, pinned int64
	err := store.pool.QueryRow(t.Context(), `SELECT
		(SELECT COUNT(*) FROM slabs),
		(SELECT COUNT(*) FROM sectors),
		(SELECT COUNT(*) FROM slab_deletion_queue),
		(SELECT COALESCE(SUM(pinned_data + pinned_size), 0) FROM accounts)`).Scan(&slabCount, &sectorCount, &queued, &pinned)
	if err != nil {
		t.Fatal(err)
	}
	if slabCount != 0 || sectorCount != 0 || queued != 0 || pinned != 0 {
		t.Fatalf("expected everything cleaned up, got %d slabs, %d sectors, %d queued, %d pinned", slabCount, sectorCount, queued, pinned)
	}
}

// TestPinObjectAfterScheduledDeletion asserts that an object can be pinned
// while its slabs are queued for deletion and that processing the queue
// afterwards keeps the referenced slabs alive.
func TestPinObjectAfterScheduledDeletion(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// both accounts pin the slab, only acc1 references it with an object
	slab := newTestSlab(hk)
	store.pinTestSlabs(t, acc1, slab)
	store.pinTestSlabs(t, acc2, slab)
	obj1 := store.pinRandomObject(t, acc1, []slabs.SlabSlice{slab.Slice(0, 100)})

	// delete the object; the slab is unpinned for acc1 and queued for deletion
	if err := store.DeleteObject(acc1, obj1.ID()); err != nil {
		t.Fatal(err)
	}

	// acc1 unpinned the slab, so it can't pin the object again without
	// re-pinning the slab first
	obj2 := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		Slabs:                []slabs.SlabSlice{slab.Slice(0, 100)},
		DataSignature:        (types.Signature)(frand.Bytes(64)),
		MetadataSignature:    (types.Signature)(frand.Bytes(64)),
	}
	if err := store.PinObject(acc1, obj2.PinRequest()); !errors.Is(err, slabs.ErrObjectUnpinnedSlab) {
		t.Fatalf("expected ErrObjectUnpinnedSlab, got %v", err)
	}

	// acc2 still pins the slab and pins the object while the slab is queued
	// for deletion
	obj2 = store.pinRandomObject(t, acc2, []slabs.SlabSlice{slab.Slice(0, 100)})

	// processing the queue must not delete the slab
	store.pruneAllDeletedSlabs(t)
	if _, err := store.Slab(slab.Digest()); err != nil {
		t.Fatalf("expected slab to survive the prune, got %v", err)
	}

	// simulate the transient state where an object references the slab while
	// no account pins it; the prune must keep the slab alive as well
	_, err := store.pool.Exec(t.Context(), `DELETE FROM account_slabs`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.pool.Exec(t.Context(), `INSERT INTO slab_deletion_queue (slab_id) SELECT id FROM slabs`)
	if err != nil {
		t.Fatal(err)
	}
	store.pruneAllDeletedSlabs(t)
	if _, err := store.Slab(slab.Digest()); err != nil {
		t.Fatalf("expected referenced slab to survive the prune, got %v", err)
	}

	// the object must still be fully intact
	obj, err := store.Object(acc2, obj2.ID())
	if err != nil {
		t.Fatal(err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs))
	} else if len(obj.Slabs[0].Sectors) != len(slab.Sectors) {
		t.Fatalf("expected %d sectors, got %d", len(slab.Sectors), len(obj.Slabs[0].Sectors))
	}
}

// TestUnpinSlabInUse asserts that a slab can not be unpinned while an object
// of the same account references it, that objects of other accounts don't
// block the unpin, and that deleting the object cleans the slab up through the
// regular unpin path.
func TestUnpinSlabInUse(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	accA, accB := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{accA, accB} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// both accounts pin the slab, only accB references it with an object
	slab := newTestSlab(hk)
	store.pinTestSlabs(t, accA, slab)
	store.pinTestSlabs(t, accB, slab)
	obj := store.pinRandomObject(t, accB, []slabs.SlabSlice{slab.Slice(0, 100)})

	// explicitly unpinning the slab while the account's own object references
	// it must fail
	if err := store.UnpinSlab(accB, slab.Digest()); !errors.Is(err, slabs.ErrSlabInUse) {
		t.Fatalf("expected ErrSlabInUse, got %v", err)
	}

	// accB's object doesn't prevent accA from unpinning the slab
	if err := store.UnpinSlab(accA, slab.Digest()); err != nil {
		t.Fatal(err)
	}

	// the queued deletion must not remove the slab since accB still pins it
	store.pruneAllDeletedSlabs(t)
	if _, err := store.Slab(slab.Digest()); err != nil {
		t.Fatalf("expected slab to survive the prune, got %v", err)
	}

	// deleting the object unpins the slab and queues it for deletion
	if err := store.DeleteObject(accB, obj.ID()); err != nil {
		t.Fatal(err)
	}
	if err := store.UnpinSlab(accB, slab.Digest()); !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatalf("expected ErrSlabNotFound, got %v", err)
	}
	store.pruneAllDeletedSlabs(t)
	if _, err := store.Slab(slab.Digest()); !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatalf("expected slab to be deleted, got %v", err)
	}

	// no slabs, sectors or queued deletions may remain
	var slabCount, sectorCount, queued int64
	err := store.pool.QueryRow(t.Context(), `SELECT
		(SELECT COUNT(*) FROM slabs),
		(SELECT COUNT(*) FROM sectors),
		(SELECT COUNT(*) FROM slab_deletion_queue)`).Scan(&slabCount, &sectorCount, &queued)
	if err != nil {
		t.Fatal(err)
	}
	if slabCount != 0 || sectorCount != 0 || queued != 0 {
		t.Fatalf("expected everything cleaned up, got %d slabs, %d sectors, %d queued", slabCount, sectorCount, queued)
	}
}

// TestDeleteObjectKeepsObjectlessPin asserts that deleting the only object
// referencing a slab unpins it for the deleting account only; another account
// pinning the slab without any object keeps its pin and the slab survives the
// deletion queue.
func TestDeleteObjectKeepsObjectlessPin(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	accA, accB := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{accA, accB} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// account A pins the slab without attaching it to an object, account B
	// pins it and references it with an object
	slab := newTestSlab(hk)
	store.pinTestSlabs(t, accA, slab)
	store.pinTestSlabs(t, accB, slab)
	obj := store.pinRandomObject(t, accB, []slabs.SlabSlice{slab.Slice(0, 100)})

	// delete the object; B's pin is removed, A's pin stays
	if err := store.DeleteObject(accB, obj.ID()); err != nil {
		t.Fatal(err)
	}

	assertSlabs := func(acc proto.Account, expected ...slabs.SlabID) {
		t.Helper()

		got, err := store.SlabIDs(acc, 0, math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(expected, got) {
			t.Fatalf("mismatched slab IDs: expected %v, got %v", expected, got)
		}
	}
	assertSlabs(accA, slab.Digest())
	assertSlabs(accB)

	// processing the queue must not delete the slab since A still pins it
	store.pruneAllDeletedSlabs(t)
	assertSlabs(accA, slab.Digest())
	assertSlabs(accB)
	if _, err := store.PinnedSlab(accA, slab.Digest()); err != nil {
		t.Fatalf("expected slab to still be pinned by account A, got %v", err)
	}
	if _, err := store.PinnedSlab(accB, slab.Digest()); !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatalf("expected slab to no longer be pinned by account B, got %v", err)
	}
}

// TestParallelObjectDeletion races deletions of objects sharing a slab as well
// as explicit unpins against object deletions. The shared slab must be cleaned
// up exactly once and the account's pinned totals must return to zero.
func TestParallelObjectDeletion(t *testing.T) {
	const rounds = 30

	store := initPostgres(t, zap.NewNop())

	acc := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(acc))

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// assertCleanedUp asserts no slabs, sectors, pins or queued deletions
	// remain and the account's pinned totals are back to zero
	assertCleanedUp := func(t testing.TB) {
		t.Helper()

		var slabCount, sectorCount, accountSlabs, queued, pinned int64
		err := store.pool.QueryRow(t.Context(), `SELECT
			(SELECT COUNT(*) FROM slabs),
			(SELECT COUNT(*) FROM sectors),
			(SELECT COUNT(*) FROM account_slabs),
			(SELECT COUNT(*) FROM slab_deletion_queue),
			(SELECT COALESCE(SUM(pinned_data + pinned_size), 0) FROM accounts)`).Scan(&slabCount, &sectorCount, &accountSlabs, &queued, &pinned)
		if err != nil {
			t.Fatal(err)
		}
		if slabCount != 0 || sectorCount != 0 || accountSlabs != 0 || queued != 0 || pinned != 0 {
			t.Fatalf("expected everything cleaned up, got %d slabs, %d sectors, %d pins, %d queued, %d pinned data", slabCount, sectorCount, accountSlabs, queued, pinned)
		}
	}

	// two objects sharing a slab are deleted in parallel; exactly one of the
	// deletions must unpin the shared slab
	for range rounds {
		shared := newTestSlab(hk)
		store.pinTestSlabs(t, acc, shared)
		obj1 := store.pinRandomObject(t, acc, []slabs.SlabSlice{shared.Slice(0, 100)})
		obj2 := store.pinRandomObject(t, acc, []slabs.SlabSlice{shared.Slice(0, 100), shared.Slice(100, 200)})

		errs := make(chan error, 2)
		for _, key := range []types.Hash256{obj1.ID(), obj2.ID()} {
			go func() {
				errs <- store.DeleteObject(acc, key)
			}()
		}
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}

		store.pruneAllDeletedSlabs(t)
		if _, err := store.Slab(shared.Digest()); !errors.Is(err, slabs.ErrSlabNotFound) {
			t.Fatalf("expected slab to be deleted, got %v", err)
		}
		assertCleanedUp(t)
	}

	// an explicit unpin races the deletion of the last object referencing the
	// slab; the account's totals must only be decremented once
	for range rounds {
		slab := newTestSlab(hk)
		store.pinTestSlabs(t, acc, slab)
		obj := store.pinRandomObject(t, acc, []slabs.SlabSlice{slab.Slice(0, 100)})

		errs := make(chan error, 2)
		go func() {
			errs <- store.DeleteObject(acc, obj.ID())
		}()
		go func() {
			// the slab is in use until the object is deleted and unpinned
			// right after, so the explicit unpin always fails with one of the
			// two errors depending on how the calls interleave
			if err := store.UnpinSlab(acc, slab.Digest()); errors.Is(err, slabs.ErrSlabInUse) || errors.Is(err, slabs.ErrSlabNotFound) {
				errs <- nil
			} else {
				errs <- fmt.Errorf("expected ErrSlabInUse or ErrSlabNotFound, got %w", err)
			}
		}()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}

		store.pruneAllDeletedSlabs(t)
		if _, err := store.Slab(slab.Digest()); !errors.Is(err, slabs.ErrSlabNotFound) {
			t.Fatalf("expected slab to be deleted, got %v", err)
		}
		assertCleanedUp(t)
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
		if err := store.PinObject(acc1, obj.PinRequest()); err != nil {
			b.Fatal(err)
		}
		objs = append(objs, obj)
	}

	obj := pinObject(b)
	pinReq := obj.PinRequest()
	for b.Loop() {
		if err := store.PinObject(acc1, pinReq); err != nil {
			b.Fatal(err)
		}
	}
}
