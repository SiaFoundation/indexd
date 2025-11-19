package postgres

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto4.Account{1}, proto4.Account{2}
	for _, acc := range []proto4.Account{acc1, acc2} {
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
	for _, acc := range []proto4.Account{acc1, acc2} {
		_, err := store.PinSlabs(acc, time.Time{}, slab)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertObjects := func(acc proto4.Account, expectedDeleted, expectedExist int) []slabs.ObjectEvent {
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
				Sectors: []slabs.PinnedSector{
					{
						Root:    frand.Entropy256(),
						HostKey: hk,
					},
				},
			}
		}
		return s
	}

	pinSlabs := func(acc proto4.Account, params []slabs.SlabPinParams) []slabs.SlabSlice {
		t.Helper()

		var ss []slabs.SlabSlice
		for _, p := range params {
			ids, err := store.PinSlabs(acc, time.Time{}, p)
			if err != nil {
				t.Fatal(err)
			}
			ss = append(ss, slabs.SlabSlice{
				SlabID: ids[0],
				Offset: 10,
				Length: 120,
			})
		}
		return ss
	}

	randomObject := func(s []slabs.SlabSlice) slabs.SealedObject {
		return slabs.SealedObject{
			EncryptedMasterKey: frand.Bytes(72),
			Slabs:              s,
			EncryptedMetadata:  []byte("hello world"),
			Signature:          (types.Signature)(frand.Bytes(64)),
		}
	}
	obj1Slabs := randomSlabs(3)
	pinSlabs(acc1, obj1Slabs)
	pinSlabs(acc2, obj1Slabs)
	obj1Acc1 := randomObject(pinSlabs(acc1, obj1Slabs))

	if err := store.SaveObject(acc1, obj1Acc1); err != nil {
		t.Fatal(err)
	}
	// pin the same object for acc2 with different master key and sig to satisfy unique constraint
	obj1Acc2 := obj1Acc1
	obj1Acc2.EncryptedMasterKey = frand.Bytes(72)
	obj1Acc2.Signature = (types.Signature)(frand.Bytes(64))
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
	obj2 := randomObject(pinSlabs(acc2, randomSlabs(2)))
	if err := store.SaveObject(acc2, obj2); err != nil {
		t.Fatal(err)
	}

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
	obj2Acc1 := randomObject(pinSlabs(acc1, randomSlabs(3)))
	if err := store.SaveObject(acc1, obj2Acc1); err != nil {
		t.Fatal(err)
	}
	assertObjects(acc1, 1, 1)
}

// TestListObjectsRegression is a small regression tests that asserts proper
// handling of cursor.key which was not casted as a sqlHash256 at one point.
func TestListObjectsRegression(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create account
	acc := proto4.Account{1}
	store.addTestAccount(t, types.PublicKey(acc))

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	randomObject := func() slabs.SealedObject {
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
		slabID, err := slab.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return slabs.SealedObject{
			EncryptedMasterKey: frand.Bytes(72),
			Slabs: []slabs.SlabSlice{
				{
					SlabID: slabID,
					Offset: 10,
					Length: 100,
				},
				{
					SlabID: slabID,
					Offset: 110,
					Length: 200,
				},
			},
			EncryptedMetadata: []byte("hello world"),
			Signature:         (types.Signature)(frand.Bytes(64)),
		}
	}

	// add multiple objects
	var objectIDs []types.Hash256
	for range 3 {
		obj := randomObject()
		objectIDs = append(objectIDs, obj.ID())
		if err := store.SaveObject(acc, obj); err != nil {
			t.Fatal(err)
		}
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

func TestSharedObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto4.Account{1}, proto4.Account{2}
	for _, acc := range []proto4.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hostKeys := make([]types.PublicKey, 30)
	for i := range hostKeys {
		hostKeys[i] = store.addTestHost(t)
		store.addTestContract(t, hostKeys[i])
	}

	pinRandomSlab := func(t *testing.T) slabs.SharedSlab {
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
		} else if id, err := s.Digest(); err != nil {
			t.Fatal(err)
		} else if id != slabIDs[0] {
			t.Fatalf("expected slab ID %v, got %v", id, slabIDs[0])
		}

		so := slabs.SharedSlab{
			PinnedSlab: slabs.PinnedSlab{
				ID:            slabIDs[0],
				EncryptionKey: s.EncryptionKey,
				MinShards:     s.MinShards,
				Sectors:       make([]slabs.PinnedSector, len(s.Sectors)),
			},
			Offset: uint32(frand.Uint64n(math.MaxInt32)),
			Length: uint32(frand.Uint64n(math.MaxInt32)),
		}
		for i := range s.Sectors {
			so.Sectors[i] = slabs.PinnedSector{
				Root:    s.Sectors[i].Root,
				HostKey: s.Sectors[i].HostKey,
			}
		}
		return so
	}

	// add an object with multiple slabs
	expectedSharedObj := slabs.SharedObject{
		Slabs:             []slabs.SharedSlab{pinRandomSlab(t), pinRandomSlab(t), pinRandomSlab(t)},
		EncryptedMetadata: []byte("hello world"),
	}
	obj := slabs.SealedObject{
		EncryptedMasterKey: frand.Bytes(72),
		Slabs:              make([]slabs.SlabSlice, len(expectedSharedObj.Slabs)),
		EncryptedMetadata:  expectedSharedObj.EncryptedMetadata,
	}
	for i, slab := range expectedSharedObj.Slabs {
		obj.Slabs[i] = slabs.SlabSlice{
			SlabID: slab.ID,
			Offset: slab.Offset,
			Length: slab.Length,
		}
	}
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
			MinShards: slab.MinShards,
			Sectors: func() []slabs.PinnedSector {
				sps := make([]slabs.PinnedSector, len(slab.Sectors))
				for i := range slab.Sectors {
					sps[i] = slabs.PinnedSector{
						Root:    slab.Sectors[i].Root,
						HostKey: slab.Sectors[i].HostKey,
					}
				}
				return sps
			}(),
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
	acc1, acc2 := proto4.Account{1}, proto4.Account{2}
	for _, acc := range []proto4.Account{acc1, acc2} {
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

		id, err := s.Digest()
		if err != nil {
			b.Fatal(err)
		} else if id != slabID {
			b.Fatalf("expected slab ID %v, got %v", id, slabID)
		}

		obj.Slabs = append(obj.Slabs, slabs.SlabSlice{
			SlabID: id,
			Offset: 0,
			Length: 256,
		})
		for i := 0; i < 20 && i < len(objs); i++ {
			obj.Slabs = append(obj.Slabs, slabs.SlabSlice{
				SlabID: objs[i].Slabs[0].SlabID,
				Offset: 0,
				Length: 256,
			})
		}
		obj.EncryptedMetadata = frand.Bytes(1024)
		obj.EncryptedMasterKey = frand.Bytes(72)
		obj.Signature = types.Signature(frand.Bytes(64))

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
