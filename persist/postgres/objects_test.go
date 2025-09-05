package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/objects"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto4.Account{1}, proto4.Account{2}
	for _, acc := range []proto4.Account{acc1, acc2} {
		err := store.AddAccount(context.Background(), types.PublicKey(acc), accounts.AccountMeta{})
		if err != nil {
			t.Fatal(err)
		}
	}

	// pin slab for both accounts
	slab := slabs.SlabPinParams{MinShards: 1}
	for _, acc := range []proto4.Account{acc1, acc2} {
		_, err := store.PinSlab(context.Background(), acc, time.Time{}, slab)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertObjects := func(acc proto4.Account, n int) []objects.Object {
		t.Helper()
		objects, err := store.ListObjects(context.Background(), acc, objects.Cursor{}, 10)
		if err != nil {
			t.Fatal(err)
		} else if len(objects) != n {
			t.Fatalf("expected %d objects, got %d", n, len(objects))
		}
		return objects
	}

	// no objects should exist
	assertObjects(acc1, 0)
	assertObjects(acc2, 0)

	// add objects for both accounts
	objKey := frand.Entropy256()
	slabID, _ := slab.Digest()
	obj := objects.Object{
		Key: objKey,
		Slabs: []objects.SlabSlice{
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
		Meta: []byte("hello world"),
	}
	for _, acc := range []proto4.Account{acc1, acc2} {
		err := store.SaveObject(context.Background(), acc, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertObj := func(obj, other objects.Object) {
		t.Helper()
		if other.CreatedAt.IsZero() || other.UpdatedAt.IsZero() {
			t.Fatalf("expected non-zero timestamps, got %v and %v", other.CreatedAt, other.UpdatedAt)
		}
		other.CreatedAt = time.Time{}
		other.UpdatedAt = time.Time{}
		if !reflect.DeepEqual(obj, other) {
			t.Fatalf("objects not equal: expected %+v, got %+v", obj, other)
		}
	}

	// 1 object should exist for both accounts
	objs := assertObjects(acc1, 1)
	assertObj(obj, objs[0])

	objs = assertObjects(acc2, 1)
	assertObj(obj, objs[0])

	// delete object for acc1
	if err := store.DeleteObject(context.Background(), acc1, objKey); err != nil {
		t.Fatal(err)
	}

	// no object should exist for acc1, 1 for acc2
	assertObjects(acc1, 0)
	objs = assertObjects(acc2, 1)
	assertObj(obj, objs[0])

	// add another object to acc2
	obj2 := obj
	obj2.Key = frand.Entropy256()
	if err := store.SaveObject(context.Background(), acc2, obj2); err != nil {
		t.Fatal(err)
	}

	// listing the objects should return obj1 first since it was updated first
	assertObjects(acc1, 0)
	objs = assertObjects(acc2, 2)
	assertObj(obj, objs[0])
	assertObj(obj2, objs[1])

	// save object 1 again to update its timestamp
	obj3 := obj // same key as obj
	obj3.Meta = []byte("updated meta")
	if err := store.SaveObject(context.Background(), acc2, obj3); err != nil {
		t.Fatal(err)
	}

	// the order should be reversed now
	assertObjects(acc1, 0)
	objs = assertObjects(acc2, 2)
	assertObj(obj2, objs[0])
	assertObj(obj3, objs[1])

	// make sure the limit works
	objs, err := store.ListObjects(context.Background(), acc2, objects.Cursor{}, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 {
		t.Fatalf("expected 1 objects, got %d", len(objs))
	}

	// increasing 'after' to now should not yield any results
	objs, err = store.ListObjects(context.Background(), acc2, objects.Cursor{After: time.Now()}, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}
}
