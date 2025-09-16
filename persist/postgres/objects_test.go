package postgres

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
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

	assertObjects := func(acc proto4.Account, n int) []slabs.Object {
		t.Helper()
		objects, err := store.ListObjects(context.Background(), acc, slabs.Cursor{}, 10)
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
	obj := slabs.Object{
		Key: objKey,
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
		Meta: []byte("hello world"),
	}
	for _, acc := range []proto4.Account{acc1, acc2} {
		err := store.SaveObject(context.Background(), acc, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertObj := func(obj, other slabs.Object) {
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
	objs, err := store.ListObjects(context.Background(), acc2, slabs.Cursor{}, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 {
		t.Fatalf("expected 1 objects, got %d", len(objs))
	}

	// increasing 'after' to now should not yield any results
	objs, err = store.ListObjects(context.Background(), acc2, slabs.Cursor{After: time.Now()}, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}

	// assert we can fetch a single object
	obj, err = store.Object(context.Background(), acc2, obj2.Key)
	if err != nil {
		t.Fatal(err)
	} else if obj.CreatedAt.IsZero() || obj.UpdatedAt.IsZero() {
		t.Fatalf("expected non-zero timestamps, got %v and %v", obj.CreatedAt, obj.UpdatedAt)
	}
	assertObj(obj2, obj)

	// assert account is taken into consideration when fetching an object
	_, err = store.Object(context.Background(), acc1, obj2.Key)
	if !errors.Is(err, slabs.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	// assert fetching a non-existent object returns the correct error
	_, err = store.Object(context.Background(), acc2, frand.Entropy256())
	if !errors.Is(err, slabs.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

// TestListObjectsRegression is a small regression tests that asserts proper
// handling of cursor.key which was not casted as a sqlHash256 at one point.
func TestListObjectsRegression(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create account
	acc := proto4.Account{1}
	err := store.AddAccount(context.Background(), types.PublicKey(acc), accounts.AccountMeta{})
	if err != nil {
		t.Fatal(err)
	}

	// pin slab for both accounts
	slab := slabs.SlabPinParams{MinShards: 1}
	_, err = store.PinSlab(context.Background(), acc, time.Time{}, slab)
	if err != nil {
		t.Fatal(err)
	}
	slabID, err := slab.Digest()
	if err != nil {
		t.Fatal(err)
	}

	// add multiple objects
	for i := 3; i >= 1; i-- {
		if err := store.SaveObject(context.Background(), acc, slabs.Object{
			Key: types.Hash256{byte(i)},
			Slabs: []slabs.SlabSlice{
				{
					SlabID: slabID,
					Offset: 0,
					Length: 12,
				},
			},
			Meta: []byte("meta"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	ts := time.Now().Round(time.Second)
	_, err = store.pool.Exec(context.Background(), "UPDATE objects SET updated_at = $1", ts)
	if err != nil {
		t.Fatal(err)
	}

	objs, err := store.ListObjects(context.Background(), acc, slabs.Cursor{After: ts}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(objs) != 3 {
		t.Fatal("expected 3 objects, got", len(objs))
	} else if objs[0].Key != (types.Hash256{1}) || objs[1].Key != (types.Hash256{2}) || objs[2].Key != (types.Hash256{3}) {
		t.Fatal("objects not in expected order")
	}
}

func TestSharedObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto4.Account{1}, proto4.Account{2}
	for _, acc := range []proto4.Account{acc1, acc2} {
		err := store.AddAccount(context.Background(), types.PublicKey(acc), accounts.AccountMeta{})
		if err != nil {
			t.Fatal(err)
		}
	}

	hostKeys := make([]types.PublicKey, 30)
	for i := range hostKeys {
		hostKeys[i] = types.GeneratePrivateKey().PublicKey()
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hostKeys[i], chain.V2HostAnnouncement{{Protocol: quic.Protocol, Address: "[::]:4848"}}, time.Now())
		}); err != nil {
			t.Fatal(err)
		}
	}

	pinRandomSlab := func(t *testing.T) slabs.SharedObjectSlab {
		t.Helper()

		s := slabs.SlabPinParams{
			MinShards:     uint(frand.Intn(255)),
			EncryptionKey: frand.Entropy256(),
			Sectors:       make([]slabs.SectorPinParams, 30),
		}
		for i := range s.Sectors {
			s.Sectors[i].HostKey = hostKeys[i%len(hostKeys)]
			s.Sectors[i].Root = frand.Entropy256()
		}

		slabID, err := store.PinSlab(t.Context(), acc1, time.Time{}, s)
		if err != nil {
			t.Fatal(err)
		} else if id, err := s.Digest(); err != nil {
			t.Fatal(err)
		} else if id != slabID {
			t.Fatalf("expected slab ID %v, got %v", id, slabID)
		}

		so := slabs.SharedObjectSlab{
			ID:            slabID,
			EncryptionKey: s.EncryptionKey,
			MinShards:     s.MinShards,
			Sectors:       make([]slabs.PinnedSector, len(s.Sectors)),
			Offset:        uint32(frand.Uint64n(math.MaxInt32)),
			Length:        uint32(frand.Uint64n(math.MaxInt32)),
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
		Key:   frand.Entropy256(),
		Slabs: []slabs.SharedObjectSlab{pinRandomSlab(t), pinRandomSlab(t), pinRandomSlab(t)},
		Meta:  []byte("hello world"),
	}
	obj := slabs.Object{
		Key:   expectedSharedObj.Key,
		Slabs: make([]slabs.SlabSlice, len(expectedSharedObj.Slabs)),
		Meta:  expectedSharedObj.Meta,
	}
	for i, slab := range expectedSharedObj.Slabs {
		obj.Slabs[i] = slabs.SlabSlice{
			SlabID: slab.ID,
			Offset: slab.Offset,
			Length: slab.Length,
		}
	}
	if err := store.SaveObject(context.Background(), acc1, obj); err != nil {
		t.Fatal(err)
	}

	sharedObj, err := store.SharedObject(t.Context(), obj.Key)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(expectedSharedObj, sharedObj) {
		t.Fatalf("shared objects not equal: expected %+v, got %+v", expectedSharedObj, sharedObj)
	}

	// pin the slabs to the second account
	for _, slab := range expectedSharedObj.Slabs {
		_, err := store.PinSlab(t.Context(), acc2, time.Time{}, slabs.SlabPinParams{
			MinShards: slab.MinShards,
			Sectors: func() []slabs.SectorPinParams {
				sps := make([]slabs.SectorPinParams, len(slab.Sectors))
				for i := range slab.Sectors {
					sps[i] = slabs.SectorPinParams{
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
		err := store.AddAccount(context.Background(), types.PublicKey(acc), accounts.AccountMeta{})
		if err != nil {
			b.Fatal(err)
		}
	}

	hostKeys := make([]types.PublicKey, 30)
	for i := range hostKeys {
		hostKeys[i] = types.GeneratePrivateKey().PublicKey()
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hostKeys[i], chain.V2HostAnnouncement{{Protocol: quic.Protocol, Address: "[::]:4848"}}, time.Now())
		}); err != nil {
			b.Fatal(err)
		}
	}

	var objs []slabs.Object
	pinObject := func(b *testing.B) (obj slabs.Object) {
		b.Helper()

		s := slabs.SlabPinParams{
			MinShards:     uint(frand.Intn(255)) + 1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       make([]slabs.SectorPinParams, 30),
		}
		for i := range s.Sectors {
			s.Sectors[i].HostKey = hostKeys[i%len(hostKeys)]
			s.Sectors[i].Root = frand.Entropy256()
		}

		slabID, err := store.PinSlab(b.Context(), acc1, time.Time{}, s)
		if err != nil {
			b.Fatal(err)
		}

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

		obj.Key = frand.Entropy256()
		obj.Meta = make([]byte, 1024)
		frand.Read(obj.Meta)

		return
	}

	for i := 0; i < 10000; i++ {
		obj := pinObject(b)
		if err := store.SaveObject(b.Context(), acc1, obj); err != nil {
			b.Fatal(err)
		}
		objs = append(objs, obj)
	}

	obj := pinObject(b)
	for b.Loop() {
		obj.Key = frand.Entropy256()
		if err := store.SaveObject(b.Context(), acc1, obj); err != nil {
			b.Fatal(err)
		}
	}
}
