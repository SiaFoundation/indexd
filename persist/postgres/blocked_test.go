package postgres

import (
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestBlocklist(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	keys := []types.Hash256{frand.Entropy256(), frand.Entropy256(), frand.Entropy256()}

	if _, err := store.BlockedObject(keys[0]); !errors.Is(err, slabs.ErrObjectNotBlocked) {
		t.Fatalf("expected ErrObjectNotBlocked, got %v", err)
	} else if err := store.UnblockObject(keys[0]); err != nil {
		t.Fatalf("expected unblocking an unblocked key to be a no-op, got %v", err)
	}

	// a key does not have to reference an existing object
	for _, key := range keys[:2] {
		if err := store.BlockObject(key, "dmca-1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.BlockObject(keys[2], "dmca-2"); err != nil {
		t.Fatal(err)
	}

	for _, key := range keys[:2] {
		blocked, err := store.BlockedObject(key)
		if err != nil {
			t.Fatal(err)
		} else if blocked.Key != key {
			t.Fatalf("expected key %v, got %v", key, blocked.Key)
		} else if blocked.Reason != "dmca-1" {
			t.Fatalf("expected reason %q, got %q", "dmca-1", blocked.Reason)
		} else if blocked.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created at")
		}
	}

	// blocking an already blocked key updates its reason
	if err := store.BlockObject(keys[0], "dmca-3"); err != nil {
		t.Fatal(err)
	} else if blocked, err := store.BlockedObject(keys[0]); err != nil {
		t.Fatal(err)
	} else if blocked.Reason != "dmca-3" {
		t.Fatalf("expected reason %q, got %q", "dmca-3", blocked.Reason)
	}

	list, err := store.BlockedObjects(0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	} else if len(list) != 3 {
		t.Fatalf("expected 3 blocked objects, got %d", len(list))
	}
	for i := 1; i < len(list); i++ {
		if list[i].CreatedAt.After(list[i-1].CreatedAt) {
			t.Fatal("expected blocklist sorted by created at descending")
		}
	}

	// pagination
	if page, err := store.BlockedObjects(1, 1); err != nil {
		t.Fatal(err)
	} else if len(page) != 1 {
		t.Fatalf("expected 1 blocked object, got %d", len(page))
	} else if page[0].Key != list[1].Key {
		t.Fatalf("expected key %v, got %v", list[1].Key, page[0].Key)
	}

	if err := store.UnblockObject(keys[0]); err != nil {
		t.Fatal(err)
	} else if _, err := store.BlockedObject(keys[0]); !errors.Is(err, slabs.ErrObjectNotBlocked) {
		t.Fatalf("expected ErrObjectNotBlocked, got %v", err)
	} else if list, err := store.BlockedObjects(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(list) != 2 {
		t.Fatalf("expected 2 blocked objects, got %d", len(list))
	}
}

func TestBlockedObjectAccess(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	obj := store.pinTestObject(t, acc, hk)
	key := obj.ID()

	if _, err := store.Object(acc, key); err != nil {
		t.Fatal(err)
	} else if _, err := store.SharedObject(key); err != nil {
		t.Fatal(err)
	}

	if err := store.BlockObject(key, "dmca"); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Object(acc, key); !errors.Is(err, slabs.ErrObjectBlocked) {
		t.Fatalf("expected ErrObjectBlocked, got %v", err)
	} else if _, err := store.SharedObject(key); !errors.Is(err, slabs.ErrObjectBlocked) {
		t.Fatalf("expected ErrObjectBlocked, got %v", err)
	}

	if err := store.PinObject(acc, obj.PinRequest()); !errors.Is(err, slabs.ErrObjectBlocked) {
		t.Fatalf("expected ErrObjectBlocked, got %v", err)
	}

	other := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(other))
	if err := store.PinObject(other, obj.PinRequest()); !errors.Is(err, slabs.ErrObjectBlocked) {
		t.Fatalf("expected ErrObjectBlocked, got %v", err)
	}

	// unblocking restores access
	if err := store.UnblockObject(key); err != nil {
		t.Fatal(err)
	} else if got, err := store.Object(acc, key); err != nil {
		t.Fatal(err)
	} else if got.ID() != key {
		t.Fatalf("expected object %v, got %v", key, got.ID())
	}
}

func TestBlockedObjectsHiddenFromListing(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	const n = 5
	objs := make([]slabs.SealedObject, n)
	for i := range objs {
		objs[i] = store.pinTestObject(t, acc, hk)
	}

	// listAll pages through every event two at a time
	listAll := func() []slabs.ObjectEvent {
		t.Helper()
		awaitEventSecond(t)
		var all []slabs.ObjectEvent
		var cursor slabs.Cursor
		for {
			events, err := store.ListObjects(acc, cursor, 2)
			if err != nil {
				t.Fatal(err)
			} else if len(events) == 0 {
				return all
			}
			all = append(all, events...)
			last := events[len(events)-1]
			cursor = slabs.Cursor{After: last.UpdatedAt, Key: last.Key}
		}
	}

	updatedAt := make(map[types.Hash256]time.Time)
	if events := listAll(); len(events) != n {
		t.Fatalf("expected %d objects, got %d", n, len(events))
	} else {
		for _, ev := range events {
			updatedAt[ev.Key] = ev.UpdatedAt
		}
	}

	// block a run larger than the page size: filtering after the limit would
	// return an empty page and stop a paginating client early
	blocked := []types.Hash256{objs[1].ID(), objs[2].ID(), objs[3].ID()}
	for _, key := range blocked {
		if err := store.BlockObject(key, "dmca"); err != nil {
			t.Fatal(err)
		}
	}

	events := listAll()
	if len(events) != n-len(blocked) {
		t.Fatalf("expected %d objects, got %d", n-len(blocked), len(events))
	}
	for _, ev := range events {
		for _, b := range blocked {
			if ev.Key == b {
				t.Fatalf("blocked object %v was listed", ev.Key)
			}
		}
	}

	// unblocking bumps the event timestamps so a caught-up client sees the
	// objects again. The bump has to advance even when the unblock lands in the
	// same second as the last event.
	for _, b := range blocked {
		if err := store.UnblockObject(b); err != nil {
			t.Fatal(err)
		}
	}

	all := listAll()
	if len(all) != n {
		t.Fatalf("expected %d objects after unblocking, got %d", n, len(all))
	}
	for _, ev := range all {
		if !slices.Contains(blocked, ev.Key) {
			continue
		} else if !ev.UpdatedAt.After(updatedAt[ev.Key]) {
			t.Fatalf("expected object %v to be bumped past %v, got %v", ev.Key, updatedAt[ev.Key], ev.UpdatedAt)
		} else if ev.Object == nil {
			t.Fatalf("expected object %v to be populated", ev.Key)
		}
	}
}

// TestUnblockObjectVisibleToLiveCursor checks that an unblocked object reaches a
// client whose cursor is parked on a newer event with a larger key.
func TestUnblockObjectVisibleToLiveCursor(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// the object we block has to sort before the one the cursor rests on
	first, second := store.pinTestObject(t, acc, hk), store.pinTestObject(t, acc, hk)
	firstID, secondID := first.ID(), second.ID()
	blocked, live := firstID, secondID
	if slices.Compare(firstID[:], secondID[:]) > 0 {
		blocked, live = secondID, firstID
	}

	if err := store.BlockObject(blocked, "dmca"); err != nil {
		t.Fatal(err)
	}

	// park a cursor on the live object, all a client can see for now
	awaitEventSecond(t)
	events, err := store.ListObjects(acc, slabs.Cursor{}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 visible object, got %d", len(events))
	} else if events[0].Key != live {
		t.Fatalf("expected live object %v, got %v", live, events[0].Key)
	}
	cursor := slabs.Cursor{After: events[0].UpdatedAt, Key: events[0].Key}

	if err := store.UnblockObject(blocked); err != nil {
		t.Fatal(err)
	}

	awaitEventSecond(t)
	events, err = store.ListObjects(acc, cursor, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected unblocked object %v to be visible from the parked cursor, got %d events", blocked, len(events))
	} else if events[0].Key != blocked {
		t.Fatalf("expected unblocked object %v, got %v", blocked, events[0].Key)
	} else if events[0].Object == nil {
		t.Fatalf("expected object %v to be populated", blocked)
	}
}

func TestBlockedSharedObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	sharingKey := store.addTestSharingKey(t, acc, "share")

	const n = 4
	objs := make([]slabs.SealedObject, n)
	for i := range objs {
		objs[i] = store.pinTestObject(t, acc, hk)
		attachTestObject(t, store, acc, sharingKey, objs[i].ID())
	}

	key, err := store.SharingKey(sharingKey)
	if err != nil {
		t.Fatal(err)
	}
	total := key

	blockedKey := objs[0].ID()
	if err := store.BlockObject(blockedKey, "dmca"); err != nil {
		t.Fatal(err)
	}

	// blocked objects are dropped before the limit, so a full page is still full
	shared, err := store.SharedObjects(sharingKey, 0, n-1)
	if err != nil {
		t.Fatal(err)
	} else if len(shared) != n-1 {
		t.Fatalf("expected %d shared objects, got %d", n-1, len(shared))
	}
	for _, obj := range shared {
		if obj.ID() == blockedKey {
			t.Fatal("blocked object was listed")
		}
	}

	// paging to the end returns one object fewer than before
	if shared, err := store.SharedObjects(sharingKey, 0, math.MaxInt16); err != nil {
		t.Fatal(err)
	} else if len(shared) != n-1 {
		t.Fatalf("expected %d shared objects, got %d", n-1, len(shared))
	}

	if _, err := store.SharingKeyObject(sharingKey, blockedKey); !errors.Is(err, slabs.ErrObjectBlocked) {
		t.Fatalf("expected ErrObjectBlocked, got %v", err)
	}

	other := store.addTestSharingKey(t, acc, "other")
	err = store.AddSharedObject(acc, other, sharing.SharedObjectRequest{
		ObjectID:          blockedKey,
		EncryptedDataKey:  frand.Bytes(sharing.EncryptionKeySize),
		DataSignature:     types.Signature(frand.Bytes(64)),
		MetadataSignature: types.Signature(frand.Bytes(64)),
	})
	if !errors.Is(err, slabs.ErrObjectBlocked) {
		t.Fatalf("expected ErrObjectBlocked, got %v", err)
	}

	// blocking does not touch the key's totals
	assertKeyTotals(t, store, sharingKey, total.ObjectCount, total.ObjectSize, total.PinnedData, total.PinnedSize)

	// unblocking restores the listing
	if err := store.UnblockObject(blockedKey); err != nil {
		t.Fatal(err)
	} else if shared, err := store.SharedObjects(sharingKey, 0, math.MaxInt16); err != nil {
		t.Fatal(err)
	} else if len(shared) != n {
		t.Fatalf("expected %d shared objects, got %d", n, len(shared))
	}
}
