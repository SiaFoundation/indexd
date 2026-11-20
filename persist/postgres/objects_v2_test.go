package postgres

import (
	"bytes"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestMixedObjectIDs(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	sk := types.GeneratePrivateKey()
	acc := proto.Account(sk.PublicKey())
	store.addTestAccount(t, sk.PublicKey())
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)
	params := newTestPinParams(3, hk)
	store.pinTestSlabs(t, acc, params...)
	spaceKey := store.addTestSpace(t, acc)
	shareKey := types.GeneratePrivateKey()
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{PublicKey: shareKey.PublicKey(), Nonce: sharing.Nonce(frand.Entropy256()), Description: "mixed versions"}); err != nil {
		t.Fatal(err)
	}

	objects := make(map[slabs.ObjectID]slabs.SealedObject)
	for _, v2 := range []bool{false, true} {
		obj := slabs.SealedObject{
			EncryptedDataKey:     frand.Bytes(slabs.EncryptionKeySize),
			EncryptedMetadataKey: frand.Bytes(slabs.EncryptionKeySize),
			EncryptedMetadata:    []byte("metadata"),
			Slabs:                []slabs.SlabSlice{params[0].Slice(7, 11), params[1].Slice(13, 23), params[2].Slice(31, 47)},
		}
		id := slabs.ObjectDigest(obj.Slabs)
		if v2 {
			id = obj.ID()
		}
		obj.DataSignature = sk.SignHash(id.DataSigHash(obj.EncryptedDataKey))
		obj.MetadataSignature = sk.SignHash(id.MetaSigHash(obj.EncryptedMetadataKey, obj.EncryptedMetadata))
		req := obj.PinRequestForID(id)
		if err := req.Validate(sk.PublicKey()); err != nil {
			t.Fatal(err)
		}
		if err := store.PinObject(acc, req); err != nil {
			t.Fatal(err)
		}
		objects[id] = obj

		got, err := store.Object(acc, id)
		if err != nil {
			t.Fatal(err)
		}
		if resolvedObjectID(t, got, sk.PublicKey()) != id {
			t.Fatal("object read lost its signed identity")
		}
		shared := sharing.SharedObjectRequest{
			ObjectID:             id,
			EncryptedDataKey:     frand.Bytes(slabs.EncryptionKeySize),
			EncryptedMetadataKey: frand.Bytes(slabs.EncryptionKeySize),
			EncryptedMetadata:    []byte("shared"),
		}
		shared.Sign(shareKey)
		if err := store.AddSharedObject(acc, shareKey.PublicKey(), shared); err != nil {
			t.Fatal(err)
		}
		got.EncryptedMetadataKey = frand.Bytes(slabs.EncryptionKeySize)
		got.EncryptedMetadata = []byte("updated")
		got.DataSignature = sk.SignHash(id.DataSigHash(got.EncryptedDataKey))
		got.MetadataSignature = sk.SignHash(id.MetaSigHash(got.EncryptedMetadataKey, got.EncryptedMetadata))
		if err := store.PinObject(acc, got.PinRequestForID(id)); err != nil {
			t.Fatal(err)
		}
		if updated, err := store.Object(acc, id); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(updated.EncryptedMetadataKey, got.EncryptedMetadataKey) || !bytes.Equal(updated.EncryptedMetadata, got.EncryptedMetadata) {
			t.Fatal("metadata update was not saved")
		}
		got, err = store.SharingKeyObject(shareKey.PublicKey(), id)
		if err != nil {
			t.Fatal(err)
		}
		if resolvedObjectID(t, got, shareKey.PublicKey()) != id || !bytes.Equal(got.EncryptedMetadataKey, shared.EncryptedMetadataKey) || !bytes.Equal(got.EncryptedMetadata, shared.EncryptedMetadata) {
			t.Fatal("metadata update changed the shared object")
		}

		obj.DataSignature = spaceKey.SignHash(id.DataSigHash(obj.EncryptedDataKey))
		obj.MetadataSignature = spaceKey.SignHash(id.MetaSigHash(obj.EncryptedMetadataKey, obj.EncryptedMetadata))
		if err := store.PinSpaceObject(acc, spaceKey.PublicKey(), obj.PinRequestForID(id)); err != nil {
			t.Fatal(err)
		}
		got, err = store.SpaceObject(acc, spaceKey.PublicKey(), id)
		if err != nil {
			t.Fatal(err)
		}
		if resolvedObjectID(t, got, spaceKey.PublicKey()) != id {
			t.Fatal("space read lost its signed identity")
		}
	}
	store.publishEvents(t)

	// Both versions coexist, including in paginated event streams.
	for _, space := range []bool{false, true} {
		cursor := slabs.Cursor{}
		seen := make(map[slabs.ObjectID]bool)
		for {
			var events []slabs.ObjectEventWithoutSlabs
			var err error
			pk := sk.PublicKey()
			if space {
				pk = spaceKey.PublicKey()
				events, err = store.ListSpaceObjectsWithoutSlabs(acc, pk, cursor, 1)
			} else {
				events, err = store.ListObjectsWithoutSlabs(acc, cursor, 1)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(events) == 0 {
				break
			}
			event := events[0]
			obj, ok := objects[event.Key]
			if !ok || seen[event.Key] || event.Object == nil {
				t.Fatalf("invalid mixed-version event: %+v", event)
			}
			if got := resolvedObjectID(t, *event.Object.WithSlabs(obj.Slabs), pk); got != event.Key {
				t.Fatal("event changed signed identity")
			}
			seen[event.Key] = true
			cursor = slabs.Cursor{After: event.UpdatedAt, Key: event.Key}
		}
		if len(seen) != 2 {
			t.Fatalf("expected both versions, got %d objects", len(seen))
		}
	}
	shared, err := store.SharedObjects(shareKey.PublicKey(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(shared) != 2 {
		t.Fatalf("expected two shared objects, got %d", len(shared))
	}
	for _, obj := range shared {
		if _, ok := objects[resolvedObjectID(t, obj, shareKey.PublicKey())]; !ok {
			t.Fatal("unknown shared object ID")
		}
		if err := obj.VerifySignatures(shareKey.PublicKey()); err != nil {
			t.Fatal(err)
		}
	}

	for id := range objects {
		if err := store.DeleteSpaceObject(acc, spaceKey.PublicKey(), id); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteObject(acc, id); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Object(acc, id); !errors.Is(err, slabs.ErrObjectNotFound) {
			t.Fatalf("expected deleted object, got %v", err)
		}
	}
	store.publishEvents(t)
	for _, space := range []bool{false, true} {
		var events []slabs.ObjectEvent
		if space {
			events, err = store.ListSpaceObjects(acc, spaceKey.PublicKey(), slabs.Cursor{}, 10)
		} else {
			events, err = store.ListObjects(acc, slabs.Cursor{}, 10)
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 {
			t.Fatalf("expected two tombstones, got %d", len(events))
		}
		for _, event := range events {
			if _, ok := objects[event.Key]; !ok || !event.Deleted || event.Object != nil || event.UpdatedAt == (time.Time{}) {
				t.Fatalf("invalid tombstone: %+v", event)
			}
		}
	}
}

func resolvedObjectID(t testing.TB, obj slabs.SealedObject, pk types.PublicKey) slabs.ObjectID {
	t.Helper()
	id, err := obj.SignedID(pk)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
