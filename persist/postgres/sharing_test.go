package postgres

import (
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

func (s *Store) pinTestObject(t testing.TB, acc proto.Account, hk types.PublicKey) slabs.SealedObject {
	t.Helper()
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors:       []slabs.PinnedSector{{Root: frand.Entropy256(), HostKey: hk}},
	}
	if _, err := s.PinSlabs(acc, time.Time{}, params); err != nil {
		t.Fatalf("failed to pin slabs: %v", err)
	}
	obj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(72),
		EncryptedMetadataKey: frand.Bytes(72),
		EncryptedMetadata:    frand.Bytes(50),
		DataSignature:        types.Signature(frand.Bytes(64)),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
		Slabs:                []slabs.SlabSlice{params.Slice(0, 100)},
	}
	if err := s.PinObject(acc, obj.PinRequest()); err != nil {
		t.Fatalf("failed to pin object: %v", err)
	}
	return obj
}

func TestSharingKeys(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))

	pk := types.GeneratePrivateKey().PublicKey()
	if _, err := store.SharingKey(pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}

	nonce := (sharing.Nonce)(frand.Bytes(32))
	expires := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	req := sharing.KeyRequest{
		PublicKey:   pk,
		Nonce:       nonce,
		Description: "my share",
		ExpiresAt:   &expires,
	}

	key, err := store.AddSharingKey(acc, req)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case key.PublicKey != pk:
		t.Fatalf("expected public key %v, got %v", pk, key.PublicKey)
	case key.Account != types.PublicKey(acc):
		t.Fatalf("expected account %v, got %v", types.PublicKey(acc), key.Account)
	case key.Nonce != nonce:
		t.Fatalf("expected nonce %x, got %x", nonce, key.Nonce)
	case key.Description != "my share":
		t.Fatalf("expected description %q, got %q", "my share", key.Description)
	case key.ExpiresAt == nil || !key.ExpiresAt.Equal(expires):
		t.Fatalf("expected expiry %v, got %v", expires, key.ExpiresAt)
	case key.CreatedAt.IsZero() || key.UpdatedAt.IsZero():
		t.Fatalf("expected non-zero timestamps, got %v and %v", key.CreatedAt, key.UpdatedAt)
	}

	// adding the same public key again fails
	if _, err := store.AddSharingKey(acc, req); !errors.Is(err, sharing.ErrSharingKeyExists) {
		t.Fatalf("expected ErrSharingKeyExists, got %v", err)
	}

	// reusing the nonce with a different public key is a domain conflict, not a raw db error
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   types.GeneratePrivateKey().PublicKey(),
		Nonce:       nonce,
		Description: "duplicate nonce",
	}); !errors.Is(err, sharing.ErrSharingKeyExists) {
		t.Fatalf("expected ErrSharingKeyExists for duplicate nonce, got %v", err)
	}

	// fetch by public key
	got, err := store.SharingKey(pk)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case got.PublicKey != key.PublicKey || got.Account != key.Account:
		t.Fatalf("fetched key mismatch: %+v vs %+v", got, key)
	case got.Nonce != key.Nonce || got.Description != key.Description:
		t.Fatalf("fetched key mismatch: %+v vs %+v", got, key)
	case !got.ExpiresAt.Equal(*key.ExpiresAt) || !got.CreatedAt.Equal(key.CreatedAt):
		t.Fatalf("fetched key mismatch: %+v vs %+v", got, key)
	}

	// add a second key without an expiration
	pk2 := types.GeneratePrivateKey().PublicKey()
	key2, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   pk2,
		Nonce:       (sharing.Nonce)(frand.Bytes(32)),
		Description: "second",
	})
	if err != nil {
		t.Fatal(err)
	} else if key2.ExpiresAt != nil {
		t.Fatalf("expected nil expiry, got %v", key2.ExpiresAt)
	}

	// list, newest first
	keys, err := store.SharingKeys(acc, 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	} else if keys[0].PublicKey != pk2 || keys[1].PublicKey != pk {
		t.Fatalf("unexpected key order: %v, %v", keys[0].PublicKey, keys[1].PublicKey)
	}

	// pagination
	if page, err := store.SharingKeys(acc, 0, 1); err != nil {
		t.Fatal(err)
	} else if len(page) != 1 || page[0].PublicKey != pk2 {
		t.Fatalf("unexpected first page: %+v", page)
	}
	if page, err := store.SharingKeys(acc, 1, 1); err != nil {
		t.Fatal(err)
	} else if len(page) != 1 || page[0].PublicKey != pk {
		t.Fatalf("unexpected second page: %+v", page)
	}

	// another account only sees its own keys
	acc2 := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc2))
	if keys, err := store.SharingKeys(acc2, 0, 10); err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatalf("expected 0 keys for other account, got %d", len(keys))
	}

	// another account cannot delete this account's key
	if err := store.DeleteSharingKey(acc2, pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}

	if err := store.DeleteSharingKey(acc, pk); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SharingKey(pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound after delete, got %v", err)
	}
	// deleting again fails
	if err := store.DeleteSharingKey(acc, pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}
}

func TestPruneExpiredSharingKeys(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))

	add := func(desc string, expiresAt *time.Time) types.PublicKey {
		pk := types.GeneratePrivateKey().PublicKey()
		if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
			PublicKey:   pk,
			Nonce:       (sharing.Nonce)(frand.Bytes(32)),
			Description: desc,
			ExpiresAt:   expiresAt,
		}); err != nil {
			t.Fatal(err)
		}
		return pk
	}

	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	expiredPK := add("expired", &past)
	futurePK := add("future", &future)
	neverPK := add("never", nil)

	// an expired key is not returned even before it is pruned
	if _, err := store.SharingKey(expiredPK); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected expired key to be filtered, got %v", err)
	}

	if err := store.PruneExpiredSharingKeys(time.Now()); err != nil {
		t.Fatal(err)
	}

	// prune physically deletes the expired row; the others remain
	count := func(pk types.PublicKey) (n int) {
		if err := store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM sharing_keys WHERE public_key = $1`, sqlPublicKey(pk)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return
	}
	if count(expiredPK) != 0 {
		t.Fatal("expected expired key to be pruned")
	} else if count(futurePK) != 1 {
		t.Fatal("expected future key to survive")
	} else if count(neverPK) != 1 {
		t.Fatal("expected non-expiring key to survive")
	}
}

func TestSharingKeyDeletedAccount(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))

	pk := types.GeneratePrivateKey().PublicKey()
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   pk,
		Nonce:       (sharing.Nonce)(frand.Bytes(32)),
		Description: "share",
	}); err != nil {
		t.Fatal(err)
	}

	// the key is reachable while the account is active
	if _, err := store.SharingKey(pk); err != nil {
		t.Fatal(err)
	}

	// soft-deleting the account hides the key from recipient reads before pruning
	if err := store.DeleteAccount(acc); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SharingKey(pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound for soft-deleted account, got %v", err)
	}
	if _, err := store.SharedObjects(pk, 0, 10); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound for soft-deleted account, got %v", err)
	}
}

func TestSharedObjectRequest(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	obj := store.pinTestObject(t, acc, hk)

	skPK := types.GeneratePrivateKey().PublicKey()
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   skPK,
		Nonce:       (sharing.Nonce)(frand.Bytes(32)),
		Description: "share",
	}); err != nil {
		t.Fatal(err)
	}

	req := sharing.SharedObjectRequest{
		ObjectID:             obj.ID(),
		EncryptedDataKey:     frand.Bytes(72),
		DataSignature:        types.Signature(frand.Bytes(64)),
		EncryptedMetadataKey: frand.Bytes(72),
		EncryptedMetadata:    frand.Bytes(40),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
	}

	if err := store.AddSharedObject(acc, types.GeneratePrivateKey().PublicKey(), req); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}

	// attaching to an expired key is rejected
	expiredPK := types.GeneratePrivateKey().PublicKey()
	past := time.Now().Add(-time.Hour)
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   expiredPK,
		Nonce:       (sharing.Nonce)(frand.Bytes(32)),
		Description: "expired",
		ExpiresAt:   &past,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.AddSharedObject(acc, expiredPK, req); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound attaching to expired key, got %v", err)
	}

	badReq := req
	badReq.ObjectID = types.Hash256(frand.Entropy256())
	if err := store.AddSharedObject(acc, skPK, badReq); !errors.Is(err, slabs.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	if err := store.AddSharedObject(acc, skPK, req); err != nil {
		t.Fatal(err)
	}
	if n := countSharedObjects(t, store); n != 1 {
		t.Fatalf("expected 1 shared object, got %d", n)
	}

	// re-attaching overwrites (upsert) rather than duplicating
	req2 := req
	req2.EncryptedDataKey = frand.Bytes(72)
	req2.DataSignature = types.Signature(frand.Bytes(64))
	if err := store.AddSharedObject(acc, skPK, req2); err != nil {
		t.Fatal(err)
	}
	if n := countSharedObjects(t, store); n != 1 {
		t.Fatalf("expected 1 shared object after re-attach, got %d", n)
	}

	skPK2 := types.GeneratePrivateKey().PublicKey()
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   skPK2,
		Nonce:       (sharing.Nonce)(frand.Bytes(32)),
		Description: "second share",
	}); err != nil {
		t.Fatal(err)
	} else if err := store.AddSharedObject(acc, skPK2, req2); !errors.Is(err, sharing.ErrSharedObjectConflict) {
		t.Fatalf("expected ErrSharedObjectConflict for reused sealed keys, got %v", err)
	}
	if n := countSharedObjects(t, store); n != 1 {
		t.Fatalf("expected 1 shared object after conflict, got %d", n)
	}

	// another account can neither reach the sharing key nor detach
	acc2 := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc2))
	if err := store.AddSharedObject(acc2, skPK, req); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}
	if err := store.DeleteSharedObject(acc2, skPK, obj.ID()); !errors.Is(err, sharing.ErrSharedObjectNotFound) {
		t.Fatalf("expected ErrSharedObjectNotFound, got %v", err)
	}

	if err := store.DeleteSharedObject(acc, skPK, obj.ID()); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSharedObject(acc, skPK, obj.ID()); !errors.Is(err, sharing.ErrSharedObjectNotFound) {
		t.Fatalf("expected ErrSharedObjectNotFound after detach, got %v", err)
	}

	// deleting the object removes it from the sharing key via cascade
	if err := store.AddSharedObject(acc, skPK, req); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(acc, obj.ID()); err != nil {
		t.Fatal(err)
	}
	if n := countSharedObjects(t, store); n != 0 {
		t.Fatalf("expected shared objects to be cascade-deleted, got %d", n)
	}
}

func countSharedObjects(t testing.TB, store *Store) (n int) {
	t.Helper()
	if err := store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM shared_objects`).Scan(&n); err != nil {
		t.Fatalf("failed to count shared objects: %v", err)
	}
	return
}

func TestSharingKeyTotals(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	acc := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(acc))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	obj := store.pinTestObject(t, acc, hk)

	skPK := types.GeneratePrivateKey().PublicKey()
	if _, err := store.AddSharingKey(acc, sharing.KeyRequest{
		PublicKey:   skPK,
		Nonce:       (sharing.Nonce)(frand.Bytes(32)),
		Description: "totals",
	}); err != nil {
		t.Fatal(err)
	}

	assertTotals := func(count, size, pinnedData, pinnedSize uint64) {
		t.Helper()
		key, err := store.SharingKey(skPK)
		if err != nil {
			t.Fatal(err)
		} else if key.ObjectCount != count || key.ObjectSize != size || key.PinnedData != pinnedData || key.PinnedSize != pinnedSize {
			t.Fatalf("unexpected totals: count=%d objectSize=%d pinnedData=%d pinnedSize=%d, want %d/%d/%d/%d",
				key.ObjectCount, key.ObjectSize, key.PinnedData, key.PinnedSize, count, size, pinnedData, pinnedSize)
		}
	}
	resetUpdatedAt := func() time.Time {
		t.Helper()
		updatedAt := time.Unix(1, 0).UTC()
		if _, err := store.pool.Exec(t.Context(), `UPDATE sharing_keys SET updated_at = $1 WHERE public_key = $2`, updatedAt, sqlPublicKey(skPK)); err != nil {
			t.Fatal(err)
		}
		return updatedAt
	}
	assertUpdatedAtBumped := func(previous time.Time) {
		t.Helper()
		key, err := store.SharingKey(skPK)
		if err != nil {
			t.Fatal(err)
		} else if !key.UpdatedAt.After(previous) {
			t.Fatalf("expected updated_at after %v, got %v", previous, key.UpdatedAt)
		}
	}

	assertTotals(0, 0, 0, 0)

	updatedAt := resetUpdatedAt()
	if err := store.AddSharedObject(acc, skPK, sharing.SharedObjectRequest{
		ObjectID:          obj.ID(),
		EncryptedDataKey:  frand.Bytes(sharing.EncryptionKeySize),
		DataSignature:     types.Signature(frand.Bytes(64)),
		MetadataSignature: types.Signature(frand.Bytes(64)),
	}); err != nil {
		t.Fatal(err)
	}
	assertTotals(1, 100, uint64(proto.SectorSize), uint64(proto.SectorSize))
	assertUpdatedAtBumped(updatedAt)

	// re-attaching the same object is an upsert; the totals are unchanged
	updatedAt = resetUpdatedAt()
	if err := store.AddSharedObject(acc, skPK, sharing.SharedObjectRequest{
		ObjectID:          obj.ID(),
		EncryptedDataKey:  frand.Bytes(sharing.EncryptionKeySize),
		DataSignature:     types.Signature(frand.Bytes(64)),
		MetadataSignature: types.Signature(frand.Bytes(64)),
	}); err != nil {
		t.Fatal(err)
	}
	assertTotals(1, 100, uint64(proto.SectorSize), uint64(proto.SectorSize))
	assertUpdatedAtBumped(updatedAt)

	// deleting the object cascades into shared_objects
	updatedAt = resetUpdatedAt()
	if err := store.DeleteObject(acc, obj.ID()); err != nil {
		t.Fatal(err)
	}
	assertTotals(0, 0, 0, 0)
	assertUpdatedAtBumped(updatedAt)
}
