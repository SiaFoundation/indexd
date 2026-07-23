package sharing_test

import (
	"errors"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func newTestStore(t testing.TB) testutils.TestStore {
	s := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

func TestManager(t *testing.T) {
	store := newTestStore(t)
	m, err := sharing.NewManager(store, sharing.WithLogger(zaptest.NewLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	appKey := types.GeneratePrivateKey()
	acc := proto.Account(appKey.PublicKey())
	store.AddTestAccount(t, appKey.PublicKey())

	// the sharing key is derived from the app key and a random nonce
	nonce := (sharing.Nonce)(frand.Bytes(32))
	shareKey := sharing.DeriveSharingKey(appKey, nonce)
	pk := shareKey.PublicKey()

	if _, err := m.SharingKey(pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}

	req := sharing.KeyRequest{
		PublicKey:   pk,
		Nonce:       nonce,
		Description: "share",
	}
	if _, err := m.AddSharingKey(acc, req); !errors.Is(err, sharing.ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for unsigned sharing key, got %v", err)
	}
	req.Sign(shareKey)

	key, err := m.AddSharingKey(acc, req)
	if err != nil {
		t.Fatal(err)
	} else if key.PublicKey != pk || key.Description != "share" || key.Account != appKey.PublicKey() {
		t.Fatalf("unexpected key: %+v", key)
	}

	got, err := m.SharingKey(pk)
	if err != nil {
		t.Fatal(err)
	} else if got.PublicKey != pk {
		t.Fatalf("expected public key %v, got %v", pk, got.PublicKey)
	} else if got.Nonce != nonce {
		t.Fatalf("expected stored nonce %x, got %x", nonce, got.Nonce)
	}

	if got, err := m.OwnedSharingKey(acc, pk); err != nil {
		t.Fatal(err)
	} else if got.PublicKey != pk {
		t.Fatalf("expected public key %v, got %v", pk, got.PublicKey)
	}
	otherAccount := proto.Account(types.GeneratePrivateKey().PublicKey())
	if _, err := m.OwnedSharingKey(otherAccount, pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound for another account, got %v", err)
	}

	// the owner can re-derive the same sharing key from the app key and the
	// stored nonce
	if rederived := sharing.DeriveSharingKey(appKey, got.Nonce); rederived.PublicKey() != pk {
		t.Fatalf("re-derived sharing key mismatch: got %v, want %v", rederived.PublicKey(), pk)
	}

	if keys, err := m.SharingKeys(acc, 0, 10); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 || keys[0].PublicKey != pk {
		t.Fatalf("unexpected keys: %+v", keys)
	}

	// a shared object with an invalid signature is rejected before the store
	shareKeyPriv := types.GeneratePrivateKey()
	sharedReq := sharing.SharedObjectRequest{
		ObjectID:         types.Hash256(frand.Entropy256()),
		EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize),
	}
	if err := m.AddSharedObject(acc, shareKeyPriv.PublicKey(), sharedReq); !errors.Is(err, sharing.ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}

	// once signed, attaching to a sharing key the account does not own fails
	sharedReq.Sign(shareKeyPriv)
	if err := m.AddSharedObject(acc, shareKeyPriv.PublicKey(), sharedReq); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound, got %v", err)
	}
	if err := m.DeleteSharedObject(acc, pk, types.Hash256(frand.Entropy256())); !errors.Is(err, sharing.ErrSharedObjectNotFound) {
		t.Fatalf("expected ErrSharedObjectNotFound, got %v", err)
	}

	if err := m.DeleteSharingKey(acc, pk); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SharingKey(pk); !errors.Is(err, sharing.ErrSharingKeyNotFound) {
		t.Fatalf("expected ErrSharingKeyNotFound after delete, got %v", err)
	}
}
