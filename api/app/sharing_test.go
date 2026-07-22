package app_test

import (
	"errors"
	"net/http"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func assertStatus(t *testing.T, err error, status int) {
	t.Helper()
	var he *app.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *app.HTTPError, got %v", err)
	} else if he.StatusCode != status {
		t.Fatalf("expected status %d, got %d (%s)", status, he.StatusCode, he.Body)
	}
}

func TestSharingKeys(t *testing.T) {
	ctx := t.Context()

	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithHosts(14), testutils.WithLogger(logger))
	indexer := cluster.Indexer

	hc := client.New(client.NewProvider(hosts.NewHostStore(indexer.Store())), logger)
	defer hc.Close()

	cluster.WaitForContracts(t)

	hostList, err := indexer.Admin.Hosts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sk1, _ := newAccount(t, cluster)
	sk2, _ := newAccount(t, cluster)
	appClient := indexer.App

	slabParams := uploadRandomSlab(t, hc, sk1, hostList)
	if _, err := appClient.PinSlabs(ctx, sk1, slabParams); err != nil {
		t.Fatal(err)
	}
	obj := slabs.SealedObject{
		EncryptedDataKey:     frand.Bytes(sharing.EncryptionKeySize),
		EncryptedMetadataKey: frand.Bytes(sharing.EncryptionKeySize),
		Slabs:                []slabs.SlabSlice{slabParams.Slice(0, 256)},
	}
	obj.Sign(sk1)
	if err := appClient.PinObject(ctx, sk1, obj); err != nil {
		t.Fatal(err)
	}

	nonce := (sharing.Nonce)(frand.Bytes(32))
	shareKeyPriv := sharing.DeriveSharingKey(sk1, nonce)
	shareKey := shareKeyPriv.PublicKey()
	req := sharing.KeyRequest{
		PublicKey:   shareKey,
		Nonce:       nonce,
		Description: "my share",
	}
	key, err := appClient.AddSharingKey(ctx, sk1, req)
	if err != nil {
		t.Fatal(err)
	} else if key.PublicKey != shareKey || key.Account != sk1.PublicKey() || key.Description != "my share" {
		t.Fatalf("unexpected key: %+v", key)
	}

	if _, err := appClient.AddSharingKey(ctx, sk1, req); err == nil {
		t.Fatal("expected error creating duplicate sharing key")
	} else {
		assertStatus(t, err, http.StatusConflict)
	}

	bad := req
	bad.PublicKey = types.GeneratePrivateKey().PublicKey()
	bad.Nonce = sharing.Nonce{}
	if _, err := appClient.AddSharingKey(ctx, sk1, bad); err == nil {
		t.Fatal("expected error for empty nonce")
	} else {
		assertStatus(t, err, http.StatusBadRequest)
	}

	if got, err := appClient.SharingKey(ctx, sk1, shareKey); err != nil {
		t.Fatal(err)
	} else if got.PublicKey != shareKey {
		t.Fatalf("unexpected key: %+v", got)
	}
	if keys, err := appClient.SharingKeys(ctx, sk1); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 || keys[0].PublicKey != shareKey {
		t.Fatalf("unexpected keys: %+v", keys)
	}

	if _, err := appClient.SharingKey(ctx, sk2, shareKey); err == nil {
		t.Fatal("expected error fetching another account's key")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}
	if keys, err := appClient.SharingKeys(ctx, sk2); err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatalf("expected 0 keys for sk2, got %d", len(keys))
	}

	sharedReq := sharing.SharedObjectRequest{
		ObjectID:             obj.ID(),
		EncryptedDataKey:     frand.Bytes(sharing.EncryptionKeySize),
		EncryptedMetadataKey: frand.Bytes(sharing.EncryptionKeySize),
		EncryptedMetadata:    frand.Bytes(32),
	}
	sharedReq.Sign(shareKeyPriv)
	if err := appClient.AddSharedObject(ctx, sk1, shareKey, sharedReq); err != nil {
		t.Fatal(err)
	}

	if key, err := appClient.SharingKey(ctx, sk1, shareKey); err != nil {
		t.Fatal(err)
	} else if key.ObjectCount != 1 || key.ObjectSize != 256 ||
		key.PinnedData != 4*uint64(proto.SectorSize) ||
		key.PinnedSize != uint64(len(hostList))*uint64(proto.SectorSize) {
		t.Fatalf("unexpected totals: %+v", key)
	}

	if objs, err := appClient.SharedObjects(ctx, sk1, shareKey); err != nil {
		t.Fatal(err)
	} else if len(objs) != 1 || objs[0].ID() != obj.ID() {
		t.Fatalf("unexpected shared objects: %v", objs)
	}

	if objs, err := appClient.SharedObjects(ctx, sk1, shareKey, api.WithOffset(1)); err != nil {
		t.Fatal(err)
	} else if len(objs) != 0 {
		t.Fatalf("expected no objects at offset 1, got %d", len(objs))
	}

	if _, err := appClient.SharedObjects(ctx, sk1, types.GeneratePrivateKey().PublicKey()); err == nil {
		t.Fatal("expected error listing objects of unknown key")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}

	if _, err := appClient.SharedObjects(ctx, sk2, shareKey); err == nil {
		t.Fatal("expected error listing another account's key objects")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}

	forged := sharedReq
	forged.DataSignature = types.Signature{}
	if err := appClient.AddSharedObject(ctx, sk1, shareKey, forged); err == nil {
		t.Fatal("expected error for invalid signature")
	} else {
		assertStatus(t, err, http.StatusBadRequest)
	}

	oversized := sharedReq
	oversized.EncryptedMetadata = frand.Bytes(sharing.MaxMetadataSize + 1)
	oversized.Sign(shareKeyPriv)
	if err := appClient.AddSharedObject(ctx, sk1, shareKey, oversized); err == nil {
		t.Fatal("expected error for oversized metadata")
	} else {
		assertStatus(t, err, http.StatusBadRequest)
	}

	unknown := sharedReq
	unknown.ObjectID = types.Hash256(frand.Entropy256())
	unknown.Sign(shareKeyPriv)
	if err := appClient.AddSharedObject(ctx, sk1, shareKey, unknown); err == nil {
		t.Fatal("expected error attaching unknown object")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}

	if err := appClient.AddSharedObject(ctx, sk2, shareKey, sharedReq); err == nil {
		t.Fatal("expected error attaching to another account's key")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}

	if err := appClient.DeleteSharedObject(ctx, sk1, shareKey, obj.ID()); err != nil {
		t.Fatal(err)
	}
	if err := appClient.DeleteSharedObject(ctx, sk1, shareKey, obj.ID()); err == nil {
		t.Fatal("expected error detaching already-detached object")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}

	if err := appClient.DeleteSharingKey(ctx, sk1, shareKey); err != nil {
		t.Fatal(err)
	}
	if _, err := appClient.SharingKey(ctx, sk1, shareKey); err == nil {
		t.Fatal("expected error fetching deleted key")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}
	if err := appClient.DeleteSharingKey(ctx, sk1, shareKey); err == nil {
		t.Fatal("expected error deleting already-deleted key")
	} else {
		assertStatus(t, err, http.StatusNotFound)
	}
}
