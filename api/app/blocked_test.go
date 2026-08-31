package app_test

import (
	"net/http"
	"testing"
	"time"

	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// TestBlockedObjects blocks an object through the admin API and asserts it is
// hidden from every listing and can not be fetched, pinned or shared until it
// is unblocked.
func TestBlockedObjects(t *testing.T) {
	ctx := t.Context()

	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithHosts(14), testutils.WithLogger(logger))
	indexer := cluster.Indexer
	appClient, adminClient := indexer.App, indexer.Admin

	hc := client.New(client.NewProvider(hosts.NewHostStore(indexer.Store())), logger)
	defer hc.Close()

	cluster.WaitForContracts(t)

	hostList, err := adminClient.Hosts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sk, _ := newAccount(t, cluster)

	// pin two objects so hiding one is distinguishable from a broken listing
	pin := func() slabs.SealedObject {
		t.Helper()
		params := uploadRandomSlab(t, hc, sk, hostList)
		if _, err := appClient.PinSlabs(ctx, sk, params); err != nil {
			t.Fatal(err)
		}
		obj := slabs.SealedObject{
			EncryptedDataKey:     frand.Bytes(sharing.EncryptionKeySize),
			EncryptedMetadataKey: frand.Bytes(sharing.EncryptionKeySize),
			Slabs:                []slabs.SlabSlice{params.Slice(0, 256)},
		}
		obj.Sign(sk)
		if err := appClient.PinObject(ctx, sk, obj); err != nil {
			t.Fatal(err)
		}
		return obj
	}
	blockedObj, otherObj := pin(), pin()

	// attach both to a sharing key
	nonce := (sharing.Nonce)(frand.Bytes(32))
	shareKeyPriv := sharing.DeriveSharingKey(sk, nonce)
	shareKey := shareKeyPriv.PublicKey()
	keyReq := sharing.KeyRequest{PublicKey: shareKey, Nonce: nonce, Description: "share"}
	keyReq.Sign(shareKeyPriv)
	if _, err := appClient.AddSharingKey(ctx, sk, keyReq); err != nil {
		t.Fatal(err)
	}
	attach := func(obj slabs.SealedObject) sharing.SharedObjectRequest {
		t.Helper()
		req := sharing.SharedObjectRequest{
			ObjectID:             obj.ID(),
			EncryptedDataKey:     frand.Bytes(sharing.EncryptionKeySize),
			EncryptedMetadataKey: frand.Bytes(sharing.EncryptionKeySize),
		}
		req.Sign(shareKeyPriv)
		if err := appClient.AddSharedObject(ctx, sk, shareKey, req); err != nil {
			t.Fatal(err)
		}
		return req
	}
	blockedReq := attach(blockedObj)
	attach(otherObj)

	// a URL signed before the block must not resolve afterwards
	shareURL, err := appClient.CreateSharedObjectURL(ctx, sk, blockedObj.ID(), frand.Bytes(32), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := appClient.SharedObject(ctx, shareURL); err != nil {
		t.Fatal(err)
	}

	assertVisible := func(want int) {
		t.Helper()
		publishEvents(t, indexer)
		if events, err := appClient.ListObjects(ctx, sk, slabs.Cursor{}, 100); err != nil {
			t.Fatal(err)
		} else if len(events) != want {
			t.Fatalf("expected %d objects, got %d", want, len(events))
		}
		if objs, err := appClient.SharingKeyObjects(ctx, sk, shareKey); err != nil {
			t.Fatal(err)
		} else if len(objs) != want {
			t.Fatalf("expected %d objects on the sharing key, got %d", want, len(objs))
		}
		if objs, err := appClient.SharedObjects(ctx, shareKeyPriv); err != nil {
			t.Fatal(err)
		} else if len(objs) != want {
			t.Fatalf("expected %d shared objects, got %d", want, len(objs))
		}
	}
	assertVisible(2)

	// the sharing key's totals still count every attached object
	stats, err := appClient.SharedStats(ctx, shareKeyPriv)
	if err != nil {
		t.Fatal(err)
	}

	if err := adminClient.ObjectBlocklistAdd(ctx, blockedObj.ID(), "dmca-1"); err != nil {
		t.Fatal(err)
	}

	// the blocked object is hidden everywhere it would be listed
	assertVisible(1)
	if got, err := appClient.SharedStats(ctx, shareKeyPriv); err != nil {
		t.Fatal(err)
	} else if got.ObjectCount != stats.ObjectCount {
		t.Fatalf("expected sharing key stats to be unchanged, got %d objects want %d", got.ObjectCount, stats.ObjectCount)
	}

	if _, err := appClient.Object(ctx, sk, blockedObj.ID()); err == nil {
		t.Fatal("expected error fetching a blocked object")
	} else {
		assertStatus(t, err, http.StatusUnavailableForLegalReasons)
	}
	if _, err := appClient.SharedObjectByID(ctx, shareKeyPriv, blockedObj.ID()); err == nil {
		t.Fatal("expected error fetching a blocked shared object")
	} else {
		assertStatus(t, err, http.StatusUnavailableForLegalReasons)
	}
	if _, _, err := appClient.SharedObject(ctx, shareURL); err == nil {
		t.Fatal("expected error fetching a blocked object by shared URL")
	} else {
		assertStatus(t, err, http.StatusUnavailableForLegalReasons)
	}

	// the key is derived from the object's slabs, so the same content can not be
	// pinned again by any account
	if err := appClient.PinObject(ctx, sk, blockedObj); err == nil {
		t.Fatal("expected error re-pinning a blocked object")
	} else {
		assertStatus(t, err, http.StatusUnavailableForLegalReasons)
	}
	sk2, _ := newAccount(t, cluster)
	otherAccountObj := blockedObj
	otherAccountObj.Sign(sk2)
	if err := appClient.PinObject(ctx, sk2, otherAccountObj); err == nil {
		t.Fatal("expected error pinning a blocked object from another account")
	} else {
		assertStatus(t, err, http.StatusUnavailableForLegalReasons)
	}

	otherNonce := (sharing.Nonce)(frand.Bytes(32))
	otherKeyPriv := sharing.DeriveSharingKey(sk, otherNonce)
	otherKeyReq := sharing.KeyRequest{PublicKey: otherKeyPriv.PublicKey(), Nonce: otherNonce}
	otherKeyReq.Sign(otherKeyPriv)
	if _, err := appClient.AddSharingKey(ctx, sk, otherKeyReq); err != nil {
		t.Fatal(err)
	}
	reattach := blockedReq
	reattach.Sign(otherKeyPriv)
	if err := appClient.AddSharedObject(ctx, sk, otherKeyPriv.PublicKey(), reattach); err == nil {
		t.Fatal("expected error attaching a blocked object")
	} else {
		assertStatus(t, err, http.StatusUnavailableForLegalReasons)
	}

	if entry, err := adminClient.ObjectBlocklistEntry(ctx, blockedObj.ID()); err != nil {
		t.Fatal(err)
	} else if entry.Key != blockedObj.ID() || entry.Reason != "dmca-1" {
		t.Fatalf("unexpected blocklist entry: %+v", entry)
	}
	if list, err := adminClient.ObjectsBlocklist(ctx); err != nil {
		t.Fatal(err)
	} else if len(list) != 1 || list[0].Key != blockedObj.ID() {
		t.Fatalf("unexpected blocklist: %+v", list)
	}
	if _, err := adminClient.ObjectBlocklistEntry(ctx, otherObj.ID()); err == nil {
		t.Fatal("expected error fetching an unblocked object")
	}

	// unblocking restores access
	if err := adminClient.ObjectBlocklistRemove(ctx, blockedObj.ID()); err != nil {
		t.Fatal(err)
	}
	assertVisible(2)
	if got, err := appClient.Object(ctx, sk, blockedObj.ID()); err != nil {
		t.Fatal(err)
	} else if got.ID() != blockedObj.ID() {
		t.Fatalf("expected object %v, got %v", blockedObj.ID(), got.ID())
	}
	if _, _, err := appClient.SharedObject(ctx, shareURL); err != nil {
		t.Fatal(err)
	}

	// DELETE is idempotent, so unblocking an unblocked object is a no-op
	if err := adminClient.ObjectBlocklistRemove(ctx, blockedObj.ID()); err != nil {
		t.Fatalf("expected unblocking an unblocked object to be a no-op, got %v", err)
	}
}
