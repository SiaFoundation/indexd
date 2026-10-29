package spaces_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/frand"
)

func sealSpaceTest(t testing.TB, key [32]byte, plaintext []byte) []byte {
	t.Helper()
	cipher, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		t.Fatal(err)
	}
	nonce := frand.Bytes(cipher.NonceSize())
	return cipher.Seal(nonce, nonce, plaintext, nil)
}

func openSpaceTest(t testing.TB, key [32]byte, ciphertext []byte) []byte {
	t.Helper()
	cipher, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		t.Fatal(err)
	} else if len(ciphertext) < cipher.NonceSize() {
		t.Fatal("ciphertext is missing its nonce")
	}
	plaintext, err := cipher.Open(nil, ciphertext[:cipher.NonceSize()], ciphertext[cipher.NonceSize():], nil)
	if err != nil {
		t.Fatal(err)
	}
	return plaintext
}

func TestManagerClientWorkflow(t *testing.T) {
	store := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() { store.Close() })
	m := spaces.NewManager(store)
	ownerKey, writerKey, readerKey := types.GeneratePrivateKey(), types.GeneratePrivateKey(), types.GeneratePrivateKey()
	owner, writer, reader := proto.Account(ownerKey.PublicKey()), proto.Account(writerKey.PublicKey()), proto.Account(readerKey.PublicKey())
	for _, account := range []proto.Account{owner, writer, reader} {
		store.AddTestAccount(t, types.PublicKey(account))
	}
	spaceKey := types.GeneratePrivateKey()
	key, seed := spaceKey.PublicKey(), [32]byte(spaceKey[:32])
	ownerWrappingKey := spaces.DeriveMemberKey(ownerKey, key)
	req := spaces.AddSpaceRequest{
		Key:               key,
		EncryptedKey:      sealSpaceTest(t, ownerWrappingKey, seed[:]),
		EncryptedName:     sealSpaceTest(t, seed, []byte("Shared documents")),
		EncryptedMetadata: sealSpaceTest(t, seed, []byte(`{"color":"blue"}`)),
	}
	req.Sign(spaceKey, owner)
	if err := m.AddSpace(owner, req); err != nil {
		t.Fatal(err)
	}
	created, err := m.Space(owner, key)
	if err != nil {
		t.Fatal(err)
	} else if created.Permission != spaces.Owner || !key.VerifyHash(created.SigHash(), created.Signature) {
		t.Fatalf("invalid owner space: %+v", created)
	} else if recovered := openSpaceTest(t, ownerWrappingKey, created.EncryptedKey); !bytes.Equal(recovered, seed[:]) {
		t.Fatal("owner could not recover the space seed")
	} else if name := openSpaceTest(t, seed, created.EncryptedName); string(name) != "Shared documents" {
		t.Fatalf("unexpected space name %q", name)
	}

	join := func(inviterKey, recipientKey types.PrivateKey, inviterSpaceKey types.PrivateKey, permission spaces.Permission) types.PrivateKey {
		t.Helper()
		inviter, recipient := proto.Account(inviterKey.PublicKey()), proto.Account(recipientKey.PublicKey())
		wrappingKey, err := spaces.DeriveInvitationKey(inviterKey, recipientKey.PublicKey(), key)
		if err != nil {
			t.Fatal(err)
		}
		invite := spaces.Invitation{
			SpaceKey: key, Inviter: inviter, Recipient: recipient, Permission: permission,
			EncryptedKey: sealSpaceTest(t, wrappingKey, inviterSpaceKey[:32]),
			CreatedAt:    time.Now(), ExpiresAt: time.Now().Add(time.Hour),
		}
		invite.Signature = inviterKey.SignHash(invite.SigHash())
		if err := m.InviteSpaceMember(inviter, invite); err != nil {
			t.Fatal(err)
		}
		pending, err := m.SpaceInvitations(recipient, 0, 10)
		if err != nil || len(pending) != 1 {
			t.Fatalf("expected one invitation: %+v, %v", pending, err)
		} else if !types.PublicKey(pending[0].Inviter).VerifyHash(pending[0].SigHash(), pending[0].Signature) {
			t.Fatal("stored invitation signature is invalid")
		}
		recipientWrappingKey, err := spaces.DeriveInvitationKey(recipientKey, types.PublicKey(pending[0].Inviter), pending[0].SpaceKey)
		if err != nil {
			t.Fatal(err)
		}
		recoveredSeed := openSpaceTest(t, recipientWrappingKey, pending[0].EncryptedKey)
		recoveredKey := types.NewPrivateKeyFromSeed(recoveredSeed)
		if recoveredKey.PublicKey() != pending[0].SpaceKey {
			t.Fatal("invitation decrypted to a different space key")
		}
		memberWrappingKey := spaces.DeriveMemberKey(recipientKey, pending[0].SpaceKey)
		acceptedKey := sealSpaceTest(t, memberWrappingKey, recoveredSeed)
		if err := m.AcceptSpaceInvitation(recipient, key, acceptedKey); err != nil {
			t.Fatal(err)
		} else if pending, err := m.SpaceInvitations(recipient, 0, 10); err != nil || len(pending) != 0 {
			t.Fatalf("accepted invitation remained pending: %+v, %v", pending, err)
		}
		listed, err := m.Spaces(recipient, 0, 10)
		if err != nil || len(listed) != 1 {
			t.Fatalf("missing accepted space: %+v, %v", listed, err)
		} else if listed[0].Permission != permission || !key.VerifyHash(listed[0].SigHash(), listed[0].Signature) {
			t.Fatalf("invalid member space: %+v", listed[0])
		}
		storedSeed := openSpaceTest(t, memberWrappingKey, listed[0].EncryptedKey)
		if !bytes.Equal(storedSeed, seed[:]) {
			t.Fatal("accepted space key changed during rewrapping")
		}
		return types.NewPrivateKeyFromSeed(storedSeed)
	}
	writerSpaceKey := join(ownerKey, writerKey, spaceKey, spaces.Write)

	params := slabs.SlabPinParams{Version: 1, MinShards: 3, EncryptionKey: frand.Entropy256()}
	for range 12 {
		hostKey := types.GeneratePrivateKey().PublicKey()
		store.AddTestHost(t, hosts.Host{PublicKey: hostKey})
		if err := store.AddFormedContract(hostKey, types.FileContractID(hostKey), types.V2FileContract{
			HostPublicKey: hostKey, ProofHeight: 600, ExpirationHeight: 800,
		}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
			t.Fatal(err)
		}
		params.Sectors = append(params.Sectors, slabs.PinnedSector{HostKey: hostKey, Root: frand.Entropy256()})
	}
	if ids, err := m.PinSpaceSlabs(writer, key, time.Now().Add(time.Hour), params); err != nil || len(ids) != 1 || ids[0] != params.Digest() {
		t.Fatalf("pinning space slab: %v, %v", ids, err)
	}
	dataKey, metadataKey := frand.Entropy256(), frand.Entropy256()
	writerSeed := [32]byte(writerSpaceKey[:32])
	object := slabs.SealedObject{
		EncryptedDataKey:     sealSpaceTest(t, writerSeed, dataKey[:]),
		EncryptedMetadataKey: sealSpaceTest(t, writerSeed, metadataKey[:]),
		EncryptedMetadata:    sealSpaceTest(t, metadataKey, []byte(`{"name":"notes.txt"}`)),
		Slabs:                []slabs.SlabSlice{params.Slice(0, 32), params.Slice(32, 64)},
	}
	object.Sign(writerKey)
	if err := m.PinSpaceObject(writer, key, object.PinRequest()); err != nil {
		t.Fatal(err)
	} else if err := m.DeleteSpaceObject(writer, key, object.ID()); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("writer deleted an object: %v", err)
	}
	update := spaces.AddSpaceRequest{Key: key, EncryptedName: sealSpaceTest(t, writerSeed, []byte("Team documents")), EncryptedMetadata: req.EncryptedMetadata}
	update.Sign(writerSpaceKey, owner)
	if err := m.UpdateSpace(writer, key, update.EncryptedName, update.EncryptedMetadata, update.Signature); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("writer updated space fields: %v", err)
	} else if err := m.UpdateSpaceMember(owner, key, writer, spaces.Admin); err != nil {
		t.Fatal(err)
	} else if err := m.UpdateSpace(writer, key, update.EncryptedName, update.EncryptedMetadata, update.Signature); err != nil {
		t.Fatal(err)
	}
	readerSpaceKey := join(writerKey, readerKey, writerSpaceKey, spaces.Read)
	readerSeed := [32]byte(readerSpaceKey[:32])
	updated, err := m.Space(reader, key)
	if err != nil {
		t.Fatal(err)
	} else if !key.VerifyHash(updated.SigHash(), updated.Signature) || string(openSpaceTest(t, readerSeed, updated.EncryptedName)) != "Team documents" {
		t.Fatal("reader could not verify and decrypt updated space")
	}
	if members, err := m.SpaceMembers(reader, key, 0, 10); err != nil || len(members) != 3 {
		t.Fatalf("unexpected members: %+v, %v", members, err)
	}
	readerObject := object
	readerObject.Sign(readerKey)
	if err := m.PinSpaceObject(reader, key, readerObject.PinRequest()); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("reader pinned an object: %v", err)
	} else if _, err := m.PinSpaceSlabs(reader, key, time.Now().Add(time.Hour), params); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("reader pinned a slab: %v", err)
	}
	got, err := m.SpaceObject(reader, key, object.ID())
	if err != nil {
		t.Fatal(err)
	} else if got.Account != writer || got.Key != object.ID() || got.VerifySignatures(types.PublicKey(got.Account)) != nil {
		t.Fatal("reader received an invalid object")
	} else if recoveredDataKey := openSpaceTest(t, readerSeed, got.EncryptedDataKey); !bytes.Equal(recoveredDataKey, dataKey[:]) {
		t.Fatal("reader recovered a different data key")
	}
	recoveredMetadataKey := [32]byte(openSpaceTest(t, readerSeed, got.EncryptedMetadataKey))
	if metadata := openSpaceTest(t, recoveredMetadataKey, got.EncryptedMetadata); string(metadata) != `{"name":"notes.txt"}` {
		t.Fatalf("unexpected object metadata %q", metadata)
	}
	for i, want := range object.Slabs {
		if page, err := m.SpaceObjectSlabs(reader, key, object.ID(), int64(i), 1); err != nil || len(page) != 1 || !reflect.DeepEqual(page[0], want) {
			t.Fatalf("unexpected slab page %d: %+v, %v", i, page, err)
		}
	}
	if page, err := m.SpaceObjectSlabs(reader, key, object.ID(), int64(len(object.Slabs)), 1); err != nil || len(page) != 0 {
		t.Fatalf("unexpected final slab page: %+v, %v", page, err)
	}
	publish := func() {
		t.Helper()
		var claimed bool
		if err := store.QueryRow(t.Context(), `SELECT object_events_last_published >= date_trunc('second', NOW()) FROM global_settings`).Scan(&claimed); err != nil {
			t.Fatal(err)
		}
		if claimed {
			time.Sleep(time.Second)
		}
		if err := store.PublishObjectEvents(); err != nil {
			t.Fatal(err)
		}
	}
	publish()
	events, err := m.ListSpaceObjects(reader, key, slabs.Cursor{}, 1)
	if err != nil || len(events) != 1 || events[0].Object == nil || events[0].Key != object.ID() || events[0].Object.VerifySignatures(writerKey.PublicKey()) != nil {
		t.Fatalf("unexpected object event: %+v, %v", events, err)
	}
	cursor := slabs.Cursor{After: events[0].UpdatedAt, Key: events[0].Key}
	if events, err := m.ListSpaceObjects(reader, key, cursor, 1); err != nil || len(events) != 0 {
		t.Fatalf("unexpected events after cursor: %+v, %v", events, err)
	}
	object.EncryptedMetadata = sealSpaceTest(t, metadataKey, []byte(`{"name":"renamed.txt"}`))
	object.Sign(writerKey)
	if err := m.PinSpaceObject(writer, key, object.PinRequest()); err != nil {
		t.Fatal(err)
	}
	publish()
	withoutSlabs, err := m.ListSpaceObjectsWithoutSlabs(reader, key, cursor, 1)
	if err != nil || len(withoutSlabs) != 1 || withoutSlabs[0].Object == nil || withoutSlabs[0].Object.Size != object.Size() {
		t.Fatalf("unexpected updated object event: %+v, %v", withoutSlabs, err)
	} else if metadata := openSpaceTest(t, recoveredMetadataKey, withoutSlabs[0].Object.EncryptedMetadata); string(metadata) != `{"name":"renamed.txt"}` {
		t.Fatalf("unexpected updated metadata %q", metadata)
	}
	cursor = slabs.Cursor{After: withoutSlabs[0].UpdatedAt, Key: withoutSlabs[0].Key}
	if err := m.DeleteSpaceObject(writer, key, object.ID()); err != nil {
		t.Fatal(err)
	}
	publish()
	events, err = m.ListSpaceObjects(reader, key, cursor, 1)
	if err != nil || len(events) != 1 || !events[0].Deleted || events[0].Object != nil || events[0].Key != object.ID() {
		t.Fatalf("unexpected deletion event: %+v, %v", events, err)
	}
	if err := m.DeleteSpaceMember(writer, key, reader); err != nil {
		t.Fatal(err)
	} else if _, err := m.Space(reader, key); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("revoked reader accessed the space: %v", err)
	}
	wrappingKey, err := spaces.DeriveInvitationKey(ownerKey, readerKey.PublicKey(), key)
	if err != nil {
		t.Fatal(err)
	}
	invite := spaces.Invitation{SpaceKey: key, Inviter: owner, Recipient: reader, Permission: spaces.Read,
		EncryptedKey: sealSpaceTest(t, wrappingKey, seed[:]), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	invite.Signature = ownerKey.SignHash(invite.SigHash())
	if err := m.InviteSpaceMember(owner, invite); err != nil {
		t.Fatal(err)
	} else if err := m.DeleteSpaceInvitation(reader, key, reader); err != nil {
		t.Fatal(err)
	} else if err := m.AcceptSpaceInvitation(reader, key, sealSpaceTest(t, spaces.DeriveMemberKey(readerKey, key), seed[:])); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("accepted a declined invitation: %v", err)
	} else if err := m.DeleteSpace(owner, key); err != nil {
		t.Fatal(err)
	} else if _, err := m.Space(writer, key); !errors.Is(err, spaces.ErrNotFound) {
		t.Fatalf("accessed a deleted space: %v", err)
	}
}
