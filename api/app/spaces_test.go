package app_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/frand"
)

func sealSpaceAPI(t *testing.T, key [32]byte, plaintext []byte) []byte {
	t.Helper()
	cipher, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		t.Fatal(err)
	}
	nonce := frand.Bytes(cipher.NonceSize())
	return cipher.Seal(nonce, nonce, plaintext, nil)
}

func openSpaceAPI(t *testing.T, key [32]byte, ciphertext []byte) []byte {
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

func TestSpacesAPI(t *testing.T) {
	for _, encoding := range []string{"json", "cbor"} {
		t.Run(encoding, func(t *testing.T) {
			ctx := t.Context()
			store := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zap.NewNop())
			t.Cleanup(func() { store.Close() })
			am, err := accounts.NewManager(store)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { am.Close() })
			server := httptest.NewUnstartedServer(nil)
			handler, err := app.NewAPI("http://"+server.Listener.Addr().String(), nil, am, nil, nil, nil, spaces.NewManager(store))
			if err != nil {
				t.Fatal(err)
			}
			server.Config.Handler = handler
			server.Start()
			t.Cleanup(server.Close)
			client := app.NewClient(server.URL, app.WithEnableCBOR(encoding == "cbor"))
			ownerKey, memberKey, outsiderKey := types.GeneratePrivateKey(), types.GeneratePrivateKey(), types.GeneratePrivateKey()
			owner, member, outsider := proto.Account(ownerKey.PublicKey()), proto.Account(memberKey.PublicKey()), proto.Account(outsiderKey.PublicKey())
			for _, account := range []proto.Account{owner, member, outsider} {
				store.AddTestAccount(t, types.PublicKey(account))
			}
			spaceKey := types.GeneratePrivateKey()
			key, seed := spaceKey.PublicKey(), [32]byte(spaceKey[:32])
			req := spaces.AddSpaceRequest{
				Key:               key,
				EncryptedKey:      sealSpaceAPI(t, spaces.DeriveMemberKey(ownerKey, key), seed[:]),
				EncryptedName:     sealSpaceAPI(t, seed, []byte("Shared documents")),
				EncryptedMetadata: sealSpaceAPI(t, seed, []byte(`{"color":"blue"}`)),
			}
			assertStatus(t, client.AddSpace(ctx, ownerKey, req), http.StatusBadRequest)
			req.Sign(spaceKey, owner)
			if err := client.AddSpace(ctx, ownerKey, req); err != nil {
				t.Fatal(err)
			}
			assertStatus(t, client.AddSpace(ctx, ownerKey, req), http.StatusConflict)
			created, err := client.Space(ctx, ownerKey, key)
			if err != nil {
				t.Fatal(err)
			} else if created.Owner != owner || created.Permission != spaces.Owner || !key.VerifyHash(created.SigHash(), created.Signature) {
				t.Fatalf("invalid space response: %+v", created)
			} else if recovered := openSpaceAPI(t, spaces.DeriveMemberKey(ownerKey, key), created.EncryptedKey); !bytes.Equal(recovered, seed[:]) {
				t.Fatal("owner could not recover the space key")
			}
			_, err = client.Space(ctx, types.GeneratePrivateKey(), key)
			assertStatus(t, err, http.StatusUnauthorized)
			_, err = client.Space(ctx, outsiderKey, key)
			assertStatus(t, err, http.StatusForbidden)

			inviteFor := func(recipient types.PrivateKey) spaces.Invitation {
				t.Helper()
				wrappingKey, err := spaces.DeriveInvitationKey(ownerKey, recipient.PublicKey(), key)
				if err != nil {
					t.Fatal(err)
				}
				invite := spaces.Invitation{SpaceKey: key, Inviter: owner, Recipient: proto.Account(recipient.PublicKey()), Permission: spaces.Read,
					EncryptedKey: sealSpaceAPI(t, wrappingKey, seed[:]), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
				invite.Signature = ownerKey.SignHash(invite.SigHash())
				return invite
			}
			invite := inviteFor(memberKey)
			assertStatus(t, client.InviteSpaceMember(ctx, outsiderKey, invite), http.StatusForbidden)
			if err := client.InviteSpaceMember(ctx, ownerKey, invite); err != nil {
				t.Fatal(err)
			}
			pending, err := client.SpaceInvitations(ctx, memberKey)
			if err != nil || len(pending) != 1 {
				t.Fatalf("expected one invitation: %+v, %v", pending, err)
			} else if !ownerKey.PublicKey().VerifyHash(pending[0].SigHash(), pending[0].Signature) {
				t.Fatal("invitation signature did not survive the API round trip")
			}
			wrappingKey, err := spaces.DeriveInvitationKey(memberKey, types.PublicKey(pending[0].Inviter), pending[0].SpaceKey)
			if err != nil {
				t.Fatal(err)
			}
			memberSeed := openSpaceAPI(t, wrappingKey, pending[0].EncryptedKey)
			memberSpaceKey := types.NewPrivateKeyFromSeed(memberSeed)
			if memberSpaceKey.PublicKey() != key {
				t.Fatal("invitation contained a different space key")
			}
			memberWrappingKey := spaces.DeriveMemberKey(memberKey, key)
			if err := client.AcceptSpaceInvitation(ctx, memberKey, key, sealSpaceAPI(t, memberWrappingKey, memberSeed)); err != nil {
				t.Fatal(err)
			} else if pending, err := client.SpaceInvitations(ctx, memberKey); err != nil || len(pending) != 0 {
				t.Fatalf("accepted invitation remained pending: %+v, %v", pending, err)
			}
			listed, err := client.Spaces(ctx, memberKey)
			if err != nil || len(listed) != 1 {
				t.Fatalf("accepted space was not listed: %+v, %v", listed, err)
			} else if recovered := openSpaceAPI(t, memberWrappingKey, listed[0].EncryptedKey); !bytes.Equal(recovered, memberSeed) {
				t.Fatal("member could not recover the rewrapped key")
			}
			if members, err := client.SpaceMembers(ctx, memberKey, key); err != nil || len(members) != 2 {
				t.Fatalf("unexpected members: %+v, %v", members, err)
			}
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
			_, err = client.PinSpaceSlabs(ctx, memberKey, key, params)
			assertStatus(t, err, http.StatusForbidden)
			if err := client.UpdateSpaceMember(ctx, ownerKey, key, member, spaces.Write); err != nil {
				t.Fatal(err)
			} else if ids, err := client.PinSpaceSlabs(ctx, memberKey, key, params); err != nil || len(ids) != 1 || ids[0] != params.Digest() {
				t.Fatalf("unexpected slab IDs: %v, %v", ids, err)
			}
			dataKey, metadataKey := frand.Entropy256(), frand.Entropy256()
			object := slabs.SealedObject{
				EncryptedDataKey:     sealSpaceAPI(t, [32]byte(memberSeed), dataKey[:]),
				EncryptedMetadataKey: sealSpaceAPI(t, [32]byte(memberSeed), metadataKey[:]),
				EncryptedMetadata:    sealSpaceAPI(t, metadataKey, []byte(`{"name":"notes.txt"}`)),
				Slabs:                []slabs.SlabSlice{params.Slice(0, 32), params.Slice(32, 64)},
			}
			object.Sign(memberKey)
			if err := client.PinSpaceObject(ctx, memberKey, key, object); err != nil {
				t.Fatal(err)
			}
			assertStatus(t, client.DeleteSpaceObject(ctx, memberKey, key, object.ID()), http.StatusForbidden)
			got, err := client.SpaceObject(ctx, ownerKey, key, object.ID())
			if err != nil {
				t.Fatal(err)
			} else if got.Key != object.ID() || got.Account != member || got.VerifySignatures(memberKey.PublicKey()) != nil {
				t.Fatalf("invalid object response: %+v", got)
			} else if recovered := openSpaceAPI(t, seed, got.EncryptedDataKey); !bytes.Equal(recovered, dataKey[:]) {
				t.Fatal("owner could not decrypt the member's object key")
			}
			for i, want := range object.Slabs {
				page, err := client.SpaceObjectSlabs(ctx, ownerKey, key, object.ID(), int64(i), 1)
				if err != nil || len(page) != 1 || !reflect.DeepEqual(page[0], want) {
					t.Fatalf("unexpected slab page %d: %+v, %v", i, page, err)
				}
			}
			publish := func() {
				t.Helper()
				var claimed bool
				if err := store.QueryRow(ctx, `SELECT object_events_last_published >= date_trunc('second', NOW()) FROM global_settings`).Scan(&claimed); err != nil {
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
			events, err := client.ListSpaceObjects(ctx, ownerKey, key, slabs.Cursor{}, 1)
			if err != nil || len(events) != 1 || events[0].Object == nil || events[0].Object.VerifySignatures(memberKey.PublicKey()) != nil {
				t.Fatalf("unexpected object events: %+v, %v", events, err)
			}
			cursor := slabs.Cursor{After: events[0].UpdatedAt, Key: events[0].Key}
			if page, err := client.ListSpaceObjects(ctx, ownerKey, key, cursor, 1); err != nil || len(page) != 0 {
				t.Fatalf("unexpected events after cursor: %+v, %v", page, err)
			}
			object.EncryptedMetadata = sealSpaceAPI(t, metadataKey, []byte(`{"name":"renamed.txt"}`))
			object.Sign(memberKey)
			if err := client.PinSpaceObject(ctx, memberKey, key, object); err != nil {
				t.Fatal(err)
			}
			publish()
			withoutSlabs, err := client.ListSpaceObjectsWithoutSlabs(ctx, ownerKey, key, cursor, 1)
			if err != nil || len(withoutSlabs) != 1 || withoutSlabs[0].Object == nil || withoutSlabs[0].Object.Size != object.Size() {
				t.Fatalf("unexpected object update: %+v, %v", withoutSlabs, err)
			} else if metadata := openSpaceAPI(t, metadataKey, withoutSlabs[0].Object.EncryptedMetadata); string(metadata) != `{"name":"renamed.txt"}` {
				t.Fatalf("unexpected metadata: %s", metadata)
			}
			cursor = slabs.Cursor{After: withoutSlabs[0].UpdatedAt, Key: withoutSlabs[0].Key}
			updated := spaces.AddSpaceRequest{Key: key, EncryptedName: sealSpaceAPI(t, seed, []byte("Team documents")), EncryptedMetadata: req.EncryptedMetadata}
			updated.Sign(memberSpaceKey, owner)
			assertStatus(t, client.UpdateSpace(ctx, memberKey, key, updated.EncryptedName, updated.EncryptedMetadata, updated.Signature), http.StatusForbidden)
			if err := client.UpdateSpaceMember(ctx, ownerKey, key, member, spaces.Admin); err != nil {
				t.Fatal(err)
			} else if err := client.UpdateSpace(ctx, memberKey, key, updated.EncryptedName, updated.EncryptedMetadata, updated.Signature); err != nil {
				t.Fatal(err)
			}
			if updated, err := client.Space(ctx, ownerKey, key); err != nil {
				t.Fatal(err)
			} else if !key.VerifyHash(updated.SigHash(), updated.Signature) || string(openSpaceAPI(t, seed, updated.EncryptedName)) != "Team documents" {
				t.Fatal("space update signature or encrypted name changed")
			}
			if err := client.DeleteSpaceObject(ctx, memberKey, key, object.ID()); err != nil {
				t.Fatal(err)
			}
			publish()
			events, err = client.ListSpaceObjects(ctx, ownerKey, key, cursor, 1)
			if err != nil || len(events) != 1 || !events[0].Deleted || events[0].Object != nil || events[0].Key != object.ID() {
				t.Fatalf("unexpected deletion event: %+v, %v", events, err)
			}
			_, err = client.SpaceObject(ctx, ownerKey, key, object.ID())
			assertStatus(t, err, http.StatusNotFound)
			if err := client.DeleteSpaceMember(ctx, ownerKey, key, member); err != nil {
				t.Fatal(err)
			}
			_, err = client.Space(ctx, memberKey, key)
			assertStatus(t, err, http.StatusForbidden)
			if err := client.InviteSpaceMember(ctx, ownerKey, inviteFor(outsiderKey)); err != nil {
				t.Fatal(err)
			} else if err := client.DeleteSpaceInvitation(ctx, outsiderKey, key, outsider); err != nil {
				t.Fatal(err)
			}
			assertStatus(t, client.AcceptSpaceInvitation(ctx, outsiderKey, key, sealSpaceAPI(t, spaces.DeriveMemberKey(outsiderKey, key), seed[:])), http.StatusNotFound)
			if err := client.DeleteSpace(ctx, ownerKey, key); err != nil {
				t.Fatal(err)
			}
			_, err = client.Space(ctx, ownerKey, key)
			assertStatus(t, err, http.StatusNotFound)
		})
	}
}
