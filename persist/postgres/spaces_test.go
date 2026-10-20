package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestSpaces(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	ownerKey := types.GeneratePrivateKey()
	owner := proto.Account(ownerKey.PublicKey())
	member := proto.Account(types.GeneratePrivateKey().PublicKey())
	outsider := proto.Account(types.GeneratePrivateKey().PublicKey())
	for _, a := range []proto.Account{owner, member, outsider} {
		store.addTestAccount(t, types.PublicKey(a))
	}
	key, seed := types.Hash256(frand.Entropy256()), types.Hash256(frand.Entropy256())
	invitation := spaces.Invitation{SpaceKey: key, Inviter: owner, Recipient: member, Permission: spaces.Write, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	invitation.Signature = ownerKey.SignHash(invitation.SigHash())
	metadata, invitedKey, acceptedKey := frand.Bytes(50), frand.Bytes(72), frand.Bytes(72)
	if err := store.AddSpace(owner, key, seed, metadata); err != nil {
		t.Fatal(err)
	}
	expired := invitation
	expired.CreatedAt = time.Now().Add(-6 * time.Minute)
	expired.Signature = ownerKey.SignHash(expired.SigHash())
	if err := store.InviteSpaceMember(owner, key, member, spaces.Write, invitedKey, expired.CreatedAt, expired.ExpiresAt, expired.Signature); !errors.Is(err, spaces.ErrInvitationTimestampTooOld) {
		t.Fatalf("expected expired invitation timestamp, got %v", err)
	} else if pending, err := store.SpaceInvitations(member, 0, 10); err != nil || len(pending) != 0 {
		t.Fatalf("expired invitation persisted: %+v, %v", pending, err)
	}
	space, err := store.Space(owner, key)
	if err != nil {
		t.Fatal(err)
	} else if space.Key != key || space.Seed != seed || space.Owner != owner || !bytes.Equal(space.EncryptedMetadata, metadata) || space.CreatedAt.IsZero() {
		t.Fatalf("unexpected space: %+v", space)
	} else if err := store.InviteSpaceMember(owner, key, member, spaces.Write, invitedKey, invitation.CreatedAt, invitation.ExpiresAt, invitation.Signature); err != nil {
		t.Fatal(err)
	}
	invitations, err := store.SpaceInvitations(member, 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(invitations) != 1 || invitations[0].Inviter != owner || invitations[0].SpaceKey != key || invitations[0].Permission != spaces.Write || !bytes.Equal(invitations[0].EncryptedKey, invitedKey) {
		t.Fatalf("unexpected invitations: %+v", invitations)
	} else if !ownerKey.PublicKey().VerifyHash(invitations[0].SigHash(), invitations[0].Signature) {
		t.Fatal("stored invitation signature is invalid")
	} else if _, err := store.Space(member, key); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("pending member access: %v", err)
	} else if err := store.AcceptSpaceInvitation(outsider, key, acceptedKey); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("uninvited acceptance: %v", err)
	} else if err := store.AcceptSpaceInvitation(member, key, nil); err == nil {
		t.Fatal("accepted without a key")
	} else if err := store.AcceptSpaceInvitation(member, key, acceptedKey); err != nil {
		t.Fatal(err)
	} else if invitations, err := store.SpaceInvitations(member, 0, 10); err != nil || len(invitations) != 0 {
		t.Fatalf("invitation not consumed: %v, %v", invitations, err)
	} else if listed, err := store.Spaces(member, 0, 10); err != nil || len(listed) != 1 || !bytes.Equal(listed[0].EncryptedKey, acceptedKey) || listed[0].Permission != spaces.Write {
		t.Fatalf("unexpected member spaces: %+v, %v", listed, err)
	} else if listed, err := store.Spaces(outsider, 0, 10); err != nil || len(listed) != 0 {
		t.Fatalf("outsider spaces: %+v, %v", listed, err)
	} else if err := store.UpdateSpace(member, key, nil); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("member updated metadata: %v", err)
	} else if err := store.UpdateSpace(owner, key, []byte("updated")); err != nil {
		t.Fatal(err)
	} else if err := store.UpdateSpaceMember(member, key, member, spaces.Admin); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("member promoted themselves: %v", err)
	} else if err := store.UpdateSpaceMember(owner, key, member, spaces.Delete); err != nil {
		t.Fatal(err)
	}
	members, err := store.SpaceMembers(owner, key, 0, 10)
	if err != nil || len(members) != 1 || members[0].Account != member || members[0].InvitedBy != owner || members[0].Permission != spaces.Delete || !members[0].InvitedAt.Equal(invitation.CreatedAt.Truncate(time.Microsecond)) {
		t.Fatalf("unexpected members: %+v, %v", members, err)
	} else if err := store.DeleteSpaceMember(owner, key, member); err != nil {
		t.Fatal(err)
	} else if _, err := store.Space(member, key); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("revoked member access: %v", err)
	} else if listed, err := store.Spaces(member, 0, 10); err != nil || len(listed) != 0 {
		t.Fatalf("revoked spaces: %+v, %v", listed, err)
	} else if err := store.AcceptSpaceInvitation(member, key, acceptedKey); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("replayed invitation: %v", err)
	} else if err := store.InviteSpaceMember(owner, key, member, spaces.Read, invitedKey, time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpaceInvitation(owner, key, member); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(member, key, acceptedKey); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("cancelled invitation: %v", err)
	} else if err := store.InviteSpaceMember(owner, key, member, spaces.Read, invitedKey, time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(member, key, acceptedKey); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpace(member, key); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("member deleted space: %v", err)
	} else if err := store.DeleteSpace(owner, key); err != nil {
		t.Fatal(err)
	} else if _, err := store.Space(member, key); !errors.Is(err, spaces.ErrNotFound) {
		t.Fatalf("deleted space: %v", err)
	}
}

func TestSpacePermissions(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	}
	members := make([]proto.Account, 5)
	for i := range members {
		members[i] = proto.Account(types.GeneratePrivateKey().PublicKey())
		store.addTestAccount(t, types.PublicKey(members[i]))
		permission := spaces.Permission(min(i, 3))
		if err := store.InviteSpaceMember(owner, key, members[i], permission, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
			t.Fatal(err)
		} else if err := store.AcceptSpaceInvitation(members[i], key, frand.Bytes(72)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpdateSpace(members[2], key, []byte("denied")); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("non-admin updated metadata: %v", err)
	}
	metadata := []byte("admin metadata")
	if err := store.UpdateSpace(members[3], key, metadata); err != nil {
		t.Fatal(err)
	} else if space, err := store.Space(owner, key); err != nil || !bytes.Equal(space.EncryptedMetadata, metadata) {
		t.Fatalf("admin metadata update: %+v, %v", space, err)
	}
	for _, target := range []proto.Account{owner, members[3], members[4]} {
		if err := store.DeleteSpaceMember(members[3], key, target); !errors.Is(err, spaces.ErrPermission) {
			t.Fatalf("admin removed protected account: %v", err)
		}
	}
	if err := store.DeleteSpaceMember(members[2], key, members[0]); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("non-admin removed member: %v", err)
	} else if err := store.DeleteSpaceMember(members[3], key, members[0]); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpaceMember(owner, key, members[4]); err != nil {
		t.Fatal(err)
	} else if _, err := store.SpaceMembers(members[0], key, 0, 10); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("revoked member listed members: %v", err)
	} else if err := store.DeleteAccount(owner); err != nil {
		t.Fatal(err)
	} else if _, err := store.Space(members[1], key); !errors.Is(err, spaces.ErrNotFound) {
		t.Fatalf("deleted owner access: %v", err)
	}
}

func TestSpaceInvitations(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	admin := proto.Account(types.GeneratePrivateKey().PublicKey())
	member := proto.Account(types.GeneratePrivateKey().PublicKey())
	recipient := proto.Account(types.GeneratePrivateKey().PublicKey())
	otherApp := proto.Account(types.GeneratePrivateKey().PublicKey())
	appID := types.Hash256(frand.Entropy256())
	for _, account := range []proto.Account{owner, admin, member, recipient, otherApp} {
		store.addTestAccount(t, types.PublicKey(account))
		id := appID
		if account == otherApp {
			id = frand.Entropy256()
		}
		if _, err := store.pool.Exec(t.Context(), `UPDATE accounts SET app_id = $1 WHERE public_key = $2`, sqlHash256(id), sqlPublicKey(account)); err != nil {
			t.Fatal(err)
		}
	}
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	}
	for i, account := range []proto.Account{member, admin} {
		if err := store.InviteSpaceMember(owner, key, account, spaces.Permission(i+2), frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
			t.Fatal(err)
		} else if err := store.AcceptSpaceInvitation(account, key, frand.Bytes(72)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.InviteSpaceMember(member, key, recipient, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("non-admin invited member: %v", err)
	}
	for _, inviter := range []proto.Account{owner, admin} {
		if err := store.InviteSpaceMember(inviter, key, otherApp, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); !errors.Is(err, spaces.ErrPermission) {
			t.Fatalf("invited account from another app: %v", err)
		}
	}
	if invitations, err := store.SpaceInvitations(otherApp, 0, 10); err != nil || len(invitations) != 0 {
		t.Fatalf("cross-app invitation persisted: %+v, %v", invitations, err)
	}
	for _, inviter := range []proto.Account{owner, admin} {
		if err := store.InviteSpaceMember(inviter, key, recipient, spaces.Write, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
			t.Fatal(err)
		}
	}
	invitations, err := store.SpaceInvitations(recipient, 0, 10)
	if err != nil || len(invitations) != 1 || invitations[0].Inviter != admin || invitations[0].Permission != spaces.Write {
		t.Fatalf("unexpected admin invitation: %+v, %v", invitations, err)
	} else if err := store.AcceptSpaceInvitation(recipient, key, frand.Bytes(72)); err != nil {
		t.Fatal(err)
	} else if space, err := store.Space(recipient, key); err != nil || space.Permission != spaces.Write {
		t.Fatalf("accepted admin invitation: %+v, %v", space, err)
	} else if err := store.DeleteSpaceMember(owner, key, recipient); err != nil {
		t.Fatal(err)
	} else if err := store.InviteSpaceMember(admin, key, recipient, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteAccount(admin); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(recipient, key, frand.Bytes(72)); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("expected missing invitation from deleted inviter, got %v", err)
	} else if err := store.PruneAccounts(100); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(recipient, key, frand.Bytes(72)); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("accepted pruned inviter's invitation: %v", err)
	}
}

func TestSpaceObjects(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	writer := proto.Account(types.GeneratePrivateKey().PublicKey())
	reader := proto.Account(types.GeneratePrivateKey().PublicKey())
	for _, a := range []proto.Account{owner, writer, reader} {
		store.addTestAccount(t, types.PublicKey(a))
	}
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	}
	for i, a := range []proto.Account{reader, writer} {
		if err := store.InviteSpaceMember(owner, key, a, spaces.Permission(i), frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
			t.Fatal(err)
		} else if err := store.AcceptSpaceInvitation(a, key, frand.Bytes(72)); err != nil {
			t.Fatal(err)
		}
	}
	hk := types.GeneratePrivateKey().PublicKey()
	store.addTestHost(t, hk)
	store.addTestContract(t, hk)
	obj := store.pinTestObject(t, writer, hk)
	if err := store.PinSpaceObject(reader, key, obj.PinRequest()); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("reader pinned object: %v", err)
	} else if err := store.PinSpaceObject(writer, key, obj.PinRequest()); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	got, err := store.SpaceObject(reader, key, obj.ID())
	if err != nil {
		t.Fatal(err)
	} else if got.Account != writer || got.Key != obj.ID() || !reflect.DeepEqual(got.Slabs, obj.Slabs) || !bytes.Equal(got.EncryptedDataKey, obj.EncryptedDataKey) {
		t.Fatalf("object mismatch: %+v", got)
	} else if _, err := store.Object(owner, obj.ID()); !errors.Is(err, slabs.ErrObjectNotFound) {
		t.Fatalf("object in personal scope: %v", err)
	} else if objects, err := store.ListSpaceObjects(reader, key, slabs.Cursor{}, 10); err != nil || len(objects) != 1 {
		t.Fatalf("unexpected objects: %+v, %v", objects, err)
	}
	assertUsage := func(want uint64) {
		t.Helper()
		var data, size, connectData, connectSize uint64
		err := store.pool.QueryRow(t.Context(), `SELECT a.pinned_data, a.pinned_size, c.pinned_data, c.pinned_size FROM accounts a JOIN app_connect_keys c ON c.id = a.connect_key_id WHERE a.public_key = $1`, sqlPublicKey(owner)).Scan(&data, &size, &connectData, &connectSize)
		if err != nil {
			t.Fatal(err)
		} else if data != want || size != want || connectData != want || connectSize != want {
			t.Fatalf("usage: %d %d %d %d, want %d", data, size, connectData, connectSize, want)
		}
	}
	assertUsage(proto.SectorSize)
	update := obj.PinRequest()
	update.EncryptedDataKey = frand.Bytes(72)
	update.EncryptedMetadata = frand.Bytes(32)
	if err := store.PinSpaceObject(writer, key, update); !errors.Is(err, spaces.ErrObjectKeyMismatch) {
		t.Fatalf("expected data key mismatch, got %v", err)
	} else if unchanged, err := store.SpaceObject(reader, key, obj.ID()); err != nil || !reflect.DeepEqual(unchanged, got) {
		t.Fatalf("rejected update changed object: %+v, %v", unchanged, err)
	}
	update.EncryptedDataKey = obj.EncryptedDataKey
	if err := store.PinSpaceObject(writer, key, update); err != nil {
		t.Fatal(err)
	} else if updated, err := store.SpaceObject(reader, key, obj.ID()); err != nil || !bytes.Equal(updated.EncryptedMetadata, update.EncryptedMetadata) || !bytes.Equal(updated.EncryptedDataKey, obj.EncryptedDataKey) {
		t.Fatalf("unexpected updated object: %+v, %v", updated, err)
	}
	assertUsage(proto.SectorSize)
	if err := store.DeleteObject(writer, obj.ID()); err != nil {
		t.Fatal(err)
	} else if err := store.UnpinSlab(owner, obj.Slabs[0].Digest()); !errors.Is(err, slabs.ErrSlabInUse) {
		t.Fatalf("unpinned space slab: %v", err)
	} else if err := store.PruneSlabs(owner, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertUsage(proto.SectorSize)
	if _, err := store.PruneDeletedSlabs(100); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpaceObject(writer, key, obj.ID()); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("writer deleted object: %v", err)
	} else if err := store.UpdateSpaceMember(owner, key, writer, spaces.Delete); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpaceObject(writer, key, obj.ID()); err != nil {
		t.Fatal(err)
	}
	assertUsage(0)
	store.publishEvents(t)
	if events, err := store.ListSpaceObjects(reader, key, slabs.Cursor{}, 10); err != nil || len(events) != 1 || !events[0].Deleted || events[0].Object != nil {
		t.Fatalf("unexpected deletion event: %+v, %v", events, err)
	}
}

func TestSpaceQuotaRollback(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	member := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner), accounts.WithMaxPinnedData(0))
	store.addTestAccount(t, types.PublicKey(member))
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	} else if err := store.InviteSpaceMember(owner, key, member, spaces.Write, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(member, key, frand.Bytes(72)); err != nil {
		t.Fatal(err)
	}
	hk := types.GeneratePrivateKey().PublicKey()
	store.addTestHost(t, hk)
	store.addTestContract(t, hk)
	obj := store.pinTestObject(t, member, hk)
	if _, err := store.PinSpaceSlabs(member, key, time.Time{}, obj.Slabs[0].Pin()); !errors.Is(err, accounts.ErrAccountStorageLimitExceeded) {
		t.Fatalf("upload quota failure: %v", err)
	} else if err := store.PinSpaceObject(member, key, obj.PinRequest()); !errors.Is(err, accounts.ErrAccountStorageLimitExceeded) {
		t.Fatalf("quota failure: %v", err)
	}
	store.assertPinnedSlabs(t, owner)
	if objects, err := store.ListSpaceObjects(owner, key, slabs.Cursor{}, 10); err != nil || len(objects) != 0 {
		t.Fatalf("failed pin persisted: %+v, %v", objects, err)
	}
}

func TestSpaceStorageLifecycle(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	member := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	store.addTestAccount(t, types.PublicKey(member))
	keys := []types.Hash256{frand.Entropy256(), frand.Entropy256()}
	for _, key := range keys {
		if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
			t.Fatal(err)
		} else if err := store.InviteSpaceMember(owner, key, member, spaces.Write, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
			t.Fatal(err)
		} else if err := store.AcceptSpaceInvitation(member, key, frand.Bytes(72)); err != nil {
			t.Fatal(err)
		}
	}
	hk := types.GeneratePrivateKey().PublicKey()
	store.addTestHost(t, hk)
	store.addTestContract(t, hk)
	slab := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors:       []slabs.PinnedSector{{Root: frand.Entropy256(), HostKey: hk}},
	}
	if _, err := store.PinSpaceSlabs(member, keys[0], time.Time{}, slab); err != nil {
		t.Fatal(err)
	}
	store.assertPinnedSlabs(t, owner, slab.Digest())
	store.assertPinnedSlabs(t, member)
	obj := slabs.SealedObject{
		EncryptedDataKey:  frand.Bytes(72),
		DataSignature:     types.Signature(frand.Bytes(64)),
		MetadataSignature: types.Signature(frand.Bytes(64)),
		Slabs:             []slabs.SlabSlice{slab.Slice(0, 100)},
	}
	if err := store.PinSpaceObject(member, keys[1], obj.PinRequest()); !errors.Is(err, slabs.ErrObjectUnpinnedSlab) {
		t.Fatalf("used another space's pin: %v", err)
	} else if _, err := store.PinSpaceSlabs(member, keys[1], time.Time{}, slab); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := store.PinSpaceObject(member, key, obj.PinRequest()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteSpace(owner, keys[0]); err != nil {
		t.Fatal(err)
	}
	store.assertPinnedSlabs(t, owner, slab.Digest())
	if err := store.DeleteSpaceMember(owner, keys[1], member); err != nil {
		t.Fatal(err)
	} else if _, err := store.SpaceObject(member, keys[1], obj.ID()); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("revoked member read: %v", err)
	} else if _, err := store.PinSpaceSlabs(member, keys[1], time.Time{}, slab); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("revoked member upload: %v", err)
	} else if err := store.PinSpaceObject(member, keys[1], obj.PinRequest()); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("revoked member pin: %v", err)
	} else if err := store.DeleteAccount(member); err != nil {
		t.Fatal(err)
	} else if err := store.PruneAccounts(100); err != nil {
		t.Fatal(err)
	}
	got, err := store.SpaceObject(owner, keys[1], obj.ID())
	if err != nil || got.Account != member {
		t.Fatalf("object lost with member: %+v, %v", got, err)
	} else if err := store.DeleteAccount(owner); err != nil {
		t.Fatal(err)
	} else if err := store.PruneAccounts(100); err != nil {
		t.Fatal(err)
	} else if _, err := store.PruneDeletedSlabs(100); err != nil {
		t.Fatal(err)
	} else if _, err := store.Slab(slab.Digest()); !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatalf("space slab not cleaned up: %v", err)
	}
}

func TestSpaceInvitationRace(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	member := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	store.addTestAccount(t, types.PublicKey(member))
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	} else if err := store.InviteSpaceMember(owner, key, member, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Go(func() { results <- store.AcceptSpaceInvitation(member, key, frand.Bytes(72)) })
	}
	wg.Wait()
	close(results)
	var accepted, missing int
	for err := range results {
		if err == nil {
			accepted++
		} else if errors.Is(err, spaces.ErrInvitationNotFound) {
			missing++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || missing != 1 {
		t.Fatalf("accepted %d, missing %d", accepted, missing)
	}
}

func TestSpaceConcurrentReads(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	key := types.Hash256(frand.Entropy256())
	obj := slabs.SealedObject{EncryptedDataKey: frand.Bytes(72), DataSignature: types.Signature(frand.Bytes(64)), MetadataSignature: types.Signature(frand.Bytes(64))}
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	} else if err := store.PinSpaceObject(owner, key, obj.PinRequest()); err != nil {
		t.Fatal(err)
	}
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		if _, _, _, err := spaceAccess(ctx, tx, owner, key, spaces.Read, true); err != nil {
			return err
		}
		result := make(chan error, 1)
		go func() {
			if _, err := store.Space(owner, key); err != nil {
				result <- err
			} else if _, err := store.SpaceMembers(owner, key, 0, 10); err != nil {
				result <- err
			} else if _, err := store.ListSpaceObjects(owner, key, slabs.Cursor{}, 10); err != nil {
				result <- err
			} else {
				_, err := store.SpaceObject(owner, key, obj.ID())
				result <- err
			}
		}()
		select {
		case err := <-result:
			if err != nil {
				return err
			}
		case <-time.After(5 * time.Second):
			return errors.New("space reads blocked each other")
		}
		_, err := store.pool.Exec(ctx, `SELECT id FROM spaces WHERE space_key = $1 FOR UPDATE NOWAIT`, sqlHash256(key))
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			return fmt.Errorf("expected read to block exclusive lock, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSpaceInvitationExpiration(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	member := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	store.addTestAccount(t, types.PublicKey(member))
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	}
	if err := store.InviteSpaceMember(owner, key, member, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(-time.Second), types.Signature(frand.Bytes(64))); !errors.Is(err, spaces.ErrInvitationExpired) {
		t.Fatalf("expected expired invitation, got %v", err)
	}
	if err := store.InviteSpaceMember(owner, key, member, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if _, err := store.pool.Exec(t.Context(), `UPDATE space_invitations SET expires_at = NOW() - INTERVAL '1 second'`); err != nil {
		t.Fatal(err)
	} else if pending, err := store.SpaceInvitations(member, 0, 10); err != nil || len(pending) != 0 {
		t.Fatalf("expired invitation listed: %+v, %v", pending, err)
	} else if err := store.AcceptSpaceInvitation(member, key, frand.Bytes(72)); !errors.Is(err, spaces.ErrInvitationNotFound) {
		t.Fatalf("expired invitation accepted: %v", err)
	} else if _, err := store.Space(member, key); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("expired invitation granted access: %v", err)
	}
	if err := store.InviteSpaceMember(owner, key, member, spaces.Read, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(member, key, frand.Bytes(72)); err != nil {
		t.Fatal(err)
	}
}

func TestSpaceSlabIsolation(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	writer := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	store.addTestAccount(t, types.PublicKey(writer))
	key := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, key, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	} else if err := store.InviteSpaceMember(owner, key, writer, spaces.Write, frand.Bytes(72), time.Now(), time.Now().Add(time.Hour), types.Signature(frand.Bytes(64))); err != nil {
		t.Fatal(err)
	} else if err := store.AcceptSpaceInvitation(writer, key, frand.Bytes(72)); err != nil {
		t.Fatal(err)
	}
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)
	obj := store.pinTestObject(t, owner, hk)
	if err := store.PinSpaceObject(writer, key, obj.PinRequest()); !errors.Is(err, slabs.ErrObjectUnpinnedSlab) {
		t.Fatalf("used owner's personal pin: %v", err)
	} else if _, err := store.PinSpaceSlabs(writer, key, time.Time{}, obj.Slabs[0].Pin()); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteObject(owner, obj.ID()); err != nil {
		t.Fatal(err)
	} else if err := store.PruneSlabs(owner, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if err := store.UnpinSlab(owner, obj.Slabs[0].Digest()); !errors.Is(err, slabs.ErrSlabInUse) {
		t.Fatalf("unpinned staged space slab: %v", err)
	}
	store.assertPinnedSlabs(t, owner, obj.Slabs[0].Digest())
	store.assertPinnedSlabs(t, writer)
	if err := store.PinSpaceObject(writer, key, obj.PinRequest()); err != nil {
		t.Fatal(err)
	}
	other := types.Hash256(frand.Entropy256())
	if err := store.AddSpace(owner, other, frand.Entropy256(), nil); err != nil {
		t.Fatal(err)
	} else if _, err := store.PinSpaceSlabs(owner, other, time.Time{}, obj.Slabs[0].Pin()); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpaceObject(owner, key, obj.ID()); err != nil {
		t.Fatal(err)
	} else if err := store.DeleteSpace(owner, key); err != nil {
		t.Fatal(err)
	}
	store.assertPinnedSlabs(t, owner, obj.Slabs[0].Digest())
	if err := store.DeleteSpace(owner, other); err != nil {
		t.Fatal(err)
	}
	store.assertPinnedSlabs(t, owner)
	var remaining int
	if err := store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM space_slabs`).Scan(&remaining); err != nil {
		t.Fatal(err)
	} else if remaining != 0 {
		t.Fatalf("space pins remain after deletion: %d", remaining)
	}
}

func TestSpaceObjectEvents(t *testing.T) {
	store := initPostgres(t, zap.NewNop())
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	store.addTestAccount(t, types.PublicKey(owner))
	key, other := types.Hash256(frand.Entropy256()), types.Hash256(frand.Entropy256())
	for _, k := range []types.Hash256{key, other} {
		if err := store.AddSpace(owner, k, frand.Entropy256(), nil); err != nil {
			t.Fatal(err)
		}
	}
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)
	first, second := store.pinTestObject(t, owner, hk), store.pinTestObject(t, owner, hk)
	for _, obj := range []slabs.SealedObject{first, second} {
		if err := store.PinSpaceObject(owner, key, obj.PinRequest()); err != nil {
			t.Fatal(err)
		}
	}
	if events, err := store.ListSpaceObjects(owner, key, slabs.Cursor{}, 10); err != nil || len(events) != 0 {
		t.Fatalf("unpublished events: %+v, %v", events, err)
	}
	lock, err := store.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(t.Context())
	if _, err := lock.Exec(t.Context(), `SELECT object_key FROM space_object_events WHERE object_key = $1 FOR UPDATE`, sqlHash256(first.ID())); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	page, err := store.ListSpaceObjects(owner, key, slabs.Cursor{}, 1)
	if err != nil || len(page) != 1 || page[0].Key != second.ID() || page[0].Object == nil || page[0].Object.Account != owner {
		t.Fatalf("unexpected page: %+v, %v", page, err)
	}
	cursor := slabs.Cursor{After: page[0].UpdatedAt, Key: page[0].Key}
	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	page, err = store.ListSpaceObjects(owner, key, cursor, 1)
	if err != nil || len(page) != 1 || page[0].Key != first.ID() || !reflect.DeepEqual(page[0].Object.Slabs, first.Slabs) {
		t.Fatalf("skipped event: %+v, %v", page, err)
	}
	cursor = slabs.Cursor{After: page[0].UpdatedAt, Key: page[0].Key}
	if events, err := store.ListSpaceObjects(owner, other, slabs.Cursor{}, 10); err != nil || len(events) != 0 {
		t.Fatalf("events leaked to another space: %+v, %v", events, err)
	}
	update := first.PinRequest()
	update.EncryptedMetadata = frand.Bytes(40)
	if err := store.PinSpaceObject(owner, key, update); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	without, err := store.ListSpaceObjectsWithoutSlabs(owner, key, cursor, 10)
	if err != nil || len(without) != 1 || without[0].Object == nil || without[0].Object.Size != first.Size() || !bytes.Equal(without[0].Object.EncryptedMetadata, update.EncryptedMetadata) {
		t.Fatalf("unexpected update: %+v, %v", without, err)
	}
	cursor = slabs.Cursor{After: without[0].UpdatedAt, Key: without[0].Key}
	parts, err := store.SpaceObjectSlabs(owner, key, first.ID(), 0, 1)
	if err != nil || !reflect.DeepEqual(parts, first.Slabs[:1]) {
		t.Fatalf("unexpected slabs: %+v, %v", parts, err)
	}
	if parts, err := store.SpaceObjectSlabs(owner, key, first.ID(), int64(len(first.Slabs)), 1); err != nil || len(parts) != 0 {
		t.Fatalf("unexpected final page: %+v, %v", parts, err)
	}
	if err := store.DeleteSpaceObject(owner, key, first.ID()); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	page, err = store.ListSpaceObjects(owner, key, cursor, 10)
	if err != nil || len(page) != 1 || !page[0].Deleted || page[0].Object != nil {
		t.Fatalf("unexpected tombstone: %+v, %v", page, err)
	}
	cursor = slabs.Cursor{After: page[0].UpdatedAt, Key: page[0].Key}
	if err := store.PinSpaceObject(owner, key, first.PinRequest()); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	page, err = store.ListSpaceObjects(owner, key, cursor, 10)
	if err != nil || len(page) != 1 || page[0].Deleted || page[0].Object == nil {
		t.Fatalf("unexpected repin: %+v, %v", page, err)
	}
	cursor = slabs.Cursor{After: page[0].UpdatedAt, Key: page[0].Key}
	if err := store.BlockObject(first.ID(), "test"); err != nil {
		t.Fatal(err)
	}
	if events, err := store.ListSpaceObjects(owner, key, slabs.Cursor{}, 10); err != nil || len(events) != 1 || events[0].Key != second.ID() {
		t.Fatalf("blocked event: %+v, %v", events, err)
	}
	if err := store.UnblockObject(first.ID()); err != nil {
		t.Fatal(err)
	}
	store.publishEvents(t)
	if events, err := store.ListSpaceObjects(owner, key, cursor, 10); err != nil || len(events) != 1 || events[0].Key != first.ID() {
		t.Fatalf("unblocked event: %+v, %v", events, err)
	}
}
