package spaces_test

import (
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/spaces"
	"lukechampine.com/frand"
)

type noSpaceStore struct {
	spaces.Store
}

func TestManagerValidatesSpace(t *testing.T) {
	owner := proto.Account(types.GeneratePrivateKey().PublicKey())
	spaceKey := types.GeneratePrivateKey()
	m := spaces.NewManager(noSpaceStore{})
	for _, tt := range []struct {
		name   string
		change func(*proto.Account, *spaces.AddSpaceRequest)
	}{
		{"off-curve account", func(a *proto.Account, _ *spaces.AddSpaceRequest) { *a = proto.Account{2} }},
		{"short encrypted key", func(_ *proto.Account, r *spaces.AddSpaceRequest) { r.EncryptedKey = make([]byte, 71) }},
		{"large name", func(_ *proto.Account, r *spaces.AddSpaceRequest) {
			r.EncryptedName = make([]byte, spaces.MaxNameSize+1)
		}},
		{"large metadata", func(_ *proto.Account, r *spaces.AddSpaceRequest) {
			r.EncryptedMetadata = make([]byte, spaces.MaxMetadataSize+1)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			account := owner
			req := spaces.AddSpaceRequest{Key: spaceKey.PublicKey(), EncryptedKey: frand.Bytes(72), EncryptedName: frand.Bytes(40), EncryptedMetadata: frand.Bytes(40)}
			tt.change(&account, &req)
			req.Sign(spaceKey, account)
			if err := m.AddSpace(account, req); err == nil {
				t.Fatal("accepted invalid space")
			}
			if tt.name == "large name" || tt.name == "large metadata" {
				if err := m.UpdateSpace(account, req.Key, req.EncryptedName, req.EncryptedMetadata, req.Signature); err == nil {
					t.Fatal("accepted invalid space update")
				}
			}
		})
	}

	req := spaces.AddSpaceRequest{Key: spaceKey.PublicKey(), EncryptedKey: frand.Bytes(72), EncryptedName: frand.Bytes(40), EncryptedMetadata: frand.Bytes(40)}
	req.Sign(spaceKey, owner)
	req.EncryptedName[0] ^= 1
	if err := m.AddSpace(owner, req); !errors.Is(err, spaces.ErrInvalidSignature) {
		t.Fatalf("expected invalid space signature, got %v", err)
	}
	if err := m.UpdateSpace(owner, req.Key, req.EncryptedName, req.EncryptedMetadata, types.Signature{}); !errors.Is(err, spaces.ErrInvalidSignature) {
		t.Fatalf("expected invalid update signature, got %v", err)
	}
}

func TestManagerValidatesInvitation(t *testing.T) {
	inviterKey := types.GeneratePrivateKey()
	inviter := proto.Account(inviterKey.PublicKey())
	recipient := proto.Account(types.GeneratePrivateKey().PublicKey())
	spaceKey := types.GeneratePrivateKey().PublicKey()
	m := spaces.NewManager(noSpaceStore{})
	invite := spaces.Invitation{SpaceKey: spaceKey, Inviter: inviter, Recipient: recipient, Permission: spaces.Write, EncryptedKey: frand.Bytes(72), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	invite.Signature = inviterKey.SignHash(invite.SigHash())
	if err := m.InviteSpaceMember(recipient, invite); !errors.Is(err, spaces.ErrPermission) {
		t.Fatalf("expected permission error for different caller, got %v", err)
	}
	invite.EncryptedKey[0] ^= 1
	if err := m.InviteSpaceMember(inviter, invite); !errors.Is(err, spaces.ErrInvalidSignature) {
		t.Fatalf("expected invalid invitation signature, got %v", err)
	}
}

func TestManagerValidatesObject(t *testing.T) {
	appKey := types.GeneratePrivateKey()
	account := proto.Account(appKey.PublicKey())
	spaceKey := types.GeneratePrivateKey()
	m := spaces.NewManager(noSpaceStore{})
	obj := validationObject()
	invalid := obj
	invalid.Slabs = nil
	invalid.Sign(spaceKey)
	if err := m.PinSpaceObject(account, spaceKey.PublicKey(), invalid.PinRequest()); !errors.Is(err, spaces.ErrInvalidRequest) {
		t.Fatalf("expected invalid object request, got %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*slabs.SealedObject)
	}{
		{"account signature", func(obj *slabs.SealedObject) { obj.Sign(appKey) }},
		{"other space signature", func(obj *slabs.SealedObject) { obj.Sign(types.GeneratePrivateKey()) }},
		{"invalid data signature", func(obj *slabs.SealedObject) { obj.DataSignature[0] ^= 1 }},
		{"invalid metadata signature", func(obj *slabs.SealedObject) { obj.MetadataSignature[0] ^= 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			obj := obj
			obj.Sign(spaceKey)
			tt.change(&obj)
			if err := m.PinSpaceObject(account, spaceKey.PublicKey(), obj.PinRequest()); !errors.Is(err, slabs.ErrInvalidObjectSignature) {
				t.Fatalf("expected invalid object signature, got %v", err)
			}
		})
	}
}

func validationObject() slabs.SealedObject {
	return slabs.SealedObject{
		EncryptedDataKey: frand.Bytes(72), EncryptedMetadataKey: frand.Bytes(72), EncryptedMetadata: frand.Bytes(40),
		Slabs: []slabs.SlabSlice{{Version: 1, EncryptionKey: frand.Entropy256(), MinShards: 1, Length: 1,
			Sectors: []slabs.PinnedSector{{Root: frand.Entropy256(), HostKey: types.GeneratePrivateKey().PublicKey()}, {Root: frand.Entropy256(), HostKey: types.GeneratePrivateKey().PublicKey()}}}},
	}
}

func TestManagerValidatesSlabs(t *testing.T) {
	m := spaces.NewManager(noSpaceStore{})
	account := proto.Account(types.GeneratePrivateKey().PublicKey())
	key := types.GeneratePrivateKey().PublicKey()
	baseline := slabs.SlabPinParams{Version: 1, EncryptionKey: frand.Entropy256(), MinShards: 3}
	for range 12 {
		baseline.Sectors = append(baseline.Sectors, slabs.PinnedSector{Root: frand.Entropy256(), HostKey: types.GeneratePrivateKey().PublicKey()})
	}
	if err := baseline.Validate(time.Now()); err != nil {
		t.Fatalf("invalid baseline slab: %v", err)
	}
	if _, err := m.PinSpaceSlabs(account, key, time.Now().Add(time.Hour), baseline, slabs.SlabPinParams{}); !errors.Is(err, slabs.ErrInvalidPinParams) {
		t.Fatalf("expected invalid second slab, got %v", err)
	}
}
