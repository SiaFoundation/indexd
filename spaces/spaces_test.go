package spaces

import (
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestInvitationSigHash(t *testing.T) {
	key := types.GeneratePrivateKey()
	invitation := Invitation{SpaceKey: frand.Entropy256(), Inviter: [32]byte(key.PublicKey()), Recipient: frand.Entropy256(), Permission: Write, EncryptedKey: frand.Bytes(72), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	signature := key.SignHash(invitation.SigHash())
	for _, change := range []func(*Invitation){
		func(i *Invitation) { i.SpaceKey[0] ^= 1 },
		func(i *Invitation) { i.Permission = Admin },
		func(i *Invitation) { i.Inviter[0] ^= 1 },
		func(i *Invitation) { i.Recipient[0] ^= 1 },
		func(i *Invitation) {
			i.EncryptedKey = append([]byte(nil), i.EncryptedKey...)
			i.EncryptedKey[0] ^= 1
		},
		func(i *Invitation) { i.CreatedAt = i.CreatedAt.Add(time.Microsecond) },
		func(i *Invitation) { i.ExpiresAt = i.ExpiresAt.Add(time.Microsecond) },
	} {
		modified := invitation
		change(&modified)
		if key.PublicKey().VerifyHash(modified.SigHash(), signature) {
			t.Fatal("signature accepted a modified invitation")
		}
	}
	invitation.CreatedAt = invitation.CreatedAt.UTC().Truncate(time.Microsecond)
	invitation.ExpiresAt = invitation.ExpiresAt.UTC().Truncate(time.Microsecond)
	if !key.PublicKey().VerifyHash(invitation.SigHash(), signature) {
		t.Fatal("signature rejected timestamp normalization")
	}
}

func TestSpaceSignature(t *testing.T) {
	ownerKey := types.GeneratePrivateKey()
	spaceKey := types.GeneratePrivateKey()
	req := AddSpaceRequest{Key: spaceKey.PublicKey(), EncryptedKey: frand.Bytes(72), EncryptedName: frand.Bytes(40), EncryptedMetadata: frand.Bytes(60)}
	req.Sign(spaceKey, [32]byte(ownerKey.PublicKey()))
	space := Space{Key: req.Key, EncryptedKey: req.EncryptedKey, Owner: [32]byte(ownerKey.PublicKey()), EncryptedName: req.EncryptedName, EncryptedMetadata: req.EncryptedMetadata, Signature: req.Signature}
	if !spaceKey.PublicKey().VerifyHash(space.SigHash(), space.Signature) {
		t.Fatal("space signature rejected")
	}
	for _, change := range []func(*Space){
		func(s *Space) { s.Key[0] ^= 1 },
		func(s *Space) { s.Owner[0] ^= 1 },
		func(s *Space) {
			s.EncryptedName = append([]byte(nil), s.EncryptedName...)
			s.EncryptedName[0] ^= 1
		},
		func(s *Space) {
			s.EncryptedMetadata = append([]byte(nil), s.EncryptedMetadata...)
			s.EncryptedMetadata[0] ^= 1
		},
		func(s *Space) {
			s.EncryptedName = append(append([]byte(nil), s.EncryptedName...), s.EncryptedMetadata[0])
			s.EncryptedMetadata = s.EncryptedMetadata[1:]
		},
	} {
		modified := space
		change(&modified)
		if spaceKey.PublicKey().VerifyHash(modified.SigHash(), modified.Signature) {
			t.Fatal("signature accepted modified space fields")
		}
	}
	space.EncryptedKey = frand.Bytes(72)
	if !spaceKey.PublicKey().VerifyHash(space.SigHash(), space.Signature) {
		t.Fatal("member's encrypted key changed space signature")
	}
}

func TestInvitationValidate(t *testing.T) {
	now := time.Now()
	inviterKey := types.GeneratePrivateKey()
	valid := Invitation{
		Inviter:      [32]byte(inviterKey.PublicKey()),
		SpaceKey:     types.GeneratePrivateKey().PublicKey(),
		Recipient:    [32]byte(types.GeneratePrivateKey().PublicKey()),
		Permission:   Write,
		EncryptedKey: frand.Bytes(72),
		CreatedAt:    now,
		ExpiresAt:    now.Add(time.Hour),
	}
	for _, tt := range []struct {
		name   string
		change func(*Invitation)
		valid  bool
	}{
		{"valid", func(*Invitation) {}, true},
		{"empty space key", func(i *Invitation) { i.SpaceKey = types.PublicKey{} }, false},
		{"empty inviter", func(i *Invitation) { i.Inviter = [32]byte{} }, false},
		{"empty recipient", func(i *Invitation) { i.Recipient = [32]byte{} }, false},
		{"zero permission", func(i *Invitation) { i.Permission = 0 }, false},
		{"off-curve recipient", func(i *Invitation) { i.Recipient = [32]byte{2} }, false},
		{"low-order recipient", func(i *Invitation) { i.Recipient = [32]byte{1} }, false},
		{"self invitation", func(i *Invitation) { i.Recipient = i.Inviter }, false},
		{"owner permission", func(i *Invitation) { i.Permission = Owner }, false},
		{"short key", func(i *Invitation) { i.EncryptedKey = make([]byte, 71) }, false},
		{"old timestamp", func(i *Invitation) { i.CreatedAt = now.Add(-5*time.Minute - time.Nanosecond) }, false},
		{"timestamp boundary", func(i *Invitation) { i.CreatedAt = now.Add(-5 * time.Minute) }, true},
		{"future timestamp", func(i *Invitation) { i.CreatedAt = now.Add(5*time.Minute + time.Nanosecond) }, false},
		{"future timestamp boundary", func(i *Invitation) { i.CreatedAt = now.Add(5 * time.Minute) }, true},
		{"expiration boundary", func(i *Invitation) { i.ExpiresAt = now }, false},
		{"expiration before creation", func(i *Invitation) {
			i.CreatedAt = now.Add(2 * time.Minute)
			i.ExpiresAt = now.Add(time.Minute)
		}, false},
		{"expiration at creation", func(i *Invitation) {
			i.CreatedAt = now.Add(time.Minute)
			i.ExpiresAt = i.CreatedAt
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			invite := valid
			tt.change(&invite)
			invite.Signature = inviterKey.SignHash(invite.SigHash())
			if err := invite.Validate(now); (err == nil) != tt.valid {
				t.Fatalf("Validate returned %v, want valid=%v", err, tt.valid)
			} else if errors.Is(err, ErrInvalidSignature) {
				t.Fatal("invalid field was only rejected by signature verification")
			}
		})
	}
	if err := valid.Validate(now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature, got %v", err)
	}
}
