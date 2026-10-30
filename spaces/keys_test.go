package spaces

import (
	"encoding/hex"
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestSpaceWrappingKeys(t *testing.T) {
	inviterKey, recipientKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	spaceKey := types.PublicKey(frand.Entropy256())
	invitationKey, err := DeriveInvitationKey(inviterKey, recipientKey.PublicKey(), spaceKey)
	if err != nil {
		t.Fatal(err)
	} else if receivedKey, err := DeriveInvitationKey(recipientKey, inviterKey.PublicKey(), spaceKey); err != nil || receivedKey != invitationKey {
		t.Fatalf("inviter and recipient derived different keys: %v", err)
	} else if otherKey, err := DeriveInvitationKey(inviterKey, types.GeneratePrivateKey().PublicKey(), spaceKey); err != nil || otherKey == invitationKey {
		t.Fatalf("different recipients derived the same key: %v", err)
	}
	memberKey := DeriveMemberKey(recipientKey, spaceKey)
	spaceKey[0] ^= 1
	if otherKey, err := DeriveInvitationKey(inviterKey, recipientKey.PublicKey(), spaceKey); err != nil || otherKey == invitationKey {
		t.Fatalf("different spaces derived the same invitation key: %v", err)
	} else if memberKey == DeriveMemberKey(recipientKey, spaceKey) {
		t.Fatal("different spaces derived the same member key")
	}
}

func TestDeriveInvitationKeyInvalidKeys(t *testing.T) {
	appKey := types.GeneratePrivateKey()
	peerKey := types.GeneratePrivateKey().PublicKey()
	spaceKey := types.PublicKey(frand.Entropy256())
	for _, size := range []int{0, 32, 63, 65} {
		if _, err := DeriveInvitationKey(make(types.PrivateKey, size), peerKey, spaceKey); err == nil {
			t.Fatalf("accepted %d-byte private key", size)
		}
	}
	for _, tt := range []struct {
		name string
		key  types.PublicKey
	}{
		{"off curve", types.PublicKey{2}},
		{"identity", types.PublicKey{1}},
		{"low order", types.PublicKey{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DeriveInvitationKey(appKey, tt.key, spaceKey); err == nil {
				t.Fatal("accepted invalid public key")
			}
		})
	}
}

func TestSpaceWrappingKeyVectors(t *testing.T) {
	var spaceKey types.PublicKey
	var inviterSeed, recipientSeed [32]byte
	for i := range inviterSeed {
		inviterSeed[i] = byte(i)
		recipientSeed[i] = byte(i + 64)
		spaceKey[i] = byte(i + 32)
	}
	inviterKey := types.NewPrivateKeyFromSeed(inviterSeed[:])
	recipientKey := types.NewPrivateKeyFromSeed(recipientSeed[:])
	invitationKey, err := DeriveInvitationKey(inviterKey, recipientKey.PublicKey(), spaceKey)
	if err != nil {
		t.Fatal(err)
	} else if hex.EncodeToString(invitationKey[:]) != "ed89726a87e7d278d69d8fe50776af8f74b53371775444c56cbcc63983b1c975" {
		t.Fatal("invitation wrapping key does not match test vector")
	}
	appKey, err := hex.DecodeString("b3383d90bf82c4c9bfa445a9d6090977dc9333d302309bd002ba9407c42674b4581010a63b0682cc447cd970fdb8b7d634e1e6fec0631a310e53069c7e1b61d9")
	if err != nil {
		t.Fatal(err)
	}
	memberKey := DeriveMemberKey(appKey, spaceKey)
	if hex.EncodeToString(memberKey[:]) != "ca79c8a13a7a20b1abf272f670af2c829d2a310c363ad0c2d576b164786436b8" {
		t.Fatal("member wrapping key does not match test vector")
	}
}
