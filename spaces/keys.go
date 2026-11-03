package spaces

import (
	"crypto/ed25519"
	"crypto/sha512"
	"fmt"

	"filippo.io/edwards25519"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/keys"
	"golang.org/x/crypto/curve25519"
)

// DeriveInvitationKey derives an invitation wrapping key from an app private key,
// the other account's public key, and the public space identifier. It converts the
// Ed25519 keys to X25519 and applies HKDF-BLAKE2b-256 to their shared secret.
func DeriveInvitationKey(appKey types.PrivateKey, peerKey, spaceKey types.PublicKey) ([32]byte, error) {
	if len(appKey) != ed25519.PrivateKeySize {
		return [32]byte{}, fmt.Errorf("invalid app private key length: %d", len(appKey))
	}
	peer, err := new(edwards25519.Point).SetBytes(peerKey[:])
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid peer public key: %w", err)
	}
	hashed := sha512.Sum512(appKey[:ed25519.SeedSize])
	defer clear(hashed[:])
	sharedSecret, err := curve25519.X25519(hashed[:32], peer.BytesMontgomery())
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to derive invitation shared secret: %w", err)
	}
	defer clear(sharedSecret)
	buf := keys.Derive(sharedSecret, spaceKey[:], []byte("invitationKey"), 32)
	defer clear(buf)
	return [32]byte(buf), nil
}

// DeriveMemberKey derives the wrapping key for an owner or member from their
// app private key using HKDF-BLAKE2b-256. spaceKey is the public space
// identifier, not the secret space encryption key.
func DeriveMemberKey(appKey types.PrivateKey, spaceKey types.PublicKey) [32]byte {
	buf := keys.Derive(appKey[:], spaceKey[:], []byte("memberKey"), 32)
	defer clear(buf)
	return [32]byte(buf)
}
