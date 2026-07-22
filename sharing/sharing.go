package sharing

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/keys"
)

const (
	// NonceSize is the required length of a sharing key's nonce, used as the
	// HKDF salt when deriving the sharing key from the app key.
	NonceSize = 32
	// EncryptionKeySize is the length of a sealed encryption key (xchacha20
	// nonce + key + tag).
	EncryptionKeySize = 72
	// MaxDescriptionSize is the maximum length of a sharing key description.
	MaxDescriptionSize = 1024
	// MaxMetadataSize is the maximum length of a shared object's encrypted
	// metadata, mirroring the limit on a normal object's metadata.
	MaxMetadataSize = 1024
)

var (
	// ErrSharingKeyNotFound is returned when a sharing key does not exist.
	ErrSharingKeyNotFound = errors.New("sharing key not found")
	// ErrSharingKeyExists is returned when creating a sharing key that already
	// exists.
	ErrSharingKeyExists = errors.New("sharing key already exists")
	// ErrSharedObjectNotFound is returned when an object is not attached to a
	// sharing key.
	ErrSharedObjectNotFound = errors.New("shared object not found")
	// ErrInvalidRequest is returned when a request fails validation.
	ErrInvalidRequest = errors.New("invalid request")
)

type (
	// A Nonce is the per-key HKDF salt used to derive a sharing key from the
	// creator's app key. It marshals to text as a hex string.
	Nonce [NonceSize]byte

	// A Key is a scoped, read-only sharing key that grants access to a specific
	// set of objects without requiring the recipient to log in.
	Key struct {
		Account     types.PublicKey `json:"account"`
		PublicKey   types.PublicKey `json:"publicKey"`
		Nonce       Nonce           `json:"nonce"`
		Description string          `json:"description"`
		// ObjectCount is the total number of objects attached to this key
		ObjectCount uint64 `json:"objectCount"`
		// ObjectSize is the logical size of all objects attached to this key
		ObjectSize uint64 `json:"objectSize"`
		// PinnedData is the size of all objects attached to this key on the network, excluding redundancy
		PinnedData uint64 `json:"pinnedData"`
		// PinnedSize is the size of all objects attached to this key on the network, including redundancy
		PinnedSize uint64     `json:"pinnedSize"`
		ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
		CreatedAt  time.Time  `json:"createdAt"`
		UpdatedAt  time.Time  `json:"updatedAt"`
	}

	// A KeyRequest contains the fields required to create a sharing key.
	KeyRequest struct {
		PublicKey   types.PublicKey `json:"publicKey"`
		Nonce       Nonce           `json:"nonce"`
		Description string          `json:"description"`
		ExpiresAt   *time.Time      `json:"expiresAt,omitempty"`
	}

	// A SharedObjectRequest attaches an object to a sharing key. The object's
	// encryption keys are re-sealed under the sharing key and re-signed so the
	// recipient can decrypt and verify them.
	SharedObjectRequest struct {
		ObjectID             types.Hash256   `json:"objectID"`
		EncryptedDataKey     []byte          `json:"encryptedDataKey"`
		DataSignature        types.Signature `json:"dataSignature"`
		EncryptedMetadataKey []byte          `json:"encryptedMetadataKey,omitempty"`
		EncryptedMetadata    []byte          `json:"encryptedMetadata,omitempty"`
		MetadataSignature    types.Signature `json:"metadataSignature"`
	}
)

func (r KeyRequest) validate() error {
	switch {
	case r.PublicKey == (types.PublicKey{}):
		return fmt.Errorf("%w: public key is required", ErrInvalidRequest)
	case r.Nonce == (Nonce{}):
		return fmt.Errorf("%w: nonce is required", ErrInvalidRequest)
	case len(r.Description) > MaxDescriptionSize:
		return fmt.Errorf("%w: description exceeds %d bytes", ErrInvalidRequest, MaxDescriptionSize)
	}
	return nil
}

func (r SharedObjectRequest) validate() error {
	switch {
	case r.ObjectID == (types.Hash256{}):
		return fmt.Errorf("%w: object ID is required", ErrInvalidRequest)
	case len(r.EncryptedDataKey) != EncryptionKeySize:
		return fmt.Errorf("%w: encrypted data key must be %d bytes", ErrInvalidRequest, EncryptionKeySize)
	case len(r.EncryptedMetadataKey) != 0 && len(r.EncryptedMetadataKey) != EncryptionKeySize:
		return fmt.Errorf("%w: encrypted metadata key must be %d bytes", ErrInvalidRequest, EncryptionKeySize)
	case len(r.EncryptedMetadata) > MaxMetadataSize:
		return fmt.Errorf("%w: encrypted metadata exceeds %d bytes", ErrInvalidRequest, MaxMetadataSize)
	}
	return nil
}

func (r SharedObjectRequest) dataSigHash() types.Hash256 {
	h := types.NewHasher()
	r.ObjectID.EncodeTo(h.E)
	h.E.Write(r.EncryptedDataKey)
	return h.Sum()
}

func (r SharedObjectRequest) metaSigHash() types.Hash256 {
	h := types.NewHasher()
	r.ObjectID.EncodeTo(h.E)
	h.E.Write(r.EncryptedMetadataKey)
	h.E.Write(r.EncryptedMetadata)
	return h.Sum()
}

// Sign signs the re-sealed keys with the sharing key's private key so the
// recipient can verify them.
func (r *SharedObjectRequest) Sign(sharingKey types.PrivateKey) {
	r.DataSignature = sharingKey.SignHash(r.dataSigHash())
	r.MetadataSignature = sharingKey.SignHash(r.metaSigHash())
}

// VerifySignatures verifies the re-sealed keys against the sharing key.
func (r SharedObjectRequest) VerifySignatures(sharingKey types.PublicKey) error {
	if !sharingKey.VerifyHash(r.dataSigHash(), r.DataSignature) {
		return fmt.Errorf("%w: invalid data signature", ErrInvalidRequest)
	} else if !sharingKey.VerifyHash(r.metaSigHash(), r.MetadataSignature) {
		return fmt.Errorf("%w: invalid metadata signature", ErrInvalidRequest)
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (n Nonce) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(n[:])), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (n *Nonce) UnmarshalText(b []byte) error {
	if len(b) != hex.EncodedLen(NonceSize) {
		return fmt.Errorf("invalid nonce: expected %d hex characters, got %d", hex.EncodedLen(NonceSize), len(b))
	}
	_, err := hex.Decode(n[:], b)
	return err
}

// DeriveSharingKey derives a new ed25519 private key from the given private key and random nonce
func DeriveSharingKey(key types.PrivateKey, nonce Nonce) types.PrivateKey {
	buf := keys.Derive(key[:], nonce[:], []byte("share key"), 32)
	defer clear(buf)
	return types.NewPrivateKeyFromSeed(buf)
}
