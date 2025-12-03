package sdk

import (
	"context"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/slabs"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/frand"
)

// An Object represents a collection of slabs that can be used to access
// encrypted data. The master key is used to encrypt/decrypt the data and
// metadata, and should be kept secret.
//
// It has no public fields to prevent accidental leakage of unencrypted data.
type Object struct {
	masterKey []byte
	slabs     []slabs.SlabSlice
	metadata  json.RawMessage
	createdAt time.Time
	updatedAt time.Time
}

// ID returns the object's ID, which is a hash of its slabs.
func (o *Object) ID() types.Hash256 {
	h := types.NewHasher()
	for _, slab := range o.slabs {
		slab.EncodeTo(h.E)
	}
	return h.Sum()
}

// CreatedAt returns the time the object was created.
func (o *Object) CreatedAt() time.Time {
	return o.createdAt
}

// UpdatedAt returns the time the object was last updated.
func (o *Object) UpdatedAt() time.Time {
	return o.updatedAt
}

// Seal returns a SealedObject that can be safely serialized and shared.
func (o *Object) Seal(appKey types.PrivateKey) slabs.SealedObject {
	objectID := o.ID()
	keyCipher := masterKeyCipher(appKey, objectID)
	nonce := frand.Bytes(keyCipher.NonceSize())
	encryptedMasterKey := keyCipher.Seal(nonce, nonce, o.masterKey, nil)

	metaCipher := metadataCipher(o.masterKey, objectID)
	nonce = frand.Bytes(metaCipher.NonceSize())
	encryptedMeta := metaCipher.Seal(nonce, nonce, o.metadata, nil)

	so := slabs.SealedObject{
		EncryptedMasterKey: encryptedMasterKey,
		Slabs:              o.slabs,
		EncryptedMetadata:  encryptedMeta,
		CreatedAt:          o.createdAt,
		UpdatedAt:          o.updatedAt,
	}
	so.Signature = appKey.SignHash(so.SigHash())
	return so
}

// Size returns the total size of the object in bytes.
func (o *Object) Size() uint64 {
	var size uint64
	for _, ss := range o.slabs {
		size += uint64(ss.Length)
	}
	return size
}

// Slabs returns a copy of the object's slabs.
func (o *Object) Slabs() []slabs.SlabSlice {
	return slices.Clone(o.slabs)
}

// Metadata returns a copy of the object's metadata.
func (o *Object) Metadata() json.RawMessage {
	return slices.Clone(o.metadata)
}

// UpdateMetadata updates the object's metadata.
func (o *Object) UpdateMetadata(meta json.RawMessage) {
	o.metadata = slices.Clone(meta)
}

// Object retrieves the object with the given key.
func (s *SDK) Object(ctx context.Context, objectKey types.Hash256) (Object, error) {
	lo, err := s.client.Object(ctx, s.appKey, objectKey)
	if err != nil {
		return Object{}, fmt.Errorf("failed to get locked object: %w", err)
	}
	return objectFromSealedObject(lo, s.appKey)
}

// ListObjects lists objects, starting from the given cursor, up to the given limit.
func (s *SDK) ListObjects(ctx context.Context, cursor slabs.Cursor, limit int) ([]Object, error) {
	los, err := s.client.ListObjects(ctx, s.appKey, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list locked objects: %w", err)
	}
	objs := make([]Object, 0, len(los))
	for _, lo := range los {
		if lo.Deleted {
			continue
		}
		obj, err := objectFromSealedObject(*lo.Object, s.appKey)
		if err != nil {
			return nil, fmt.Errorf("failed to unlock object: %w", err)
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

// CreateSharedObjectURL creates a URL that can be used to share the object
// until the given time. The URL contains the encryption key required to decrypt
// the object's data and metadata.
//
// Sharing the URL allows anyone with the URL to read the object's data
// and metadata. They will not be able to modify the object or access any other
// objects in the account.
func (s *SDK) CreateSharedObjectURL(ctx context.Context, objectKey types.Hash256, validUntil time.Time) (string, error) {
	obj, err := s.Object(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("failed to get object: %w", err)
	}
	return s.client.CreateSharedObjectURL(ctx, s.appKey, obj.ID(), obj.masterKey, validUntil)
}

func masterKeyCipher(appKey types.PrivateKey, objectID types.Hash256) cipher.AEAD {
	key := keys.Derive(appKey, objectID[:], []byte("master"), 32)
	cipher, _ := chacha20poly1305.NewX(key)
	return cipher
}

func metadataCipher(masterKey []byte, objectID types.Hash256) cipher.AEAD {
	key := keys.Derive(masterKey, objectID[:], []byte("metadata"), 32)
	cipher, _ := chacha20poly1305.NewX(key)
	return cipher
}

func unlockEncryptedMetadata(objectID types.Hash256, masterKey []byte, encryptedMeta []byte) (json.RawMessage, error) {
	if len(encryptedMeta) == 0 {
		return nil, nil
	}
	metadataCipher := metadataCipher(masterKey, objectID)
	if len(encryptedMeta) < metadataCipher.NonceSize() {
		return nil, fmt.Errorf("encrypted metadata too short")
	}
	nonce := encryptedMeta[:metadataCipher.NonceSize()]
	metadata, err := metadataCipher.Open(nil, nonce, encryptedMeta[metadataCipher.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to unlock metadata: %w", err)
	}
	return metadata, nil
}

func objectFromSealedObject(so slabs.SealedObject, appKey types.PrivateKey) (Object, error) {
	obj := Object{
		slabs:     slices.Clone(so.Slabs),
		createdAt: so.CreatedAt,
		updatedAt: so.UpdatedAt,
	}
	objectID := obj.ID()
	if so.ID() != objectID {
		return Object{}, fmt.Errorf("object ID mismatch")
	} else if !appKey.PublicKey().VerifyHash(so.SigHash(), so.Signature) {
		return Object{}, fmt.Errorf("invalid object signature")
	}

	keyCipher := masterKeyCipher(appKey, objectID)
	if len(so.EncryptedMasterKey) < keyCipher.NonceSize() {
		return Object{}, fmt.Errorf("encrypted master key too short")
	}
	nonce := so.EncryptedMasterKey[:keyCipher.NonceSize()]
	var err error
	obj.masterKey, err = keyCipher.Open(nil, nonce, so.EncryptedMasterKey[keyCipher.NonceSize():], nil)
	if err != nil {
		return Object{}, fmt.Errorf("failed to unlock master key: %w", err)
	}
	obj.metadata, err = unlockEncryptedMetadata(objectID, obj.masterKey, so.EncryptedMetadata)
	if err != nil {
		return Object{}, fmt.Errorf("failed to unlock metadata: %w", err)
	}
	return obj, nil
}
