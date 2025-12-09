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
	dataKey     []byte
	metaDataKey []byte
	slabs       []slabs.SlabSlice
	metadata    json.RawMessage
	createdAt   time.Time
	updatedAt   time.Time
}

// ID returns the object's ID, which is a hash of its slabs.
func (o *Object) ID() types.Hash256 {
	return slabs.ObjectID(o.slabs)
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

	seal := func(keyCipher cipher.AEAD, plaintext []byte) []byte {
		nonce := frand.Bytes(keyCipher.NonceSize())
		return keyCipher.Seal(nonce, nonce, plaintext, nil)
	}
	encryptedDataKey := seal(dataKeyCipher(appKey, objectID), o.dataKey)
	encryptedMetaKey := seal(metadataKeyCipher(appKey, objectID), o.metaDataKey)
	encryptedMetadata := seal(metadataCipher(o.metaDataKey), o.metadata)

	so := slabs.SealedObject{
		EncryptedDataKey:     encryptedDataKey,
		Slabs:                o.slabs,
		EncryptedMetadataKey: encryptedMetaKey,
		EncryptedMetadata:    encryptedMetadata,
		CreatedAt:            o.createdAt,
		UpdatedAt:            o.updatedAt,
	}
	so.Sign(appKey)
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
	return s.client.CreateSharedObjectURL(ctx, s.appKey, obj.ID(), obj.dataKey, validUntil)
}

// dataKeyCipher derives the data key cipher from the app key and object ID.
func dataKeyCipher(appKey types.PrivateKey, objectID types.Hash256) cipher.AEAD {
	key := keys.Derive(appKey, objectID[:], []byte("dataKey"), 32)
	cipher, _ := chacha20poly1305.NewX(key)
	return cipher
}

// metadataKeyCipher derives the metadata key cipher from the app key and object ID.
func metadataKeyCipher(appKey types.PrivateKey, objectID types.Hash256) cipher.AEAD {
	key := keys.Derive(appKey, objectID[:], []byte("metadataKey"), 32)
	cipher, _ := chacha20poly1305.NewX(key)
	return cipher
}

// metadataCipher returns the cipher used to encrypt/decrypt metadata.
func metadataCipher(metadataKey []byte) cipher.AEAD {
	cipher, _ := chacha20poly1305.NewX(metadataKey)
	return cipher
}

func unlockEncryptedMetadata(metadataKey, encryptedMeta []byte) (json.RawMessage, error) {
	if len(encryptedMeta) == 0 {
		return nil, nil
	}
	metadataCipher := metadataCipher(metadataKey)
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
	} else if err := so.VerifySignatures(appKey.PublicKey()); err != nil {
		return Object{}, err
	}

	decryptKey := func(keyCipher cipher.AEAD, encryptedKey []byte) ([]byte, error) {
		if len(encryptedKey) < keyCipher.NonceSize() {
			return nil, fmt.Errorf("encrypted key is too short")
		}
		nonce := encryptedKey[:keyCipher.NonceSize()]
		var err error
		key, err := keyCipher.Open(nil, nonce, encryptedKey[keyCipher.NonceSize():], nil)
		if err != nil {
			return nil, fmt.Errorf("failed to unlock key: %w", err)
		}
		return key, nil
	}
	var err error
	obj.dataKey, err = decryptKey(dataKeyCipher(appKey, objectID), so.EncryptedDataKey)
	if err != nil {
		return Object{}, fmt.Errorf("failed to unlock data key: %w", err)
	}
	obj.metaDataKey, err = decryptKey(metadataKeyCipher(appKey, objectID), so.EncryptedMetadataKey)
	if err != nil {
		return Object{}, fmt.Errorf("failed to unlock metadata key: %w", err)
	}
	obj.metadata, err = unlockEncryptedMetadata(obj.metaDataKey, so.EncryptedMetadata)
	if err != nil {
		return Object{}, fmt.Errorf("failed to unlock metadata: %w", err)
	}
	return obj, nil
}
