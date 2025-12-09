package slabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	// A SealedObject is an object that has been locked with an app key.
	// It can be safely serialized and shared, but cannot be used to access
	// the underlying data until it has been unlocked with the app key.
	SealedObject struct {
		EncryptedDataKey []byte      `json:"encryptedDataKey"`
		Slabs            []SlabSlice `json:"slabs"`
		// DataSignature is a signature of the blake2b(object ID, encrypted_data_key)
		// to attest that the object data has not been tampered with. This attestation
		// is separate from the metadata attestation to allow for validating the
		// encryption key when sharing the object which does not include the metadata.
		DataSignature types.Signature `json:"dataSignature"`

		EncryptedMetadataKey []byte `json:"encryptedMetadataKey"`
		EncryptedMetadata    []byte `json:"encryptedMetadata"`
		// MetadataSignature is a signature of the blake2b(object ID, metadata key, and encrypted_metadata)
		// to attest that the object has not been tampered with.
		MetadataSignature types.Signature `json:"metadataSignature"`

		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// SlabSlice represents a slice of a slab that is part of an object.
	SlabSlice struct {
		EncryptionKey EncryptionKey  `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
		Offset        uint32         `json:"offset"`
		Length        uint32         `json:"length"`
	}

	// SharedObject provides all the metadata necessary to retrieve and decrypt
	// an object.
	SharedObject struct {
		Slabs             []SlabSlice `json:"slabs"`
		EncryptedMetadata []byte      `json:"encryptedMetadata"`
	}

	// ObjectEvent represents an event on an object, such as it being created,
	// updated, or deleted.  The object itself is included if it has not been
	// deleted.
	ObjectEvent struct {
		Key       types.Hash256 `json:"key"`
		Deleted   bool          `json:"deleted"`
		UpdatedAt time.Time     `json:"updatedAt"`

		Object *SealedObject `json:"object,omitempty"`
	}

	// Cursor describes a cursor for paginating through objects. During
	// pagination, 'After' is meant to be set to the 'UpdatedAt' value of the
	// last object received and 'Key' is meant to be set to the 'Key' value of
	// the last object received. This allows for consistent pagination even if
	// multiple objects have the same 'UpdatedAt' timestamp since objects are
	// returned sorted by their 'Key'.
	//
	// NOTE: Considering that 'UpdatedAt' for an object can increase if updated
	// while paginating, it's possible to see the same object multiple times
	// with higher timestamps and different slabs/metadata.
	Cursor struct {
		After time.Time
		Key   types.Hash256
	}
)

// Size returns the total size of the object in bytes.
func (o *SharedObject) Size() uint64 {
	var size uint64
	for _, ss := range o.Slabs {
		size += uint64(ss.Length)
	}
	return size
}

// metadataLimit represents the maximum size of an objects metadata we will
// store.
const metadataLimit = 1024

var (
	// ErrInvalidObjectSignature is returned when an object's signature is invalid.
	ErrInvalidObjectSignature = errors.New("invalid object signature")
	// ErrObjectNotFound is returned when an object is not found in the database.
	ErrObjectNotFound = errors.New("object not found")
	// ErrObjectMinimumSlabs is returned when the object has no slabs.
	ErrObjectMinimumSlabs = errors.New("object must have at least one slab")
	// ErrObjectMetadataLimitExceeded is returned when the provided metadata is too large.
	ErrObjectMetadataLimitExceeded = fmt.Errorf("object metadata size limit (%d) exceeded", metadataLimit)
	// ErrObjectUnpinnedSlab is returned when an user attempts to save an
	// object containing a slab that is not pinned to their account.
	ErrObjectUnpinnedSlab = errors.New("object contains unpinned slab")
)

// ID returns the object's ID, which is a hash of its slabs.
func (so *SealedObject) ID() types.Hash256 {
	return ObjectID(so.Slabs)
}

// Sign signs the object's data and metadata signatures using the given private
// key.
func (so *SealedObject) Sign(pk types.PrivateKey) {
	so.DataSignature = pk.SignHash(so.DataSigHash())
	so.MetadataSignature = pk.SignHash(so.MetaSigHash())
}

// ObjectID returns the object's ID, which is a hash of its slabs.
func ObjectID(slabs []SlabSlice) types.Hash256 {
	h := types.NewHasher()
	for _, slab := range slabs {
		slabID := slab.Digest()
		h.E.Write(slabID[:])
		h.E.WriteUint64(uint64(slab.Offset)<<32 | uint64(slab.Length))
	}
	return h.Sum()
}

// DataSigHash returns the hash used for signing/verifying the data signature.
// It covers the slabs through the object ID and the encrypted data key.
func (so *SealedObject) DataSigHash() types.Hash256 {
	h := types.NewHasher()
	so.ID().EncodeTo(h.E)
	h.E.Write(so.EncryptedDataKey)
	return h.Sum()
}

// MetaSigHash returns the hash used for signing/verifying the metadata
// signature. It covers the slabs through the object ID, the encrypted metadata
// key, and the encrypted metadata.
func (so *SealedObject) MetaSigHash() types.Hash256 {
	h := types.NewHasher()
	so.ID().EncodeTo(h.E)
	h.E.Write(so.EncryptedMetadataKey)
	h.E.Write(so.EncryptedMetadata)
	return h.Sum()
}

// VerifyDataSignature verifies the object's data signature using the given
// public key.
func (so *SealedObject) VerifyDataSignature(pk types.PublicKey) error {
	if !pk.VerifyHash(so.DataSigHash(), so.DataSignature) {
		return fmt.Errorf("%w: invalid data signature", ErrInvalidObjectSignature)
	}
	return nil
}

// VerifyMetadataSignature verifies the object's metadata signature using the
// given public key.
func (so *SealedObject) VerifyMetadataSignature(pk types.PublicKey) error {
	if !pk.VerifyHash(so.MetaSigHash(), so.MetadataSignature) {
		return fmt.Errorf("%w: invalid metadata signature", ErrInvalidObjectSignature)
	}
	return nil
}

// VerifySignatures verifies both the data and metadata signatures using the
// given public key.
func (so *SealedObject) VerifySignatures(pk types.PublicKey) error {
	return errors.Join(so.VerifyDataSignature(pk), so.VerifyMetadataSignature(pk))
}

// Object retrieves the object with the given key for the given account.
func (m *SlabManager) Object(ctx context.Context, account proto.Account, key types.Hash256) (SealedObject, error) {
	return m.store.Object(account, key)
}

// DeleteObject deletes the object with the given key for the given account.
func (m *SlabManager) DeleteObject(ctx context.Context, account proto.Account, objectKey types.Hash256) error {
	return m.store.DeleteObject(account, objectKey)
}

// SaveObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (m *SlabManager) SaveObject(ctx context.Context, account proto.Account, obj SealedObject) error {
	if len(obj.Slabs) == 0 {
		return ErrObjectMinimumSlabs
	} else if len(obj.EncryptedMetadata) > metadataLimit {
		return fmt.Errorf("%w: got %d bytes", ErrObjectMetadataLimitExceeded, len(obj.EncryptedMetadata))
	} else if err := obj.VerifySignatures(types.PublicKey(account)); err != nil {
		return err
	}

	return m.store.SaveObject(account, obj)
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (m *SlabManager) ListObjects(ctx context.Context, account proto.Account, cursor Cursor, limit int) ([]ObjectEvent, error) {
	return m.store.ListObjects(account, cursor, limit)
}

// SharedObject retrieves the shared object with the given key for the given account.
func (m *SlabManager) SharedObject(ctx context.Context, key types.Hash256) (SharedObject, error) {
	return m.store.SharedObject(key)
}

// EncryptionKey is a helper type for JSON marshalling of 32-byte encryption
// keys as base64 strings.
type EncryptionKey [32]byte

// MarshalJSON implements the json.Marshaler interface using base64 encoding.
func (k EncryptionKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(k[:]))
}

// UnmarshalJSON implements the json.Unmarshaler interface using base64 decoding.
func (k *EncryptionKey) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	} else if len(decoded) != 32 {
		return fmt.Errorf("invalid encryption key length: expected 32, got %d", len(decoded))
	}
	copy(k[:], decoded)
	return nil
}

// Pin converts the SlabSlice to SlabPinParams.
func (s SlabSlice) Pin() SlabPinParams {
	return SlabPinParams{
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       slices.Clone(s.Sectors),
	}
}

// Slice creates a SlabSlice from the SlabPinParams.
func (s SlabPinParams) Slice(offset, length uint32) SlabSlice {
	return SlabSlice{
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       slices.Clone(s.Sectors),
		Offset:        offset,
		Length:        length,
	}
}

// Slice creates a SlabSlice from the PinnedSlab.
func (s PinnedSlab) Slice(offset, length uint32) SlabSlice {
	return SlabSlice{
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       slices.Clone(s.Sectors),
		Offset:        offset,
		Length:        length,
	}
}
