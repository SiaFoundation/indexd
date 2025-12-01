package slabs

import (
	"context"
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	// A SealedObject is an object that has been locked with an app key.
	// It can be safely serialized and shared, but cannot be used to access
	// the underlying data until it has been unlocked with the app key.
	SealedObject struct {
		EncryptedMasterKey []byte      `json:"encryptedMasterKey"`
		Slabs              []SlabSlice `json:"slabs"`
		EncryptedMetadata  []byte      `json:"encryptedMetadata"`
		// Signature is a signature of the blake2b(object ID, encrypted_master_key, and encrypted_metadata)
		// to attest that the object has not been tampered with.
		Signature types.Signature `json:"signature"`

		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// SlabSlice represents a slice of a slab that is part of an object.
	SlabSlice struct {
		SlabID        SlabID         `json:"slabID"`
		EncryptionKey [32]byte       `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
		Offset        uint32         `json:"offset"`
		Length        uint32         `json:"length"`
	}

	// SharedObject provides all the metadata necessary to retrieve and decrypt
	// an object.
	SharedObject struct {
		Slabs             []PinnedSlabSlice `json:"slabs"`
		EncryptedMetadata []byte            `json:"encryptedMetadata"`
	}

	// A PinnedSlabSlice represents a slice of a slab that is part of an object.
	// It contains all the metadata needed to retrieve a slab.
	PinnedSlabSlice struct {
		ID            SlabID         `json:"id"`
		EncryptionKey [32]byte       `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
		Offset        uint32         `json:"offset"`
		Length        uint32         `json:"length"`
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
	h := types.NewHasher()
	for _, slab := range so.Slabs {
		slab.EncodeTo(h.E)
	}
	return h.Sum()
}

// SigHash returns the hash that should be signed to attest to the validity of
// the object.
func (so *SealedObject) SigHash() types.Hash256 {
	h := types.NewHasher()
	so.ID().EncodeTo(h.E)
	h.E.Write(so.EncryptedMasterKey)
	h.E.Write(so.EncryptedMetadata)
	return h.Sum()
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
	} else if !types.PublicKey(account).VerifyHash(obj.SigHash(), obj.Signature) {
		return ErrInvalidObjectSignature
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
