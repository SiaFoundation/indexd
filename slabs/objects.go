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
	// A SharedObjectSlab represents a slab of a shared object.
	// It contains all the metadata needed to retrieve a slab.
	SharedObjectSlab struct {
		ID            SlabID         `json:"id"`
		EncryptionKey [32]byte       `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
		Offset        uint32         `json:"offset"`
		Length        uint32         `json:"length"`
	}

	// SharedObject provides all the metadata necessary to retrieve
	// and decrypt an object.
	SharedObject struct {
		Key   types.Hash256      `json:"key"`
		Slabs []SharedObjectSlab `json:"slabs"`
		Meta  []byte             `json:"meta,omitempty"`
	}

	// Object represents a collection of slabs that form an uploaded object.
	Object struct {
		Key       types.Hash256 `json:"key"`
		Slabs     []SlabSlice   `json:"slabs"`
		Meta      []byte        `json:"meta,omitempty"`
		CreatedAt time.Time     `json:"createdAt"`
		UpdatedAt time.Time     `json:"updatedAt"`
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

	// SlabSlice represents a slice of a slab that is part of an object.
	SlabSlice struct {
		SlabID SlabID `json:"slabID"`
		Offset uint32 `json:"offset"`
		Length uint32 `json:"length"`
	}
)

// metadataLimit represents the maximum size of an objects metadata we will
// store.
const metadataLimit = 1024

var (
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

// Object retrieves the object with the given key for the given account.
func (m *SlabManager) Object(ctx context.Context, account proto.Account, key types.Hash256) (Object, error) {
	return m.store.Object(ctx, account, key)
}

// DeleteObject deletes the object with the given key for the given account.
func (m *SlabManager) DeleteObject(ctx context.Context, account proto.Account, objectKey types.Hash256) error {
	return m.store.DeleteObject(ctx, account, objectKey)
}

// SaveObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (m *SlabManager) SaveObject(ctx context.Context, account proto.Account, obj Object) error {
	if len(obj.Slabs) == 0 {
		return ErrObjectMinimumSlabs
	} else if len(obj.Meta) > metadataLimit {
		return fmt.Errorf("%w: got %d bytes", ErrObjectMetadataLimitExceeded, len(obj.Meta))
	}

	return m.store.SaveObject(ctx, account, obj)
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (m *SlabManager) ListObjects(ctx context.Context, account proto.Account, cursor Cursor, limit int) ([]Object, error) {
	return m.store.ListObjects(ctx, account, cursor, limit)
}

// SharedObject retrieves the shared object with the given key for the given account.
func (m *SlabManager) SharedObject(ctx context.Context, key types.Hash256) (SharedObject, error) {
	return m.store.SharedObject(ctx, key)
}
