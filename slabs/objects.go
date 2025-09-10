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
	// ErrMetadataLimitExceeded is returned when the provided metadata is too large.
	ErrMetadataLimitExceeded = fmt.Errorf("object metadata size limit (%d) exceeded", metadataLimit)
)

func (m *SlabManager) Object(ctx context.Context, account proto.Account, key types.Hash256) (Object, error) {
	return m.store.Object(ctx, account, key)
}

func (m *SlabManager) DeleteObject(ctx context.Context, account proto.Account, objectKey types.Hash256) error {
	return m.store.DeleteObject(ctx, account, objectKey)
}

func (m *SlabManager) SaveObject(ctx context.Context, account proto.Account, obj Object) error {
	if len(obj.Slabs) == 0 {
		return ErrObjectMinimumSlabs
	} else if len(obj.Meta) > metadataLimit {
		return fmt.Errorf("%w: got %d bytes", ErrMetadataLimitExceeded, len(obj.Meta))
	}

	return m.store.SaveObject(ctx, account, obj)
}

func (m *SlabManager) ListObjects(ctx context.Context, account proto.Account, cursor Cursor, limit int) ([]Object, error) {
	return m.store.ListObjects(ctx, account, cursor, limit)
}
