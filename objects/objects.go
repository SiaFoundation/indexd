package objects

import (
	"errors"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
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
		SlabID slabs.SlabID `json:"slabID"`
		Offset uint32       `json:"offset"`
		Length uint32       `json:"length"`
	}
)

var (
	// ErrObjectNotFound is returned when an object is not found in the database.
	ErrObjectNotFound = errors.New("object not found")
)
