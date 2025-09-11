package slabs

import (
	"bytes"
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

// EncodeTo implements types.EncoderTo.
func (s SlabSlice) EncodeTo(e *types.Encoder) {
	e.Write(s.SlabID[:])
	e.WriteUint64(uint64(s.Offset)<<32 | uint64(s.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (s *SlabSlice) DecodeFrom(d *types.Decoder) {
	d.Read(s.SlabID[:])

	combined := d.ReadUint64()
	s.Offset = uint32(combined >> 32)
	s.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (obj Object) EncodeTo(e *types.Encoder) {
	e.Write(obj.Key[:])
	types.EncodeSlice(e, obj.Slabs)
	e.WriteBytes(obj.Meta)
	e.WriteTime(obj.CreatedAt)
	e.WriteTime(obj.UpdatedAt)
}

// DecodeFrom implements types.DecoderFrom.
func (obj *Object) DecodeFrom(d *types.Decoder) {
	d.Read(obj.Key[:])
	types.DecodeSlice(d, &obj.Slabs)
	obj.Meta = d.ReadBytes()
	obj.CreatedAt = d.ReadTime()
	obj.UpdatedAt = d.ReadTime()
}

// MarshalSia is a convenience method to encode the object metadata into bytes
// using the Sia encoding.
func (obj *Object) MarshalSia() ([]byte, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	obj.EncodeTo(e)
	e.Flush()
	return buf.Bytes(), nil
}

// UnmarshalSia is a convenience method to decode the Sia-encoded bytes into an
// object metadata type.
func (obj *Object) UnmarshalSia(b []byte) error {
	d := types.NewBufDecoder(b)
	obj.DecodeFrom(d)
	return d.Err()
}

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
func (m *SlabManager) SaveObject(ctx context.Context, account proto.Account, key types.Hash256, slabs []SlabSlice, meta []byte) error {
	if len(slabs) == 0 {
		return ErrObjectMinimumSlabs
	} else if len(meta) > metadataLimit {
		return fmt.Errorf("%w: got %d bytes", ErrObjectMetadataLimitExceeded, len(meta))
	}

	return m.store.SaveObject(ctx, account, key, slabs, meta)
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
