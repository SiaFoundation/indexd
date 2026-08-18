package slabs

import (
	"context"
	"errors"
	"time"

	"go.sia.tech/core/types"
)

var (
	// ErrObjectBlocked is returned when an object is on the blocklist.
	ErrObjectBlocked = errors.New("object is unavailable for legal reasons")
	// ErrObjectNotBlocked is returned when fetching an object that is not on
	// the blocklist.
	ErrObjectNotBlocked = errors.New("object is not blocked")
)

// A BlockedObject is an object key on the blocklist.
type BlockedObject struct {
	Key       types.Hash256 `json:"key"`
	Reason    string        `json:"reason"`
	CreatedAt time.Time     `json:"createdAt"`
}

// BlockObject adds the given object key to the blocklist with the given reason.
// If the key is already blocked its reason is updated. The key does not need to
// reference an existing object.
func (m *SlabManager) BlockObject(ctx context.Context, objectKey types.Hash256, reason string) error {
	return m.store.BlockObject(objectKey, reason)
}

// UnblockObject removes an object key from the blocklist. Unblocking a key that
// is not blocked is a no-op.
func (m *SlabManager) UnblockObject(ctx context.Context, objectKey types.Hash256) error {
	return m.store.UnblockObject(objectKey)
}

// BlockedObject returns the blocklist entry for the given object key. It
// returns ErrObjectNotBlocked if the key is not blocked.
func (m *SlabManager) BlockedObject(ctx context.Context, objectKey types.Hash256) (BlockedObject, error) {
	return m.store.BlockedObject(objectKey)
}

// BlockedObjects returns a paginated list of the blocked object keys, most
// recently blocked first.
func (m *SlabManager) BlockedObjects(ctx context.Context, offset, limit int) ([]BlockedObject, error) {
	return m.store.BlockedObjects(offset, limit)
}
