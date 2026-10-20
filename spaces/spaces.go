package spaces

import (
	"errors"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// Space permissions grant read, write, delete, and admin access cumulatively.
const (
	Read Permission = iota
	Write
	Delete
	Admin
	Owner
)

// Errors returned by space operations.
var (
	ErrNotFound                  = errors.New("space not found")
	ErrPermission                = errors.New("insufficient space permission")
	ErrInvitationNotFound        = errors.New("space invitation not found")
	ErrMemberNotFound            = errors.New("space member not found")
	ErrObjectKeyMismatch         = errors.New("space object encrypted data key cannot change")
	ErrInvitationTimestampTooOld = errors.New("space invitation timestamp is more than five minutes old")
	ErrInvitationExpired         = errors.New("space invitation has expired")
)

// Permission is a member's level of access to a space.
type Permission uint8

// Space describes a space and the requesting account's access to it.
type Space struct {
	Key               types.Hash256
	Owner             proto.Account
	Seed              types.Hash256
	EncryptedMetadata []byte
	EncryptedKey      []byte
	Permission        Permission
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Invitation is a pending invitation to join a space.
type Invitation struct {
	SpaceKey     types.Hash256
	Inviter      proto.Account
	Recipient    proto.Account
	Permission   Permission
	EncryptedKey []byte
	Signature    types.Signature
	ExpiresAt    time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SigHash returns the hash of the invitation's space, inviter, recipient,
// permission, and creation and expiration times in Unix microseconds.
func (i Invitation) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteString("indexd/space/invitation/v1")
	i.SpaceKey.EncodeTo(h.E)
	types.PublicKey(i.Inviter).EncodeTo(h.E)
	types.PublicKey(i.Recipient).EncodeTo(h.E)
	h.E.WriteUint8(uint8(i.Permission))
	h.E.WriteUint64(uint64(i.CreatedAt.UnixMicro()))
	h.E.WriteUint64(uint64(i.ExpiresAt.UnixMicro()))
	return h.Sum()
}

// Member is an active member of a space.
type Member struct {
	Account    proto.Account
	InvitedBy  proto.Account
	InvitedAt  time.Time
	Permission Permission
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Object is an encrypted object in a space and the account that signed it.
type Object struct {
	Key     types.Hash256
	Account proto.Account
	slabs.SealedObject
}

// ObjectWithoutSlabs is a space object without its slab slices.
type ObjectWithoutSlabs struct {
	Key     types.Hash256 `json:"key"`
	Account proto.Account `json:"account"`
	slabs.SealedObjectWithoutSlabs
}

// ObjectEvent is a published object change, including a deletion tombstone.
type ObjectEvent struct {
	Key       types.Hash256 `json:"key"`
	Deleted   bool          `json:"deleted"`
	UpdatedAt time.Time     `json:"updatedAt"`
	Object    *Object       `json:"object,omitempty"`
}

// ObjectEventWithoutSlabs is a published object change without slab slices.
type ObjectEventWithoutSlabs struct {
	Key       types.Hash256       `json:"key"`
	Deleted   bool                `json:"deleted"`
	UpdatedAt time.Time           `json:"updatedAt"`
	Object    *ObjectWithoutSlabs `json:"object,omitempty"`
}
