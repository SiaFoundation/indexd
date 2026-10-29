package spaces

import (
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// Space permissions grant read, write, delete, and admin access cumulatively.
const (
	Read Permission = iota + 1
	Write
	Delete
	Admin
	Owner
)

// Errors returned by space operations.
var (
	ErrInvalidSignature            = errors.New("invalid space signature")
	ErrNotFound                    = errors.New("space not found")
	ErrExists                      = errors.New("space already exists")
	ErrPermission                  = errors.New("insufficient space permission")
	ErrInvitationNotFound          = errors.New("space invitation not found")
	ErrMemberNotFound              = errors.New("space member not found")
	ErrObjectKeyMismatch           = errors.New("space object encrypted data key cannot change")
	ErrInvitationTimestampTooOld   = errors.New("space invitation timestamp is more than five minutes old")
	ErrInvitationTimestampInFuture = errors.New("space invitation timestamp is more than five minutes in the future")
	ErrInvitationExpired           = errors.New("space invitation has expired")
)

// Permission is a member's level of access to a space.
type Permission uint8

// AddSpaceRequest creates a space with an encrypted name, metadata, and the
// random space encryption key wrapped for the owner.
type AddSpaceRequest struct {
	Key               types.PublicKey `json:"key"`
	EncryptedKey      []byte          `json:"encryptedKey"`
	EncryptedName     []byte          `json:"encryptedName"`
	EncryptedMetadata []byte          `json:"encryptedMetadata"`
	Signature         types.Signature `json:"signature"`
}

// SigHash returns the hash of the space key, owner's app key, encrypted name,
// and encrypted metadata.
func (r AddSpaceRequest) SigHash(owner proto.Account) types.Hash256 {
	return (Space{Key: r.Key, Owner: owner, EncryptedName: r.EncryptedName, EncryptedMetadata: r.EncryptedMetadata}).SigHash()
}

// Sign signs the space's shared fields with the space private key.
func (r *AddSpaceRequest) Sign(spaceKey types.PrivateKey, owner proto.Account) {
	r.Signature = spaceKey.SignHash(r.SigHash(owner))
}

// Space describes a space and the requesting account's access to it.
type Space struct {
	Key               types.PublicKey `json:"key"`
	Owner             proto.Account   `json:"owner"`
	EncryptedName     []byte          `json:"encryptedName"`
	EncryptedMetadata []byte          `json:"encryptedMetadata"`
	EncryptedKey      []byte          `json:"encryptedKey"`
	Permission        Permission      `json:"permission"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	Signature         types.Signature `json:"signature"`
}

// SigHash returns the hash of the space key, owner's app key, encrypted name,
// and encrypted metadata.
func (s Space) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteString("indexd/space/v1")
	s.Key.EncodeTo(h.E)
	types.PublicKey(s.Owner).EncodeTo(h.E)
	h.E.WriteBytes(s.EncryptedName)
	h.E.WriteBytes(s.EncryptedMetadata)
	return h.Sum()
}

// Invitation is a pending invitation to join a space.
type Invitation struct {
	SpaceKey     types.PublicKey `json:"spaceKey"`
	Inviter      proto.Account   `json:"inviter"`
	Recipient    proto.Account   `json:"recipient"`
	Permission   Permission      `json:"permission"`
	EncryptedKey []byte          `json:"encryptedKey"`
	ExpiresAt    time.Time       `json:"expiresAt"`
	CreatedAt    time.Time       `json:"createdAt"`

	Signature types.Signature `json:"signature"`
}

// Validate checks the invitation fields and signature.
func (i Invitation) Validate(now time.Time) error {
	if err := validateAccountAndSpaceKeys(i.Inviter, i.SpaceKey); err != nil {
		return err
	} else if err := validatePublicKey("recipient", types.PublicKey(i.Recipient)); err != nil {
		return err
	}
	switch {
	case i.Inviter == i.Recipient:
		return fmt.Errorf("%w: inviter and recipient must be different", ErrInvalidRequest)
	case i.Permission < Read || i.Permission > Admin:
		return fmt.Errorf("%w: invalid invitation permission", ErrInvalidRequest)
	case len(i.EncryptedKey) != slabs.EncryptionKeySize:
		return fmt.Errorf("%w: encrypted key must be %d bytes", ErrInvalidRequest, slabs.EncryptionKeySize)
	case now.Sub(i.CreatedAt) > 5*time.Minute:
		return ErrInvitationTimestampTooOld
	case i.CreatedAt.Sub(now) > 5*time.Minute:
		return ErrInvitationTimestampInFuture
	case !i.ExpiresAt.After(now):
		return ErrInvitationExpired
	case !i.ExpiresAt.After(i.CreatedAt):
		return fmt.Errorf("%w: expiration must be after creation", ErrInvalidRequest)
	case !types.PublicKey(i.Inviter).VerifyHash(i.SigHash(), i.Signature):
		return ErrInvalidSignature
	}
	return nil
}

// SigHash returns the hash of the invitation's space, inviter, recipient,
// permission, encrypted key, and creation and expiration times in Unix microseconds.
func (i Invitation) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteString("indexd/space/invitation/v1")
	i.SpaceKey.EncodeTo(h.E)
	types.PublicKey(i.Inviter).EncodeTo(h.E)
	types.PublicKey(i.Recipient).EncodeTo(h.E)
	h.E.WriteUint8(uint8(i.Permission))
	h.E.WriteBytes(i.EncryptedKey)
	h.E.WriteUint64(uint64(i.CreatedAt.UnixMicro()))
	h.E.WriteUint64(uint64(i.ExpiresAt.UnixMicro()))
	return h.Sum()
}

// Member is an active member of a space.
type Member struct {
	Account    proto.Account `json:"account"`
	InvitedBy  proto.Account `json:"invitedBy"`
	InvitedAt  time.Time     `json:"invitedAt"`
	Permission Permission    `json:"permission"`
	CreatedAt  time.Time     `json:"createdAt"`
	UpdatedAt  time.Time     `json:"updatedAt"`
}

// Object is an encrypted object in a space and the account that signed it.
type Object struct {
	Key     types.Hash256 `json:"key"`
	Account proto.Account `json:"account"`
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
