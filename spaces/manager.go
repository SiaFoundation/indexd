package spaces

import (
	"fmt"
	"math"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

type (
	// Store defines the persistence layer the Manager depends on.
	Store interface {
		AddSpace(account proto.Account, req AddSpaceRequest) error
		Spaces(account proto.Account, offset, limit int) ([]Space, error)
		Space(account proto.Account, key types.PublicKey) (Space, error)
		UpdateSpace(account proto.Account, key types.PublicKey, name, metadata []byte, signature types.Signature) error
		DeleteSpace(account proto.Account, key types.PublicKey) error
		InviteSpaceMember(invite Invitation) error
		SpaceInvitations(account proto.Account, offset, limit int) ([]Invitation, error)
		AcceptSpaceInvitation(account proto.Account, key types.PublicKey, encryptedKey []byte) error
		DeleteSpaceInvitation(account proto.Account, key types.PublicKey, recipient proto.Account) error
		SpaceMembers(account proto.Account, key types.PublicKey, offset, limit int) ([]Member, error)
		UpdateSpaceMember(account proto.Account, key types.PublicKey, member proto.Account, permission Permission) error
		DeleteSpaceMember(account proto.Account, key types.PublicKey, member proto.Account) error
		PinSpaceSlabs(account proto.Account, key types.PublicKey, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) ([]slabs.SlabID, error)
		PinSpaceObject(account proto.Account, key types.PublicKey, obj slabs.PinObjectRequest) error
		DeleteSpaceObject(account proto.Account, key types.PublicKey, objectKey types.Hash256) error
		SpaceObject(account proto.Account, key types.PublicKey, objectKey types.Hash256) (Object, error)
		SpaceObjectSlabs(account proto.Account, key types.PublicKey, objectKey types.Hash256, cursor int64, limit int) ([]slabs.SlabSlice, error)
		ListSpaceObjects(account proto.Account, key types.PublicKey, cursor slabs.Cursor, limit int) ([]ObjectEvent, error)
		ListSpaceObjectsWithoutSlabs(account proto.Account, key types.PublicKey, cursor slabs.Cursor, limit int) ([]ObjectEventWithoutSlabs, error)
	}

	// Manager validates requests for spaces, memberships, and objects.
	Manager struct {
		store Store
	}
)

// NewManager creates a spaces manager.
func NewManager(store Store) *Manager {
	return &Manager{store: store}
}

// AddSpace creates a space and its owner membership.
func (m *Manager) AddSpace(account proto.Account, req AddSpaceRequest) error {
	if err := req.Validate(account); err != nil {
		return err
	}
	return m.store.AddSpace(account, req)
}

// Spaces returns a page of spaces accessible to account.
func (m *Manager) Spaces(account proto.Account, offset, limit int) ([]Space, error) {
	if err := validatePublicKey("account", types.PublicKey(account)); err != nil {
		return nil, err
	} else if err := validatePagination(offset, limit); err != nil {
		return nil, err
	}
	return m.store.Spaces(account, offset, limit)
}

// Space returns a space and the account's wrapped space key.
func (m *Manager) Space(account proto.Account, key types.PublicKey) (Space, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return Space{}, err
	}
	return m.store.Space(account, key)
}

// UpdateSpace replaces the encrypted name, metadata, and shared signature.
func (m *Manager) UpdateSpace(account proto.Account, key types.PublicKey, name, metadata []byte, signature types.Signature) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if err := validateEncryptedFields(name, metadata); err != nil {
		return err
	} else if signature == (types.Signature{}) {
		return ErrInvalidSignature
	}
	space, err := m.store.Space(account, key)
	if err != nil {
		return err
	}
	updated := Space{Key: key, Owner: space.Owner, EncryptedName: name, EncryptedMetadata: metadata}
	if !key.VerifyHash(updated.SigHash(), signature) {
		return ErrInvalidSignature
	}
	return m.store.UpdateSpace(account, key, name, metadata, signature)
}

// DeleteSpace removes a space owned by account.
func (m *Manager) DeleteSpace(account proto.Account, key types.PublicKey) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	}
	return m.store.DeleteSpace(account, key)
}

// InviteSpaceMember records an invitation signed by the authenticated account.
func (m *Manager) InviteSpaceMember(account proto.Account, invite Invitation) error {
	if account != invite.Inviter {
		return ErrPermission
	} else if err := invite.Validate(time.Now()); err != nil {
		return err
	}
	return m.store.InviteSpaceMember(invite)
}

// SpaceInvitations returns a page of pending invitations for account.
func (m *Manager) SpaceInvitations(account proto.Account, offset, limit int) ([]Invitation, error) {
	if err := validatePublicKey("account", types.PublicKey(account)); err != nil {
		return nil, err
	} else if err := validatePagination(offset, limit); err != nil {
		return nil, err
	}
	return m.store.SpaceInvitations(account, offset, limit)
}

// AcceptSpaceInvitation accepts an invitation with the space key rewrapped for account.
func (m *Manager) AcceptSpaceInvitation(account proto.Account, key types.PublicKey, encryptedKey []byte) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if len(encryptedKey) != slabs.EncryptionKeySize {
		return fmt.Errorf("%w: encrypted key must be %d bytes", ErrInvalidRequest, slabs.EncryptionKeySize)
	}
	return m.store.AcceptSpaceInvitation(account, key, encryptedKey)
}

// DeleteSpaceInvitation cancels or declines a pending invitation.
func (m *Manager) DeleteSpaceInvitation(account proto.Account, key types.PublicKey, recipient proto.Account) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if err := validatePublicKey("recipient", types.PublicKey(recipient)); err != nil {
		return err
	}
	return m.store.DeleteSpaceInvitation(account, key, recipient)
}

// SpaceMembers returns a page of active members.
func (m *Manager) SpaceMembers(account proto.Account, key types.PublicKey, offset, limit int) ([]Member, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return nil, err
	} else if err := validatePagination(offset, limit); err != nil {
		return nil, err
	}
	return m.store.SpaceMembers(account, key, offset, limit)
}

// UpdateSpaceMember changes a member's permissions.
func (m *Manager) UpdateSpaceMember(account proto.Account, key types.PublicKey, member proto.Account, permission Permission) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if err := validatePublicKey("member", types.PublicKey(member)); err != nil {
		return err
	} else if permission < Read || permission > Admin {
		return fmt.Errorf("%w: invalid member permission", ErrInvalidRequest)
	}
	return m.store.UpdateSpaceMember(account, key, member, permission)
}

// DeleteSpaceMember revokes a membership or leaves a space.
func (m *Manager) DeleteSpaceMember(account proto.Account, key types.PublicKey, member proto.Account) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if err := validatePublicKey("member", types.PublicKey(member)); err != nil {
		return err
	}
	return m.store.DeleteSpaceMember(account, key, member)
}

// PinSpaceSlabs pins slabs against the space owner's quota.
func (m *Manager) PinSpaceSlabs(account proto.Account, key types.PublicKey, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) ([]slabs.SlabID, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return nil, err
	} else if len(toPin) == 0 {
		return nil, fmt.Errorf("%w: at least one slab is required", slabs.ErrInvalidPinParams)
	}
	now := time.Now()
	for i, slab := range toPin {
		if err := slab.Validate(now); err != nil {
			return nil, fmt.Errorf("%w: slab %d: %w", slabs.ErrInvalidPinParams, i, err)
		}
		for _, sector := range slab.Sectors {
			if err := validatePublicKey("host", sector.HostKey); err != nil {
				return nil, fmt.Errorf("%w: slab %d: %w", slabs.ErrInvalidPinParams, i, err)
			}
		}
	}
	return m.store.PinSpaceSlabs(account, key, nextIntegrityCheck, toPin...)
}

// PinSpaceObject adds or replaces an object signed by account.
func (m *Manager) PinSpaceObject(account proto.Account, key types.PublicKey, obj slabs.PinObjectRequest) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if err := obj.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	} else if err := obj.VerifySignatures(types.PublicKey(account)); err != nil {
		return err
	}
	return m.store.PinSpaceObject(account, key, obj)
}

// DeleteSpaceObject removes an object and releases unused storage.
func (m *Manager) DeleteSpaceObject(account proto.Account, key types.PublicKey, objectKey types.Hash256) error {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return err
	} else if err := validateObjectKey(objectKey); err != nil {
		return err
	}
	return m.store.DeleteSpaceObject(account, key, objectKey)
}

// SpaceObject returns an object accessible to account.
func (m *Manager) SpaceObject(account proto.Account, key types.PublicKey, objectKey types.Hash256) (Object, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return Object{}, err
	} else if err := validateObjectKey(objectKey); err != nil {
		return Object{}, err
	}
	return m.store.SpaceObject(account, key, objectKey)
}

// SpaceObjectSlabs returns slab slices starting at cursor.
func (m *Manager) SpaceObjectSlabs(account proto.Account, key types.PublicKey, objectKey types.Hash256, cursor int64, limit int) ([]slabs.SlabSlice, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return nil, err
	} else if err := validateObjectKey(objectKey); err != nil {
		return nil, err
	} else if cursor < 0 || cursor > math.MaxInt32 {
		return nil, fmt.Errorf("%w: invalid slab cursor", ErrInvalidRequest)
	} else if err := validatePagination(0, limit); err != nil {
		return nil, err
	}
	return m.store.SpaceObjectSlabs(account, key, objectKey, cursor, limit)
}

// ListSpaceObjects returns published object changes after cursor.
func (m *Manager) ListSpaceObjects(account proto.Account, key types.PublicKey, cursor slabs.Cursor, limit int) ([]ObjectEvent, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return nil, err
	} else if err := validatePagination(0, limit); err != nil {
		return nil, err
	}
	return m.store.ListSpaceObjects(account, key, cursor, limit)
}

// ListSpaceObjectsWithoutSlabs returns published changes without slab slices.
func (m *Manager) ListSpaceObjectsWithoutSlabs(account proto.Account, key types.PublicKey, cursor slabs.Cursor, limit int) ([]ObjectEventWithoutSlabs, error) {
	if err := validateAccountAndSpaceKeys(account, key); err != nil {
		return nil, err
	} else if err := validatePagination(0, limit); err != nil {
		return nil, err
	}
	return m.store.ListSpaceObjectsWithoutSlabs(account, key, cursor, limit)
}
