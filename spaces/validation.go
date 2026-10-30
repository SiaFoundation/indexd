package spaces

import (
	"errors"
	"fmt"

	"filippo.io/edwards25519"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

const (
	// MaxNameSize is the maximum length of an encrypted space name.
	MaxNameSize = 1024
	// MaxMetadataSize is the maximum length of encrypted space metadata.
	MaxMetadataSize = slabs.MaxMetadataSize
	// MaxLimit is the maximum number of entries returned in a page.
	MaxLimit = 500
)

// ErrInvalidRequest is returned when a space request fails validation.
var ErrInvalidRequest = errors.New("invalid space request")

func validatePublicKey(name string, key types.PublicKey) error {
	point, err := new(edwards25519.Point).SetBytes(key[:])
	switch {
	case err != nil:
		return fmt.Errorf("%w: %s is not a valid Ed25519 public key", ErrInvalidRequest, name)
	case new(edwards25519.Point).MultByCofactor(point).Equal(edwards25519.NewIdentityPoint()) == 1:
		return fmt.Errorf("%w: %s is a low-order public key", ErrInvalidRequest, name)
	}
	return nil
}

func validateAccountAndSpaceKeys(account proto.Account, key types.PublicKey) error {
	if err := validatePublicKey("account", types.PublicKey(account)); err != nil {
		return err
	}
	return validatePublicKey("space key", key)
}

func validateObjectKey(key types.Hash256) error {
	if key == (types.Hash256{}) {
		return fmt.Errorf("%w: object key is empty", ErrInvalidRequest)
	}
	return nil
}

func validatePagination(offset, limit int) error {
	switch {
	case offset < 0:
		return fmt.Errorf("%w: offset must be non-negative", ErrInvalidRequest)
	case limit < 1 || limit > MaxLimit:
		return fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidRequest, MaxLimit)
	}
	return nil
}

func validateEncryptedFields(name, metadata []byte) error {
	switch {
	case len(name) > MaxNameSize:
		return fmt.Errorf("%w: encrypted name exceeds %d bytes", ErrInvalidRequest, MaxNameSize)
	case len(metadata) > MaxMetadataSize:
		return fmt.Errorf("%w: encrypted metadata exceeds %d bytes", ErrInvalidRequest, MaxMetadataSize)
	}
	return nil
}

// Validate checks the request fields and the shared space signature.
func (r AddSpaceRequest) Validate(owner proto.Account) error {
	if err := validateAccountAndSpaceKeys(owner, r.Key); err != nil {
		return err
	} else if err := validateEncryptedFields(r.EncryptedName, r.EncryptedMetadata); err != nil {
		return err
	}
	switch {
	case len(r.EncryptedKey) != slabs.EncryptionKeySize:
		return fmt.Errorf("%w: encrypted key must be %d bytes", ErrInvalidRequest, slabs.EncryptionKeySize)
	case !r.Key.VerifyHash(r.SigHash(owner), r.Signature):
		return ErrInvalidSignature
	}
	return nil
}
