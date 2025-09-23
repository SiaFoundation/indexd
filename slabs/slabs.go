package slabs

import (
	"context"
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

const (
	// DefaultRedundancy is the default minimum redundancy for slabs.
	DefaultRedundancy = 3

	// MaxTotalShards is the maximum number of total shards (data + parity) allowed in a slab.
	MaxTotalShards = 256
)

var (
	// ErrInsufficientRedundancy is returned when the minimum redundancy of slabs is not met.
	ErrInsufficientRedundancy = errors.New("insufficient redundancy")

	// ErrSlabNotFound is returned when a slab is not found in the database.
	ErrSlabNotFound = errors.New("slab not found")

	// ErrUnrecoverable is returned when a slab is unrecoverable, meaning it cannot be repaired or migrated.
	ErrUnrecoverable = errors.New("slab is unrecoverable")
)

type (
	// SlabID is the ID of a slab derived from the slab's sectors' roots.
	SlabID types.Hash256

	// Sector is a 4MiB sector stored on a host.
	Sector struct {
		// Root is the sector root of a 4MiB sector
		Root types.Hash256 `json:"root"`

		// ContractID is the ID of the contract the sector is pinned to.
		// 'nil' if the sector isn't pinned.
		ContractID *types.FileContractID `json:"contractID,omitempty"`

		// HostKey is the public key of the host that stores the sector data.
		// 'nil' if a host lost the sector and the sector requires migration
		HostKey *types.PublicKey `json:"host,omitempty"`
	}

	// Slab is a group of sectors that is encrypted, erasure-coded and uploaded
	// to hosts.
	Slab struct {
		ID            SlabID    `json:"id"`
		EncryptionKey [32]byte  `json:"encryptionKey"`
		MinShards     uint      `json:"minShards"`
		Sectors       []Sector  `json:"sectors"`
		PinnedAt      time.Time `json:"pinnedAt"`
	}

	// A PinnedSector is a sector that has been pinned to a host.
	PinnedSector struct {
		Root    types.Hash256   `json:"root"`
		HostKey types.PublicKey `json:"hostKey"`
	}

	// SlabPinParams is the input to PinSlabs
	SlabPinParams struct {
		EncryptionKey [32]byte       `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
	}

	// A PinnedSlab is a slab that has been pinned to hosts.
	PinnedSlab struct {
		ID            SlabID         `json:"id"`
		EncryptionKey [32]byte       `json:"encryptionKey"`
		MinShards     uint           `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
	}
)

// String implements the Stringer interface for SlabID.
func (s SlabID) String() string {
	return types.Hash256(s).String()
}

// MarshalText implements encoding.TextMarshaler.
func (s SlabID) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *SlabID) UnmarshalText(b []byte) error {
	return (*types.Hash256)(s).UnmarshalText(b)
}

// Digest creates a unique digest for the slab to be pinned by SlabPinParams. It
// is important, that the same params always result in the same hash since we
// deduplicate slabs using it. So if one user makes the mistake of pinning a
// slab with a different encryption key, this shouldn't prevent other users from
// pinning the same slab with the correct key.
func (s SlabPinParams) Digest() (SlabID, error) {
	hasher := types.NewHasher()
	hasher.E.WriteUint64(uint64(s.MinShards))
	hasher.E.Write(s.EncryptionKey[:])
	for _, sector := range s.Sectors {
		if _, err := hasher.E.Write(sector.Root[:]); err != nil {
			return SlabID{}, fmt.Errorf("failed to write sector root to hasher: %w", err)
		}
	}
	return SlabID(hasher.Sum()), nil
}

// Size returns the size of the slab in bytes including redundancy.
func (s SlabPinParams) Size() uint64 {
	return uint64(len(s.Sectors)) * proto.SectorSize
}

// Validate checks if the SlabPinParams are valid. It ensures that the
// encryption key is set, the minimum number of shards is met, and that there
// are no duplicate host keys or empty roots in the sectors.
func (s SlabPinParams) Validate() error {
	if s.EncryptionKey == ([32]byte{}) {
		return errors.New("encryption key is empty")
	} else if len(s.Sectors) < int(s.MinShards*DefaultRedundancy) {
		return fmt.Errorf("%w: minimum redundancy of %dx is not met", ErrInsufficientRedundancy, DefaultRedundancy)
	} else if len(s.Sectors) > MaxTotalShards {
		return fmt.Errorf("total number of shards %d exceeds maximum of %d", len(s.Sectors), MaxTotalShards)
	}

	hks := make(map[types.PublicKey]struct{}, len(s.Sectors))
	for i, sector := range s.Sectors {
		if sector.Root == (types.Hash256{}) {
			return errors.New("sector root is empty")
		} else if sector.HostKey == (types.PublicKey{}) {
			return fmt.Errorf("sector %d host key is empty", i)
		} else if _, exists := hks[sector.HostKey]; exists {
			return fmt.Errorf("duplicate host key %s in slab pin params", sector.HostKey)
		}
		hks[sector.HostKey] = struct{}{}
	}

	return nil
}

// PinSlab pins the given slab and associates it with the given account.
func (m *SlabManager) PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab SlabPinParams) (SlabID, error) {
	return m.store.PinSlab(ctx, account, nextIntegrityCheck, slab)
}

// Slabs returns the slabs with the given IDs from the database.
func (m *SlabManager) Slabs(ctx context.Context, accountID proto.Account, slabIDs []SlabID) ([]Slab, error) {
	return m.store.Slabs(ctx, accountID, slabIDs)
}
