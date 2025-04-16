package slabs

import (
	"errors"
	"fmt"

	"go.sia.tech/core/types"
)

var (
	// ErrSlabExists is returned when a slab already exists in the database.
	ErrSlabExists = errors.New("slab already exists")

	// ErrSlabNotFound is returned when a slab is not found in the database.
	ErrSlabNotFound = errors.New("slab not found")
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
		ID            SlabID   `json:"id"`
		EncryptionKey [32]byte `json:"encryptionKey"`
		MinShards     uint     `json:"minShards"`
		Sectors       []Sector `json:"sectors"`
	}

	// SectorPinParams describes an uploaded sector to be pinned.
	SectorPinParams struct {
		Root    types.Hash256   `json:"root"`
		HostKey types.PublicKey `json:"hostKey"`
	}

	// SlabPinParams is the input to PinSlabs
	SlabPinParams struct {
		EncryptionKey [32]byte          `json:"encryptionKey"`
		MinShards     uint              `json:"minShards"`
		Sectors       []SectorPinParams `json:"sectors"`
	}
)

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

// String implements the Stringer interface for SlabID.
func (s SlabID) String() string {
	return types.Hash256(s).String()
}
