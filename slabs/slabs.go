package slabs

import (
	"context"
	"errors"

	proto "go.sia.tech/core/rhp/v4"
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

// String implements the Stringer interface for SlabID.
func (s SlabID) String() string {
	return types.Hash256(s).String()
}

// PinSlabs pins the given slabs and associates them with the given account.
func (m *SlabManager) PinSlabs(ctx context.Context, account proto.Account, slabs []SlabPinParams) ([]SlabID, error) {
	return m.store.PinSlabs(ctx, account, slabs)
}

// Slabs returns the slabs with the given IDs from the database.
func (m *SlabManager) Slabs(ctx context.Context, accountID proto.Account, slabIDs []SlabID) ([]Slab, error) {
	return m.store.Slabs(ctx, accountID, slabIDs)
}
