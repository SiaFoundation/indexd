package contracts

import (
	"context"

	"go.sia.tech/core/types"
)

func (s *storeMock) ContractRevision(ctx context.Context, contractID types.FileContractID) (types.V2FileContract, bool, error) {
	var renewed bool
	for i, c := range s.contracts {
		if c.ID == contractID {
			renewed = c.RenewedTo != (types.FileContractID{})
			return s.revisions[i], renewed, nil
		}
	}
	return types.V2FileContract{}, false, nil
}

func (s *storeMock) UpdateContractRevision(ctx context.Context, contractID types.FileContractID, revision types.V2FileContract) error {
	// Capacity:           resp.Contract.Capacity,
	// RemainingAllowance: resp.Contract.RenterOutput.Value,
	// RevisionNumber:     resp.Contract.RevisionNumber,
	// Size:               resp.Contract.Filesize,
	// UsedCollateral:     resp.Contract.MissedHostValue,
	return nil
}
