package contracts

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
)

func (s *storeMock) ContractsForPruning(ctx context.Context, hk types.PublicKey, maxLastPrune time.Time) ([]types.FileContractID, error) {
	return nil, nil
}

func (s *storeMock) MarkPruned(ctx context.Context, contractID types.FileContractID) error {
	return nil
}

func (s *storeMock) PrunableContractRoots(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID, roots []types.Hash256) ([]types.Hash256, error) {
	return nil, nil
}

func (c *hostClientMock) ContractSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	return rhp.RPCSectorRootsResult{}, nil
}

func (c *hostClientMock) PruneSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	return rhp.RPCFreeSectorsResult{}, nil
}

func (c *hostClientMock) HostKey() types.PublicKey {
	return types.PublicKey{}
}
