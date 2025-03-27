package contracts

import (
	"context"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
)

type renewContractCall struct {
	hk          types.PublicKey
	addr        string
	settings    proto.HostSettings
	contractID  types.FileContractID
	proofHeight uint64
}

func (c *contractorMock) RenewContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	c.renewCalls = append(c.renewCalls, renewContractCall{
		hk:          hk,
		addr:        addr,
		settings:    settings,
		contractID:  contractID,
		proofHeight: proofHeight,
	})
	return rhp.RPCRenewContractResult{
		RenewalSet: rhp.TransactionSet{
			Transactions: []types.V2Transaction{
				{
					MinerFee: types.Siacoins(1),
				},
			},
		},
	}, nil
}
