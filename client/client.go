package client

import (
	"context"
	"errors"
	"fmt"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

const (
	maxContractSize = 10 * 1 << 40 // 10TB
)

var (
	// ErrContractInsufficientFunds is returned when we try to revise a contract
	// that has insufficient funds to cover the action we want to perform.
	ErrContractInsufficientFunds = errors.New("contract has insufficient funds")

	// ErrContractNotRevisable is returned when we try to revise a contract on
	// the host that's too close to the proof height and thus deemed unrevisable
	// by the host.
	ErrContractNotRevisable = errors.New("contract is not revisable")

	// ErrContractOutOfFunds is returned when we try to perform an action on a
	// contract that has no funds left to cover the action.
	ErrContractOutOfFunds = errors.New("contract is out of funds")

	// ErrContractRenewed is returned when we try to revise a contract that has
	// already been renewed.
	ErrContractRenewed = errors.New("contract got renewed")
)

type (
	// ChainManager defines an interface to access the chain state as well as
	// interact with the transaction pool.
	ChainManager interface {
		TipState() consensus.State
		rhp.TxPool
	}
)

// HostClient is a client that can be used to interact with a host using the RHP
// methods. It provides methods to form contracts, append sectors, free sectors,
// get sector roots, refresh and renew contracts, and replenish accounts. It
// uses a transport client to communicate with the host and a signer to sign the
// contract revisions. The client is expected to be closed when no longer
// needed.
type HostClient struct {
	hostKey types.PublicKey

	client rhp.TransportClient
	signer rhp.FormContractSigner

	cm  ChainManager
	log *zap.Logger
}

// newHostClient creates a new HostClient that can be used to interact with a
// host using the RHP methods. The client is expected to be closed when no
// longer needed.
func newHostClient(hk types.PublicKey, cm ChainManager, client rhp.TransportClient, signer rhp.FormContractSigner, log *zap.Logger) *HostClient {
	return &HostClient{
		hostKey: hk,

		client: client,
		signer: signer,

		cm:  cm,
		log: log.Named("client").With(zap.Stringer("hostKey", hk)),
	}
}

// AppendSectors appends the given sectors to the contract.
func (c *HostClient) AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	// sanity check
	if len(sectors) > proto.MaxSectorBatchSize {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("too many sectors, %d > %d", len(sectors), proto.MaxSectorBatchSize) // developer error
	}

	// fetch revision and check if it meets the requirements
	rev, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
	if err != nil {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !rev.Revisable {
		return rhp.RPCAppendSectorsResult{}, errors.New("contract is not revisable")
	} else if rev.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCAppendSectorsResult{}, errors.New("contract is out of funds")
	} else if rev.Contract.Filesize > maxContractSize {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("contract is too large, %d > %d", rev.Contract.Filesize, maxContractSize)
	}

	// append sectors
	revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
	return rhp.RPCAppendSectors(ctx, c.client, c.signer, c.cm.TipState(), hostPrices, revision, sectors)
}

// Close closes the underlying transport client.
func (c *HostClient) Close() error {
	return c.client.Close()
}

// FormContract forms a new contract with the host.
func (c *HostClient) FormContract(ctx context.Context, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	res, err := rhp.RPCFormContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, c.hostKey, settings.WalletAddress, params)
	if err != nil {
		return rhp.RPCFormContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

// SectorRoots returns the sector roots for a contract.
func (c *HostClient) SectorRoots(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	// fetch revision and check if it meets the requirements
	rev, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
	if err != nil {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !rev.Revisable {
		return rhp.RPCSectorRootsResult{}, errors.New("contract is not revisable")
	} else if rev.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCSectorRootsResult{}, errors.New("contract is out of funds")
	}

	// fetch contract sectors
	revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
	return rhp.RPCSectorRoots(ctx, c.client, c.cm.TipState(), hostPrices, c.signer, revision, offset, length)
}

// FreeSectors frees the specified sectors in the contract.
func (c *HostClient) FreeSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	// fetch revision and check if it meets the requirements
	rev, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
	if err != nil {
		return rhp.RPCFreeSectorsResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !rev.Revisable {
		return rhp.RPCFreeSectorsResult{}, errors.New("contract is not revisable")
	} else if rev.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCFreeSectorsResult{}, errors.New("contract is out of funds")
	}

	// free sectors
	revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
	return rhp.RPCFreeSectors(ctx, c.client, c.signer, c.cm.TipState(), hostPrices, revision, indices)
}

// RefreshContract refreshes the contract with the host.
func (c *HostClient) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	rev, err := rhp.RPCLatestRevision(ctx, c.client, params.ContractID)
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if rev.Renewed {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("contract already renewed")
	} else if !rev.Revisable {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("contract not revisable")
	}

	res, err := rhp.RPCRefreshContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, rev.Contract, proto.RPCRefreshContractParams{
		Allowance:  rev.Contract.RenterOutput.Value,
		Collateral: rev.Contract.MissedHostValue,
	})
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

// RenewContract renews the contract with the host.
func (c *HostClient) RenewContract(ctx context.Context, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	rev, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
	if err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if rev.Renewed {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("contract already renewed")
	} else if !rev.Revisable {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("contract not revisable")
	}

	// NOTE: when renewing a contract we keep the same allowance and collateral.
	// This has the following advantages:
	// 1. Contracts drain over time if they contain more funds than needed
	// 2. Renewals are very "cheap" since no party needs to lock away
	//    additional funds. Only the fees need to be paid.
	res, err := rhp.RPCRenewContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, rev.Contract, proto.RPCRenewContractParams{
		ContractID:  contractID,
		Allowance:   rev.Contract.RenterOutput.Value,
		Collateral:  rev.Contract.MissedHostValue,
		ProofHeight: proofHeight,
	})
	if err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

// ReplenishAccounts replenishes the accounts in the contract to the target value.
func (c *HostClient) ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (res rhp.RPCReplenishAccountsResult, funded int, _ error) {
	rev, err := c.LatestRevision(ctx, contractID)
	if err != nil {
		return rhp.RPCReplenishAccountsResult{}, 0, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if rev.Contract.RenterOutput.Value.Cmp(target) < 0 {
		return rhp.RPCReplenishAccountsResult{}, 0, ErrContractInsufficientFunds
	}

	// prepare batch
	batchSize := int(min(rev.Contract.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize))
	funded = min(batchSize, len(accounts))
	batch := accounts[:funded]

	// prepare parameters
	params := rhp.RPCReplenishAccountsParams{
		Accounts: batch,
		Target:   target,
		Contract: rhp.ContractRevision{ID: contractID, Revision: rev.Contract},
	}
	res, err = rhp.RPCReplenishAccounts(ctx, c.client, params, c.cm.TipState(), c.signer)
	if err != nil {
		return rhp.RPCReplenishAccountsResult{}, 0, err
	}

	return res, funded, nil
}

// LatestRevision retrieves the latest revision of a contract from the host.
func (c *HostClient) LatestRevision(ctx context.Context, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return rhp.RPCLatestRevision(ctx, c.client, contractID)
}
