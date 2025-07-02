package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

const (
	maxContractSize = 10 * 1 << 40 // 10TB

	// revisionSubmissionBuffer is a buffer that the host applies on the contract's
	// proof height before it considers the contract revisable, so if the current
	// block height plus the buffer exceed the proof height, the contract is not
	// revisable.
	revisionSubmissionBuffer = 144
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

	// RevisionStore defines an interface that allows fetching and updating a
	// contract's revision.
	RevisionStore interface {
		ContractRevision(ctx context.Context, contractID types.FileContractID) (types.V2FileContract, bool, error)
		UpdateContractRevision(ctx context.Context, contractID types.FileContractID, revision types.V2FileContract) error
	}
)

type rpcLatestRevisionFn func(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error)

// HostClient is a client that can be used to interact with a host using the RHP
// methods. It provides methods to form contracts, append sectors, free sectors,
// get sector roots, refresh and renew contracts, and replenish accounts. It
// uses a transport client to communicate with the host and a signer to sign the
// contract revisions. The client is expected to be closed when no longer
// needed.
type HostClient struct {
	hostKey types.PublicKey

	client           rhp.TransportClient
	signer           rhp.FormContractSigner
	latestRevisionFn rpcLatestRevisionFn

	cm    ChainManager
	store RevisionStore
	log   *zap.Logger
}

// newHostClient creates a new HostClient that can be used to interact with a
// host using the RHP methods. The client is expected to be closed when no
// longer needed.
func newHostClient(hk types.PublicKey, cm ChainManager, client rhp.TransportClient, signer rhp.FormContractSigner, store RevisionStore, log *zap.Logger) *HostClient {
	return &HostClient{
		hostKey: hk,

		client:           client,
		signer:           signer,
		store:            store,
		latestRevisionFn: rhp.RPCLatestRevision, // allows mocking in tests

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

	// append sectors
	var res rhp.RPCAppendSectorsResult
	if err := c.withRevision(ctx, contractID, func(revision types.V2FileContract) (types.V2FileContract, error) {
		if revision.Filesize > maxContractSize {
			return types.V2FileContract{}, fmt.Errorf("contract is too large, %d > %d", revision.Filesize, maxContractSize)
		}
		res, err := rhp.RPCAppendSectors(ctx, c.client, c.signer, c.cm.TipState(), hostPrices, rhp.ContractRevision{ID: contractID, Revision: revision}, sectors)
		if err != nil {
			return types.V2FileContract{}, fmt.Errorf("failed to append sectors: %w", err)
		}
		return res.Revision, nil
	}); err != nil {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("failed to append sectors: %w", err)
	}

	return res, nil
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
	var res rhp.RPCSectorRootsResult
	if err := c.withRevision(ctx, contractID, func(revision types.V2FileContract) (types.V2FileContract, error) {
		res, err := rhp.RPCSectorRoots(ctx, c.client, c.cm.TipState(), hostPrices, c.signer, rhp.ContractRevision{ID: contractID, Revision: revision}, offset, length)
		if err != nil {
			return types.V2FileContract{}, fmt.Errorf("failed to fetch sector roots: %w", err)
		}
		return res.Revision, nil
	}); err != nil {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("failed to fetch sector roots: %w", err)
	}
	return res, nil
}

// FreeSectors frees the specified sectors in the contract.
func (c *HostClient) FreeSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	var res rhp.RPCFreeSectorsResult
	if err := c.withRevision(ctx, contractID, func(revision types.V2FileContract) (types.V2FileContract, error) {
		res, err := rhp.RPCFreeSectors(ctx, c.client, c.signer, c.cm.TipState(), hostPrices, rhp.ContractRevision{ID: contractID, Revision: revision}, indices)
		if err != nil {
			return types.V2FileContract{}, fmt.Errorf("failed to free sectors: %w", err)
		}
		return res.Revision, nil
	}); err != nil {
		return rhp.RPCFreeSectorsResult{}, fmt.Errorf("failed to fetch free sectors: %w", err)
	}
	return res, nil
}

// RefreshContract refreshes the contract with the host.
func (c *HostClient) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	var res rhp.RPCRefreshContractResult
	if err := c.withRevision(ctx, params.ContractID, func(revision types.V2FileContract) (_ types.V2FileContract, err error) {
		res, err = rhp.RPCRefreshContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, revision, proto.RPCRefreshContractParams{
			Allowance:  revision.RenterOutput.Value,
			Collateral: revision.MissedHostValue,
		})
		if err != nil {
			return types.V2FileContract{}, err
		}
		return res.Contract.Revision, nil
	}); err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to refresh contract: %w", err)
	}
	return res, nil
}

// RenewContract renews the contract with the host.
func (c *HostClient) RenewContract(ctx context.Context, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	var res rhp.RPCRenewContractResult
	if err := c.withRevision(ctx, contractID, func(revision types.V2FileContract) (types.V2FileContract, error) {
		// NOTE: when renewing a contract we keep the same allowance and collateral.
		// This has the following advantages:
		// 1. Contracts drain over time if they contain more funds than needed
		// 2. Renewals are very "cheap" since no party needs to lock away
		//    additional funds. Only the fees need to be paid.
		res, err := rhp.RPCRenewContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, revision, proto.RPCRenewContractParams{
			ContractID:  contractID,
			Allowance:   revision.RenterOutput.Value,
			Collateral:  revision.MissedHostValue,
			ProofHeight: proofHeight,
		})
		if err != nil {
			return types.V2FileContract{}, err
		}
		return res.Contract.Revision, nil
	}); err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to renew contract: %w", err)
	}
	return res, nil
}

// ReplenishAccounts replenishes the accounts in the contract to the target value.
func (c *HostClient) ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (res rhp.RPCReplenishAccountsResult, funded int, _ error) {
	if err := c.withRevision(ctx, contractID, func(revision types.V2FileContract) (types.V2FileContract, error) {
		if revision.RenterOutput.Value.Cmp(target) < 0 {
			return types.V2FileContract{}, ErrContractInsufficientFunds
		}

		// prepare batch
		batchSize := int(min(revision.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize))
		funded = min(batchSize, len(accounts))
		batch := accounts[:funded]

		// prepare parameters
		params := rhp.RPCReplenishAccountsParams{
			Accounts: batch,
			Target:   target,
			Contract: rhp.ContractRevision{ID: contractID, Revision: revision},
		}
		res, err := rhp.RPCReplenishAccounts(ctx, c.client, params, c.cm.TipState(), c.signer)
		if err != nil {
			return types.V2FileContract{}, err
		}
		return res.Revision, nil
	}); err != nil {
		return rhp.RPCReplenishAccountsResult{}, 0, fmt.Errorf("failed to replenish accounts: %w", err)
	}
	return res, funded, nil
}

func (c *HostClient) syncRevision(ctx context.Context, contractID types.FileContractID, revision types.V2FileContract) (types.V2FileContract, bool, error) {
	// apply a sane timeout for syncing the revision
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch latest revision
	resp, err := c.latestRevisionFn(ctx, c.client, contractID)
	if err != nil {
		c.log.Debug("failed to fetch latest revision", zap.Error(err))
		return types.V2FileContract{}, false, fmt.Errorf("%w; failed to fetch latest revision", err)
	} else if resp.Contract.RevisionNumber < revision.RevisionNumber {
		return types.V2FileContract{}, false, errors.New("local revision is newer than host revision")
	}

	// update latest revision
	err = c.store.UpdateContractRevision(ctx, contractID, resp.Contract)
	if err != nil {
		c.log.Error("failed to update contract revision", zap.Stringer("contractID", contractID), zap.Error(err))
	}

	return resp.Contract, resp.Renewed, nil
}

// withRevision retrieves the current revision of the specified contract ID from
// the database and executes the provided revise function with it. If the host
// reports an invalid signature, suggesting the local revision is out of sync,
// it will synchronize with the host and retry the function using the updated
// revision. Therefore, the revise function must be idempotent.
func (c *HostClient) withRevision(ctx context.Context, contractID types.FileContractID, reviseFn func(revision types.V2FileContract) (types.V2FileContract, error)) error {
	cs := c.cm.TipState()
	bh := cs.Index.Height
	maxProofHeight := bh + revisionSubmissionBuffer

	// fetch revision from database
	rev, renewed, err := c.store.ContractRevision(ctx, contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		return ErrContractRenewed
	} else if rev.ProofHeight > maxProofHeight {
		return fmt.Errorf("%d > %d (%d+%d), %w", rev.ProofHeight, maxProofHeight, bh, revisionSubmissionBuffer, ErrContractNotRevisable)
	}

	// revise the contract
	update, err := reviseFn(rev)

	// try and sync the revision if we got an error that indicates the revision is invalid
	if err != nil && strings.Contains(err.Error(), proto.ErrInvalidSignature.Error()) {
		c.log.Debug("syncing contract revision due to invalid signature", zap.Uint64("revisionNumber", rev.RevisionNumber), zap.Stringer("contractID", contractID), zap.Error(err))
		rev, renewed, err = c.syncRevision(ctx, contractID, rev)
		if err != nil {
			return fmt.Errorf("failed to sync revision: %w", err)
		} else if renewed {
			return ErrContractRenewed
		} else if rev.ProofHeight > maxProofHeight {
			return fmt.Errorf("%d > %d (%d+%d), %w", rev.ProofHeight, maxProofHeight, bh, revisionSubmissionBuffer, ErrContractNotRevisable)
		}
		c.log.Debug("synced contract revision", zap.Uint64("revisionNumber", rev.RevisionNumber), zap.Stringer("contractID", contractID))

		// try and revise the contract again
		update, err = reviseFn(rev)
	}
	if err != nil {
		return err
	} else if update.RevisionNumber <= rev.RevisionNumber {
		return fmt.Errorf("new revision number %d is not greater than current revision number %d", update.RevisionNumber, rev.RevisionNumber)
	}

	// update revision in the database
	err = c.store.UpdateContractRevision(ctx, contractID, update)
	if err != nil {
		c.log.Error("failed to update contract revision", zap.Error(err))
	}

	return nil
}
