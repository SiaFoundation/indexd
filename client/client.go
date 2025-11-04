package client

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	// defaultRevisionSubmissionBuffer is a buffer that mainnet hosts apply on
	// the contract's proof height before they consider a contract revisable, so
	// if the current block height plus the buffer exceed the proof height, the
	// contract is not revisable.
	defaultRevisionSubmissionBuffer = 144
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
		ContractRevision(ctx context.Context, contractID types.FileContractID) (rhp.ContractRevision, bool, error)
		UpdateContractRevision(ctx context.Context, contract rhp.ContractRevision, usage proto.Usage) error
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

	client                   rhp.TransportClient
	signer                   rhp.FormContractSigner
	latestRevisionFn         rpcLatestRevisionFn
	revisionSubmissionBuffer uint64

	cm    ChainManager
	store RevisionStore
	log   *zap.Logger
}

// newHostClient creates a new HostClient that can be used to interact with a
// host using the RHP methods. The client is expected to be closed when no
// longer needed.
func newHostClient(hk types.PublicKey, cm ChainManager, client rhp.TransportClient, signer rhp.FormContractSigner, store RevisionStore, revisionSubmissionBuffer uint64, log *zap.Logger) *HostClient {
	hc := &HostClient{
		hostKey: hk,

		client:                   client,
		signer:                   signer,
		store:                    store,
		latestRevisionFn:         rhp.RPCLatestRevision, // allows mocking in tests
		revisionSubmissionBuffer: revisionSubmissionBuffer,

		cm:  cm,
		log: log.Named("client").With(zap.Stringer("hostKey", hk)),
	}
	return hc
}

// AccountBalance returns the balance of the given account.
func (c *HostClient) AccountBalance(ctx context.Context, account proto.Account) (types.Currency, error) {
	return rhp.RPCAccountBalance(ctx, c.client, account)
}

// AppendSectors appends the given sectors to the contract. If the contract
// cannot fit all sectors, as many as possible will be appended and the number of
// sectors attempted will be returned.
//
// The integer returned does not indicate the number of sectors that were
// appended, but rather the number of sectors that were attempted. Check the
// result for the actual number of sectors that were appended.
func (c *HostClient) AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (res rhp.RPCAppendSectorsResult, attempted int, err error) {
	// sanity check
	if len(sectors) > proto.MaxSectorBatchSize {
		return rhp.RPCAppendSectorsResult{}, 0, fmt.Errorf("too many sectors, %d > %d", len(sectors), proto.MaxSectorBatchSize) // developer error
	}

	// append sectors
	err = c.withRevision(ctx, contractID, func(contract rhp.ContractRevision) (_ rhp.ContractRevision, _ proto.Usage, err error) {
		if contract.Revision.Filesize >= maxContractSize {
			return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("contract is too large, %d > %d", contract.Revision.Filesize, maxContractSize)
		} else if contract.Revision.ExpirationHeight <= hostPrices.TipHeight {
			return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("contract has expired at height %d, current height is %d", contract.Revision.ExpirationHeight, hostPrices.TipHeight)
		}
		// calculate the maximum number of sectors we can append based on the
		// contract's remaining capacity and collateral
		maxRemainingSectors := (maxContractSize - contract.Revision.Filesize) / proto.SectorSize
		maxAppendSectors := (contract.Revision.Capacity - contract.Revision.Filesize) / proto.SectorSize
		duration := contract.Revision.ExpirationHeight - hostPrices.TipHeight
		sectorCollateralCost := hostPrices.RPCAppendSectorsCost(1, duration).RiskedCollateral
		if sectorCollateralCost.IsZero() {
			sectorCollateralCost = types.NewCurrency64(1) // avoid division by zero
		}
		maxAppendSectors += contract.Revision.RemainingCollateral().Div(sectorCollateralCost).Big().Uint64()
		// ensure the maximum contract size is not exceeded
		maxAppendSectors = min(maxAppendSectors, maxRemainingSectors)
		if maxAppendSectors == 0 {
			return rhp.ContractRevision{}, proto.Usage{}, ErrContractOutOfFunds
		}

		// only attempt to append up to the calculated maximum number of sectors
		if uint64(len(sectors)) > maxAppendSectors {
			sectors = sectors[:maxAppendSectors]
		}
		attempted = len(sectors)
		res, err = rhp.RPCAppendSectors(ctx, c.client, c.signer, c.cm.TipState(), hostPrices, contract, sectors)
		if err != nil {
			return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("failed to append sectors: %w", err)
		}
		contract.Revision = res.Revision
		return contract, res.Usage, nil
	})
	return
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
	if err := c.withRevision(ctx, contractID, func(contract rhp.ContractRevision) (_ rhp.ContractRevision, _ proto.Usage, err error) {
		res, err = rhp.RPCSectorRoots(ctx, c.client, c.cm.TipState(), hostPrices, c.signer, contract, offset, length)
		if err != nil {
			return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("failed to fetch sector roots: %w", err)
		}
		contract.Revision = res.Revision
		return contract, res.Usage, nil
	}); err != nil {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("failed to fetch sector roots: %w", err)
	}
	return res, nil
}

// FreeSectors frees the specified sectors in the contract.
func (c *HostClient) FreeSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	var res rhp.RPCFreeSectorsResult
	if err := c.withRevision(ctx, contractID, func(contract rhp.ContractRevision) (_ rhp.ContractRevision, _ proto.Usage, err error) {
		res, err = rhp.RPCFreeSectors(ctx, c.client, c.signer, c.cm.TipState(), hostPrices, contract, indices)
		if err != nil {
			return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("failed to free sectors: %w", err)
		}
		contract.Revision = res.Revision
		return contract, res.Usage, nil
	}); err != nil {
		return rhp.RPCFreeSectorsResult{}, fmt.Errorf("failed to fetch free sectors: %w", err)
	}
	return res, nil
}

// ReadSector reads a sector from the host and writes it to the provided writer.
func (c *HostClient) ReadSector(ctx context.Context, hostPrices proto.HostPrices, token proto.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (rhp.RPCReadSectorResult, error) {
	return rhp.RPCReadSector(ctx, c.client, hostPrices, token, w, root, offset, length)
}

// RefreshContract refreshes the contract with the host.
func (c *HostClient) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	var res rhp.RPCRefreshContractResult
	if err := c.withRevision(ctx, params.ContractID, func(contract rhp.ContractRevision) (_ rhp.ContractRevision, _ proto.Usage, err error) {
		if settings.ProtocolVersion.Cmp(rhp.ProtocolVersion500) < 0 {
			return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("host does not support contract refresh, protocol version %s < %s", settings.ProtocolVersion, rhp.ProtocolVersion500)
		}

		// TODO: this calculation matches the validation we do on the host, but it
		// is incorrect. This should be checking that the new contract's collateral
		// is not above the max collateral (existing risked + new collateral), not
		// the existing total collateral after adding the additional collateral.
		totalCollateral := contract.Revision.TotalCollateral.Add(params.Collateral)
		if totalCollateral.Cmp(settings.MaxCollateral) > 0 {
			capped, underflow := settings.MaxCollateral.SubWithUnderflow(contract.Revision.RiskedCollateral()) // cap to remaining collateral
			if underflow {
				capped = types.ZeroCurrency
			}
			params.Collateral = capped
		}

		res, err = rhp.RPCRefreshContractPartialRollover(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, contract.Revision, params)
		if err != nil {
			return rhp.ContractRevision{}, proto.Usage{}, err
		}
		// renewals return the old (or 'renewed') revision, the revision of the
		// renewal will be persisted in the database when the renewed contract
		// is added
		return contract, res.Usage, nil
	}); err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to refresh contract: %w", err)
	}
	return res, nil
}

// RenewContract renews the contract with the host.
func (c *HostClient) RenewContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRenewContractParams) (rhp.RPCRenewContractResult, error) {
	var res rhp.RPCRenewContractResult
	if err := c.withRevision(ctx, params.ContractID, func(contract rhp.ContractRevision) (_ rhp.ContractRevision, _ proto.Usage, err error) {
		estimatedRenewal, _ := proto.RenewContract(contract.Revision, settings.Prices, params)
		if estimatedRenewal.NewContract.TotalCollateral.Cmp(settings.MaxCollateral) > 0 {
			capped, underflow := settings.MaxCollateral.SubWithUnderflow(contract.Revision.RiskedCollateral()) // cap to remaining collateral
			if underflow {
				capped = types.ZeroCurrency
			}
			params.Collateral = capped
		}

		res, err = rhp.RPCRenewContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, contract.Revision, params)
		if err != nil {
			return rhp.ContractRevision{}, proto.Usage{}, err
		}

		// renewals return the old (or 'renewed') revision, the revision of the
		// renewal will be persisted in the database when the renewed contract
		// is added
		return contract, res.Usage, nil
	}); err != nil {
		return rhp.RPCRenewContractResult{}, fmt.Errorf("failed to renew contract: %w", err)
	}
	return res, nil
}

// ReplenishAccounts replenishes the accounts in the contract to the target value.
func (c *HostClient) ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (res rhp.RPCReplenishAccountsResult, funded int, _ error) {
	if err := c.withRevision(ctx, contractID, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
		if contract.Revision.RenterOutput.Value.Cmp(target) < 0 {
			return rhp.ContractRevision{}, proto.Usage{}, ErrContractInsufficientFunds
		}

		// prepare batch
		batchSize := int(min(contract.Revision.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize))
		funded = min(batchSize, len(accounts))
		batch := accounts[:funded]

		// prepare parameters
		params := rhp.RPCReplenishAccountsParams{
			Accounts: batch,
			Target:   target,
			Contract: contract,
		}
		var err error
		res, err = rhp.RPCReplenishAccounts(ctx, c.client, params, c.cm.TipState(), c.signer)
		if err != nil {
			return rhp.ContractRevision{}, proto.Usage{}, err
		}
		contract.Revision = res.Revision
		return contract, res.Usage, nil
	}); err != nil {
		return rhp.RPCReplenishAccountsResult{}, 0, fmt.Errorf("failed to replenish accounts: %w", err)
	}
	return res, funded, nil
}

// Settings returns the host settings.
func (c *HostClient) Settings(ctx context.Context) (proto.HostSettings, error) {
	return rhp.RPCSettings(ctx, c.client)
}

// VerifySector verifies the integrity of a sector on the host.
func (c *HostClient) VerifySector(ctx context.Context, hostPrices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	return rhp.RPCVerifySector(ctx, c.client, hostPrices, token, root)
}

// WriteSector writes a sector to the host.
func (c *HostClient) WriteSector(ctx context.Context, settings proto.HostPrices, token proto.AccountToken, data io.Reader, length uint64) (rhp.RPCWriteSectorResult, error) {
	// sanity check
	if length > proto.SectorSize {
		return rhp.RPCWriteSectorResult{}, fmt.Errorf("sector size too large, %d > %d", length, proto.SectorSize) // developer error
	}

	// write sector
	res, err := rhp.RPCWriteSector(ctx, c.client, settings, token, data, length)
	if err != nil {
		return rhp.RPCWriteSectorResult{}, fmt.Errorf("failed to write sector: %w", err)
	}

	return res, nil
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

	// attribute a lower remaining allowance to the usage, note: we don't know
	// what it was spent on, we track it as storage so it comes up in total
	// spending but not in account funding
	var usage proto.Usage
	if resp.Contract.RemainingAllowance().Cmp(revision.RemainingAllowance()) < 0 {
		usage.Storage = revision.RemainingAllowance().Sub(resp.Contract.RemainingAllowance())
	}

	// update latest revision
	contract := rhp.ContractRevision{ID: contractID, Revision: resp.Contract}
	err = c.store.UpdateContractRevision(ctx, contract, usage)
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
func (c *HostClient) withRevision(ctx context.Context, contractID types.FileContractID, reviseFn func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error)) error {
	cs := c.cm.TipState()
	bh := cs.Index.Height

	// fetch revision from database
	contract, renewed, err := c.store.ContractRevision(ctx, contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		return ErrContractRenewed
	} else if isBeyondMaxRevisionHeight(contract.Revision.ProofHeight, c.revisionSubmissionBuffer, bh) {
		return fmt.Errorf("%d <= %d (%d+%d), %w", contract.Revision.ProofHeight, bh+c.revisionSubmissionBuffer, bh, c.revisionSubmissionBuffer, ErrContractNotRevisable)
	}

	// revise the contract
	revised, usage, err := reviseFn(contract)

	// try and sync the revision if we got an error that indicates the revision is invalid
	if err != nil && strings.Contains(err.Error(), proto.ErrInvalidSignature.Error()) {
		c.log.Debug("syncing contract revision due to invalid signature", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Stringer("contractID", contractID), zap.Error(err))
		contract.Revision, renewed, err = c.syncRevision(ctx, contractID, contract.Revision)
		if err != nil {
			return fmt.Errorf("failed to sync revision: %w", err)
		} else if renewed {
			return ErrContractRenewed
		} else if isBeyondMaxRevisionHeight(contract.Revision.ProofHeight, c.revisionSubmissionBuffer, bh) {
			return fmt.Errorf("%d <= %d (%d+%d), %w", contract.Revision.ProofHeight, bh+c.revisionSubmissionBuffer, bh, c.revisionSubmissionBuffer, ErrContractNotRevisable)
		}
		c.log.Debug("synced contract revision", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Stringer("contractID", contractID))

		// try and revise the contract again
		revised, usage, err = reviseFn(contract)
	}
	if err != nil {
		return err
	} else if revised.ID != contractID {
		panic("contract ID mismatch") // developer error
	}

	// update revision in the database
	if revised.Revision.RevisionNumber > contract.Revision.RevisionNumber {
		if err := c.store.UpdateContractRevision(ctx, revised, usage); err != nil {
			c.log.Error("failed to update contract revision", zap.Error(err))
		}
	}

	return nil
}

// isBeyondMaxRevisionHeight checks whether we are too close to a contract's
// proofHeight for a contract to be considered revisable by the host.
func isBeyondMaxRevisionHeight(proofHeight, revisionSubmissionBuffer, blockHeight uint64) bool {
	var maxRevisionHeight uint64
	if proofHeight > revisionSubmissionBuffer {
		maxRevisionHeight = proofHeight - revisionSubmissionBuffer
	}
	return blockHeight >= maxRevisionHeight
}
