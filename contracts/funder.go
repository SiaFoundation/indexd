package contracts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// FunderHostClient defines the interface for the funder to interact with the
	// host.
	FunderHostClient interface {
		io.Closer
		ReplenishAccounts(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, params rhp.RPCReplenishAccountsParams) (rhp.RPCReplenishAccountsResult, error)
		LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error)
	}

	// RevisionStore defines an interface that allows fetching and updating a
	// contract's revision.
	RevisionStore interface {
		ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error)
		UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error
		MarkContractBad(contractID types.FileContractID) error
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		client                   FunderHostClient
		signer                   rhp.ContractSigner
		chain                    client.ChainManager
		store                    RevisionStore
		revisionSubmissionBuffer uint64

		log *zap.Logger
	}
)

const (
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

	// ErrContractRenewed is returned when we try to revise a contract that has
	// already been renewed.
	ErrContractRenewed = errors.New("contract got renewed")
)

// FunderOption is a functional option type for configuring the Funder.
type FunderOption func(*Funder)

// WithRevisionSubmissionBuffer sets the revision submission buffer for the
// Funder.
func WithRevisionSubmissionBuffer(buffer uint64) FunderOption {
	if buffer == 0 {
		panic("revisionSubmissionBuffer mustn't be 0") // developer error
	}
	return func(f *Funder) {
		f.revisionSubmissionBuffer = buffer
	}
}

// NewFunder creates a new Funder.
func NewFunder(client FunderHostClient, signer rhp.ContractSigner, chain client.ChainManager, store RevisionStore, log *zap.Logger, opts ...FunderOption) *Funder {
	f := &Funder{
		client:                   client,
		signer:                   signer,
		chain:                    chain,
		store:                    store,
		revisionSubmissionBuffer: defaultRevisionSubmissionBuffer,
		log:                      log,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. The given accounts should not exceed the batch
// size used in the replenish RPC. This method returns two numbers, the first
// one indicates the number of accounts that were funded, the second indicates
// the number of contracts that were drained. Consecutive calls for the same
// host should take this into account and adjust the contract IDs that are being
// passed in.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accs []accounts.HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, _ error) {
	// sanity check the input
	if len(accs) > proto.MaxAccountBatchSize {
		return 0, 0, errors.New("too many accounts")
	} else if len(contractIDs) == 0 {
		return 0, 0, errors.New("no contract provided")
	} else if len(accs) == 0 {
		return 0, 0, nil
	}

	// prepare account keys
	accountKeys := make([]proto.Account, len(accs))
	for i, account := range accs {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		var res rhp.RPCReplenishAccountsResult
		var err error
		err = f.withRevision(ctx, contractID, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
			if contract.Revision.RenterOutput.Value.Cmp(target) < 0 {
				return rhp.ContractRevision{}, proto.Usage{}, ErrContractInsufficientFunds
			}

			batchSize := int(max(1, min(contract.Revision.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize)))
			maxEnd := min(len(accountKeys), funded+batchSize)
			// execute replenish RPC
			res, err = f.client.ReplenishAccounts(ctx, f.signer, f.chain, rhp.RPCReplenishAccountsParams{
				Accounts: accountKeys[funded:maxEnd],
				Target:   target,
				Contract: contract,
			})
			if err != nil {
				return rhp.ContractRevision{}, proto.Usage{}, err
			}
			funded = maxEnd
			return rhp.ContractRevision{
				ID:       contractID,
				Revision: res.Revision,
			}, res.Usage, nil
		})
		if errors.Is(err, ErrContractInsufficientFunds) {
			contractLog.Debug("contract has insufficient funds", zap.Error(err))
			drained++
			continue
		} else if errors.Is(err, ErrContractNotRevisable) {
			contractLog.Debug("contract is not revisable", zap.Error(err)) // sanity check
			drained++
			continue
		} else if err != nil {
			contractLog.Debug("failed to replenish accounts", zap.Error(err))
			continue
		} else if res.Revision.RemainingAllowance().Cmp(target) < 0 {
			contractLog.Debug("contract was drained by replenish RPC",
				zap.Stringer("remainingAllowance", res.Revision.RemainingAllowance()),
				zap.Stringer("target", target))
			drained++
		}

		if funded == len(accountKeys) {
			break
		}
	}

	return funded, drained, nil
}

func (f *Funder) syncRevision(ctx context.Context, contractID types.FileContractID, revision types.V2FileContract) (types.V2FileContract, bool, error) {
	// apply a sane timeout for syncing the revision
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch latest revision
	resp, err := f.client.LatestRevision(ctx, revision.HostPublicKey, contractID)
	if err != nil {
		f.log.Debug("failed to fetch latest revision", zap.Error(err))
		return types.V2FileContract{}, false, fmt.Errorf("%w; failed to fetch latest revision", err)
	} else if resp.Contract.RevisionNumber < revision.RevisionNumber {
		f.log.Warn("contract is out of sync, marking it as bad since this situation can not be recovered from",
			zap.Stringer("contractID", contractID),
			zap.Uint64("hostRevisionNumber", resp.Contract.RevisionNumber),
			zap.Uint64("localRevisionNumber", revision.RevisionNumber),
		)
		if err := f.store.MarkContractBad(contractID); err != nil {
			f.log.Error("failed to mark contract as bad", zap.Stringer("contractID", contractID), zap.Error(err))
		}
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
	err = f.store.UpdateContractRevision(contract, usage)
	if err != nil {
		f.log.Error("failed to update contract revision", zap.Stringer("contractID", contractID), zap.Error(err))
	}

	return resp.Contract, resp.Renewed, nil
}

// withRevision retrieves the current revision of the specified contract ID from
// the database and executes the provided revise function with it. If the host
// reports an invalid signature, suggesting the local revision is out of sync,
// it will synchronize with the host and retry the function using the updated
// revision. Therefore, the revise function must be idempotent.
func (f *Funder) withRevision(ctx context.Context, contractID types.FileContractID, reviseFn func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error)) error {
	cs := f.chain.TipState()
	bh := cs.Index.Height

	// fetch revision from database
	contract, renewed, err := f.store.ContractRevision(contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		return ErrContractRenewed
	} else if isBeyondMaxRevisionHeight(contract.Revision.ProofHeight, f.revisionSubmissionBuffer, bh) {
		return fmt.Errorf("%d <= %d (%d+%d), %w", contract.Revision.ProofHeight, bh+f.revisionSubmissionBuffer, bh, f.revisionSubmissionBuffer, ErrContractNotRevisable)
	}

	// revise the contract
	revised, usage, err := reviseFn(contract)

	// try and sync the revision if we got an error that indicates the revision is invalid
	if err != nil && strings.Contains(err.Error(), proto.ErrInvalidSignature.Error()) {
		f.log.Debug("syncing contract revision due to invalid signature", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Stringer("contractID", contractID), zap.Error(err))
		contract.Revision, renewed, err = f.syncRevision(ctx, contractID, contract.Revision)
		if err != nil {
			return fmt.Errorf("failed to sync revision: %w", err)
		} else if renewed {
			return ErrContractRenewed
		} else if isBeyondMaxRevisionHeight(contract.Revision.ProofHeight, f.revisionSubmissionBuffer, bh) {
			return fmt.Errorf("%d <= %d (%d+%d), %w", contract.Revision.ProofHeight, bh+f.revisionSubmissionBuffer, bh, f.revisionSubmissionBuffer, ErrContractNotRevisable)
		}
		f.log.Debug("synced contract revision", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Stringer("contractID", contractID))

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
		if err := f.store.UpdateContractRevision(revised, usage); err != nil {
			return fmt.Errorf("failed to update contract revision: %w", err)
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
