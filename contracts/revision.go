package contracts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

const (
	// DefaultRevisionSubmissionBuffer is a buffer that mainnet hosts apply on
	// the contract's proof height before they consider a contract revisable, so
	// if the current block height plus the buffer exceed the proof height, the
	// contract is not revisable.
	DefaultRevisionSubmissionBuffer = 144
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

	// ErrContractMaxSize is returned when we try to revise a contract that has
	// reached its maximum size.
	ErrContractMaxSize = errors.New("contract has reached maximum size")

	// ErrContractOutOfFunds is returned when we try to perform an action on a
	// contract that has no funds left to cover the action.
	ErrContractOutOfFunds = errors.New("contract is out of funds")

	// ErrContractOutOfCollateral is returned when we try to perform an action on a
	// contract that has no collateral left to cover the action.
	ErrContractOutOfCollateral = errors.New("contract is out of collateral")
)

type (
	// LatestRevisionClient defines the interface for fetching the latest
	// revision from a host.
	LatestRevisionClient interface {
		LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error)
	}

	// RevisionStore defines an interface that allows fetching and updating a
	// contract's revision.
	RevisionStore interface {
		ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error)
		UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error
		MarkContractBad(contractID types.FileContractID) error
	}

	// RevisionManager handles contract revision management including syncing
	// with the host when revisions become out of sync.
	RevisionManager struct {
		client LatestRevisionClient
		chain  ChainManager
		store  RevisionStore
		buffer uint64
		log    *zap.Logger
	}
)

// NewRevisionManager creates a new RevisionManager.
func NewRevisionManager(client LatestRevisionClient, chain ChainManager, store RevisionStore, buffer uint64, log *zap.Logger) *RevisionManager {
	return &RevisionManager{
		client: client,
		chain:  chain,
		store:  store,
		buffer: buffer,
		log:    log,
	}
}

func (rm *RevisionManager) syncRevision(ctx context.Context, contractID types.FileContractID, revision types.V2FileContract) (types.V2FileContract, bool, error) {
	// apply a sane timeout for syncing the revision
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch latest revision
	resp, err := rm.client.LatestRevision(ctx, revision.HostPublicKey, contractID)
	if err != nil {
		rm.log.Debug("failed to fetch latest revision", zap.Error(err))
		return types.V2FileContract{}, false, fmt.Errorf("%w; failed to fetch latest revision", err)
	} else if resp.Contract.RevisionNumber < revision.RevisionNumber {
		rm.log.Warn("contract is out of sync, marking it as bad since this situation can not be recovered from",
			zap.Stringer("contractID", contractID),
			zap.Uint64("hostRevisionNumber", resp.Contract.RevisionNumber),
			zap.Uint64("localRevisionNumber", revision.RevisionNumber),
		)
		if err := rm.store.MarkContractBad(contractID); err != nil {
			rm.log.Error("failed to mark contract as bad", zap.Stringer("contractID", contractID), zap.Error(err))
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
	if err = rm.store.UpdateContractRevision(contract, usage); err != nil {
		rm.log.Error("failed to update contract revision", zap.Stringer("contractID", contractID), zap.Error(err))
	}

	return resp.Contract, resp.Renewed, nil
}

// WithRevision retrieves the current revision of the specified locked contract
// from the database and executes the provided revise function with it. If the
// host reports an invalid signature, suggesting the local revision is out of
// sync, it will synchronize with the host and retry the function using the
// updated revision. Therefore, the revise function must be idempotent.
func (rm *RevisionManager) WithRevision(ctx context.Context, lc *LockedContract,
	reviseFn func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage,
		error)) error {
	cs := rm.chain.TipState()
	bh := cs.Index.Height
	contractID := lc.id

	// fetch revision from database
	contract, renewed, err := rm.store.ContractRevision(contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		return ErrContractRenewed
	} else if isBeyondMaxRevisionHeight(contract.Revision.ProofHeight, rm.buffer, bh) {
		return fmt.Errorf("%d <= %d (%d+%d), %w", contract.Revision.ProofHeight, bh+rm.buffer, bh, rm.buffer, ErrContractNotRevisable)
	}

	// revise the contract
	revised, usage, err := reviseFn(contract)

	// try and sync the revision if we got an error that indicates the revision is invalid
	if err != nil && strings.Contains(err.Error(), proto.ErrInvalidSignature.Error()) {
		rm.log.Debug("syncing contract revision due to invalid signature", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Stringer("contractID", contractID), zap.Error(err))
		contract.Revision, renewed, err = rm.syncRevision(ctx, contractID, contract.Revision)
		if err != nil {
			return fmt.Errorf("failed to sync revision: %w", err)
		} else if renewed {
			return ErrContractRenewed
		} else if isBeyondMaxRevisionHeight(contract.Revision.ProofHeight, rm.buffer, bh) {
			return fmt.Errorf("%d <= %d (%d+%d), %w", contract.Revision.ProofHeight, bh+rm.buffer, bh, rm.buffer, ErrContractNotRevisable)
		}
		rm.log.Debug("synced contract revision", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Stringer("contractID", contractID))

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
		if err := rm.store.UpdateContractRevision(revised, usage); err != nil {
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
