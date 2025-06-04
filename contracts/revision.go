package contracts

import (
	"context"
	"errors"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

// revisionSubmissionBuffer is a buffer that the host applies on the contract's
// proof height before it considers the contract revisable, so if the current
// block height plus the buffer exceed the proof height, the contract is not
// revisable.
const revisionSubmissionBuffer = 144

func (c *hostClient) syncRevision(ctx context.Context, contractID types.FileContractID, revision types.V2FileContract) (types.V2FileContract, bool, error) {
	// apply a sane timeout for syncing the revision
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch latest revision
	resp, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
	if err != nil {
		c.log.Debug("failed to fetch latest revision", zap.Error(err))
		return types.V2FileContract{}, false, fmt.Errorf("%w; failed to fetch latest revision", err)
	} else if resp.Contract.RevisionNumber < revision.RevisionNumber {
		return types.V2FileContract{}, false, errors.New("local revision is newer than host revision")
	}

	// update latest revision
	err = c.store.UpdateContractRevision(ctx, contractID, resp.Contract)
	if err != nil {
		c.log.Debug("failed to update contract revision", zap.Error(err))
		return types.V2FileContract{}, false, fmt.Errorf("failed to update contract revision: %w", err)
	}

	return resp.Contract, resp.Renewed, nil
}

func (c *hostClient) withRevision(ctx context.Context, contractID types.FileContractID, fn func(revision types.V2FileContract) (types.V2FileContract, error)) error {
	cs := c.cm.TipState()

	// fetch revision from database
	rev, renewed, err := c.store.ContractRevision(ctx, contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		return errors.New("contract got renewed")
	} else if rev.ProofHeight > cs.Index.Height+revisionSubmissionBuffer {
		return errors.New("contract not revisable")
	}

	// try and revise the contract
	update, err := fn(rev)
	if errors.Is(err, proto.ErrInvalidSignature) {
		rev, renewed, err = c.syncRevision(ctx, contractID, rev)
		if err != nil {
			return fmt.Errorf("failed to sync revision: %w", err)
		} else if renewed {
			return errors.New("contract got renewed")
		} else if rev.ProofHeight > cs.Index.Height+revisionSubmissionBuffer {
			return errors.New("contract not revisable")
		}

		update, err = fn(rev) // retry
	}
	if err != nil {
		return err
	}

	// update revision in the database
	err = c.store.UpdateContractRevision(ctx, contractID, update)
	if err != nil {
		c.log.Debug("failed to update contract revision", zap.Error(err))
	}

	return nil
}
