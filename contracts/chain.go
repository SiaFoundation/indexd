package contracts

import (
	"fmt"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
)

type (
	// UpdateTx defines what the contract manager needs to atomically process a
	// chain update in the database.
	UpdateTx interface {
		ContractElements() ([]types.V2FileContractElement, error)
		ContractElementsForBroadcast(maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error)
		IsKnownContract(contractID types.FileContractID) (bool, error)
		RejectPendingContracts(maxFormation time.Time) error
		UpdateContractElements(fces ...types.V2FileContractElement) error
		UpdateContractState(contractID types.FileContractID, state ContractState) error
	}

	updateTx struct {
		UpdateTx

		knownContracts map[types.FileContractID]bool
	}
)

func (tx *updateTx) IsKnownContract(fcid types.FileContractID) (bool, error) {
	known, found := tx.knownContracts[fcid]
	if found {
		return known, nil
	}
	known, err := tx.IsKnownContract(fcid)
	if err != nil {
		return false, fmt.Errorf("failed to determine whether contract is known: %w", err)
	}
	tx.knownContracts[fcid] = known
	return known, nil
}

// UpdateChainState state updates the contracts' state in the database and
// broadcasts revisions for failed expired contracts.
func (m *ContractManager) UpdateChainState(tx UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	uTx := &updateTx{
		UpdateTx: tx,
	}

	for _, cru := range reverted {
		err := m.revertChainUpdate(uTx, cru)
		if err != nil {
			return fmt.Errorf("failed to revert chain update: %w", err)
		}
	}

	for _, cau := range applied {
		err := m.applyChainUpdate(uTx, cau)
		if err != nil {
			return fmt.Errorf("failed to apply chain update: %w", err)
		}
	}

	// reject all contracts that have been pending for more than 'contractRejectBuffer'
	maxFormation := time.Now().Add(-m.contractRejectBuffer)
	if err := tx.RejectPendingContracts(maxFormation); err != nil {
		return fmt.Errorf("failed to reject pending contracts: %w", err)
	}

	// TODO: broadcast resolutions for expired contracts
	// 'expiredContractBroadcastBuffer' blocks after their window end to give
	// hosts a chance to do it themselves before we do it

	// TODO: prune expired contracts 'expiredContractPruneBuffer' blocks after
	// we begin broadcasting resolutions

	return nil
}

func (m *ContractManager) applyChainUpdate(tx *updateTx, cau chain.ApplyUpdate) error {
	for _, diff := range cau.V2FileContractElementDiffs() {
		if known, err := tx.IsKnownContract(diff.V2FileContractElement.ID); err != nil {
			return fmt.Errorf("failed to determine whether contract is known: %w", err)
		} else if !known {
			continue // ignore unknown contracts
		}
		if err := m.applyContractDiff(tx, diff); err != nil {
			return fmt.Errorf("failed to apply contract diff: %w", err)
		}
	}

	// update state element proofs
	return updateContractElementProofs(tx, cau)
}

func (m *ContractManager) applyContractDiff(tx *updateTx, diff consensus.V2FileContractElementDiff) error {
	// update contract state
	if diff.Resolution != nil || diff.Created {
		var state ContractState
		switch {
		case diff.Resolution != nil:
			state = ContractStateResolved
		case diff.Created:
			state = ContractStateActive
		default:
			panic("unknown state") // unreachable
		}
		if err := tx.UpdateContractState(diff.V2FileContractElement.ID, state); err != nil {
			return fmt.Errorf("failed to update contract state for %v: %w", diff.V2FileContractElement.ID, err)
		}
		m.log.Info("contract state changed", zap.Stringer("contractID", diff.V2FileContractElement.ID),
			zap.Stringer("state", state))
	}

	// update contract elements
	fce := diff.V2FileContractElement
	if rev, ok := diff.V2RevisionElement(); ok {
		fce = rev
	}
	if err := tx.UpdateContractElements(fce); err != nil {
		return fmt.Errorf("failed to update contract element: %w", err)
	}
	return nil
}

func (m *ContractManager) revertChainUpdate(tx *updateTx, cru chain.RevertUpdate) error {
	for _, diff := range cru.V2FileContractElementDiffs() {
		if known, err := tx.IsKnownContract(diff.V2FileContractElement.ID); err != nil {
			return fmt.Errorf("failed to determine whether contract is known: %w", err)
		} else if !known {
			continue // ignore unknown contracts
		}
		if err := m.revertContractDiff(tx, diff); err != nil {
			return fmt.Errorf("failed to apply contract diff: %w", err)
		}
	}
	return updateContractElementProofs(tx, cru)
}

func (m *ContractManager) revertContractDiff(tx *updateTx, diff consensus.V2FileContractElementDiff) error {
	// update contract state
	if diff.Resolution != nil || diff.Created {
		var state ContractState
		switch {
		case diff.Created:
			state = ContractStatePending
		case diff.Resolution != nil:
			state = ContractStateActive
		default:
			panic("unknown state") // unreachable
		}
		if err := tx.UpdateContractState(diff.V2FileContractElement.ID, state); err != nil {
			return fmt.Errorf("failed to update contract state for %v: %w", diff.V2FileContractElement.ID, err)
		}
		m.log.Info("contract state changed", zap.Stringer("contractID", diff.V2FileContractElement.ID),
			zap.Stringer("state", state))
	}

	// update contract elements
	fce := diff.V2FileContractElement
	if rev, ok := diff.V2RevisionElement(); ok {
		fce = rev
	}
	if err := tx.UpdateContractElements(fce); err != nil {
		return fmt.Errorf("failed to update contract element: %w", err)
	}
	return nil
}

func updateContractElementProofs(tx *updateTx, updater wallet.ProofUpdater) error {
	fces, err := tx.ContractElements()
	if err != nil {
		return err
	}
	for i := range fces {
		updater.UpdateElementProof(&fces[i].StateElement)
	}
	if err := tx.UpdateContractElements(fces...); err != nil {
		return fmt.Errorf("failed to update contract state elements: %w", err)
	}
	return nil
}
