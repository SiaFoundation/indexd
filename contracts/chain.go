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
		IsKnownContract(contractID types.FileContractID) (bool, error)
		RejectContracts(time.Duration) error
		UpdateContractElement(fce types.V2FileContractElement) error
		UpdateContractElementProofs(wallet.ProofUpdater) error
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
	cTx := &updateTx{
		UpdateTx: tx,
	}

	for _, cru := range reverted {
		err := m.revertChainUpdate(cTx, cru)
		if err != nil {
			return fmt.Errorf("failed to revert chain update: %w", err)
		}
	}

	for _, cau := range applied {
		err := m.applyChainUpdate(cTx, cau)
		if err != nil {
			return fmt.Errorf("failed to apply chain update: %w", err)
		}
	}
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

	// TODO: update file contract element proofs

	// TODO: reject all contracts that have been pending for more than 'contractRejectBuffer'

	// TODO: broadcast resolutions for expired contracts
	// 'expiredContractBroadcastBuffer' blocks after their window end to give
	// hosts a chance to do it themselves before we do it

	// TODO: prune expired contracts 'expiredContractPruneBuffer' blocks after
	// we begin broadcasting resolutions

	return nil
}

func (m *ContractManager) applyContractDiff(tx *updateTx, diff consensus.V2FileContractElementDiff) error {
	// update contract state
	var state ContractState
	if resolution := diff.Resolution; resolution != nil {
		state = ContractStateResolved
	} else if diff.Created {
		state = ContractStateActive
	}
	if err := tx.UpdateContractState(diff.V2FileContractElement.ID, state); err != nil {
		return fmt.Errorf("failed to update contract state for new contract: %w", err)
	}
	m.log.Info("contract state changed", zap.Stringer("contractID", diff.V2FileContractElement.ID),
		zap.Stringer("state", state))

	// update contract elements
	var fce types.V2FileContractElement
	if rev, ok := diff.V2RevisionElement(); !ok {
		fce = diff.V2FileContractElement
	} else {
		fce = rev
	}
	if err := tx.UpdateContractElement(fce); err != nil {
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
	return nil
}

func (m *ContractManager) revertContractDiff(tx *updateTx, diff consensus.V2FileContractElementDiff) error {
	// update contract state
	var state ContractState
	if diff.Created {
		state = ContractStatePending
	} else if resolution := diff.Resolution; resolution != nil {
		state = ContractStateActive
	}
	if err := tx.UpdateContractState(diff.V2FileContractElement.ID, state); err != nil {
		return fmt.Errorf("failed to update contract state for new contract: %w", err)
	}
	m.log.Info("contract state changed", zap.Stringer("contractID", diff.V2FileContractElement.ID),
		zap.Stringer("state", state))

	// update contract elements
	var fce types.V2FileContractElement
	if rev, ok := diff.V2RevisionElement(); !ok {
		fce = diff.V2FileContractElement
	} else {
		fce = rev
	}
	if err := tx.UpdateContractElement(fce); err != nil {
		return fmt.Errorf("failed to update contract element: %w", err)
	}
	return nil
}
