package contracts

import (
	"context"
	"errors"
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
		IsKnownContract(contractID types.FileContractID) (bool, error)
		DeleteContractElements(contractIDs ...types.FileContractID) error
		UpdateContractElements(fces ...types.V2FileContractElement) error
		UpdateContractState(contractID types.FileContractID, state ContractState) error
		UpdateContractRenewedTo(contractID types.FileContractID, renewedTo *types.FileContractID) error
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
	known, err := tx.UpdateTx.IsKnownContract(fcid)
	if err != nil {
		return false, fmt.Errorf("failed to determine whether contract is known: %w", err)
	}
	tx.knownContracts[fcid] = known
	return known, nil
}

// ProcessActions performs any post-processing actions required after a call to
// UpdateChainState that don't need to be atomic with the chain update. It is
// not guaranteed to be called on every update but will eventually be called
// after applying all batches of a sync.
func (m *ContractManager) ProcessActions(ctx context.Context) error {
	// reject all contracts that have been pending for more than 'contractRejectBuffer'
	maxFormation := time.Now().Add(-m.contractRejectBuffer)
	if err := m.store.RejectPendingContracts(maxFormation); err != nil {
		return fmt.Errorf("failed to reject pending contracts: %w", err)
	}

	// broadcast resolutions for expired contracts
	// 'expiredContractBroadcastBuffer' blocks after their window end to give
	// hosts a chance to do it themselves before we do it
	if err := m.broadcastExpiredContracts(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("failed to broadcast expired contracts: %w", err)
	}

	// prune expired contracts 'expiredContractPruneBuffer' blocks after
	// we begin broadcasting resolutions
	if err := m.store.PruneExpiredContractElements(m.expiredContractBroadcastBuffer + m.expiredContractPruneBuffer); err != nil {
		return fmt.Errorf("failed to prune expired contracts: %w", err)
	}

	// prune expired contracts from contract_sectors_map
	// 'expiredContractSectorsPruneBuffer' blocks after the contract expired
	if err := m.store.PruneContractSectorsMap(m.expiredContractSectorsPruneBuffer); err != nil {
		return fmt.Errorf("failed to prune expired contracts: %w", err)
	}
	return nil
}

// UpdateChainState state updates the contracts' state in the database and
// broadcasts revisions for failed expired contracts.
func (m *ContractManager) UpdateChainState(tx UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	uTx := &updateTx{
		UpdateTx:       tx,
		knownContracts: make(map[types.FileContractID]bool),
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
	return nil
}

func (m *ContractManager) applyV2ContractDiffs(tx *updateTx, diffs []consensus.V2FileContractElementDiff) error {
	var updated []types.V2FileContractElement
	var resolved []types.FileContractID
	for _, diff := range diffs {
		if known, err := tx.IsKnownContract(diff.V2FileContractElement.ID); err != nil {
			return fmt.Errorf("failed to determine whether contract is known: %w", err)
		} else if !known {
			continue // ignore unknown contracts
		}

		switch {
		case diff.Created:
			if err := tx.UpdateContractState(diff.V2FileContractElement.ID, ContractStateActive); err != nil {
				return fmt.Errorf("failed to update contract state for %v: %w", diff.V2FileContractElement.ID, err)
			}
			updated = append(updated, diff.V2FileContractElement)
		case diff.Resolution != nil:
			if err := tx.UpdateContractState(diff.V2FileContractElement.ID, ContractStateResolved); err != nil {
				return fmt.Errorf("failed to update contract state for %v: %w", diff.V2FileContractElement.ID, err)
			} else if _, ok := diff.Resolution.(*types.V2FileContractRenewal); ok {
				// update renewed to for renewals
				renewedTo := diff.V2FileContractElement.ID.V2RenewalID()
				if err := tx.UpdateContractRenewedTo(diff.V2FileContractElement.ID, &renewedTo); err != nil {
					return fmt.Errorf("failed to update renewed to for %v: %w", diff.V2FileContractElement.ID, err)
				}
			}
			// contract was resolved, this element is no longer needed
			resolved = append(resolved, diff.V2FileContractElement.ID)
		default:
			fce := diff.V2FileContractElement
			if rev, ok := diff.V2RevisionElement(); ok {
				fce = rev
			}
			updated = append(updated, fce)
		}
	}

	if len(updated) > 0 {
		// update created and revised contract elements
		if err := tx.UpdateContractElements(updated...); err != nil {
			return fmt.Errorf("failed to update modified contract elements: %w", err)
		}
	}

	if len(resolved) > 0 {
		// delete resolved contract elements
		if err := tx.DeleteContractElements(resolved...); err != nil {
			return fmt.Errorf("failed to delete resolved contract elements: %w", err)
		}
	}
	return nil
}

func (m *ContractManager) applyChainUpdate(tx *updateTx, cau chain.ApplyUpdate) error {
	if err := m.applyV2ContractDiffs(tx, cau.V2FileContractElementDiffs()); err != nil {
		return fmt.Errorf("failed to apply contract diffs: %w", err)
	}

	// update state element proofs
	return updateContractElementProofs(tx, cau)
}

func (m *ContractManager) revertV2ContractDiffs(tx *updateTx, diffs []consensus.V2FileContractElementDiff) error {
	var reverted []types.FileContractID
	var updated []types.V2FileContractElement
	for _, diff := range diffs {
		if known, err := tx.IsKnownContract(diff.V2FileContractElement.ID); err != nil {
			return fmt.Errorf("failed to determine whether contract is known: %w", err)
		} else if !known {
			continue // ignore unknown contracts
		}

		switch {
		case diff.Created:
			// contract no longer exists
			reverted = append(reverted, diff.V2FileContractElement.ID)
			if err := tx.UpdateContractState(diff.V2FileContractElement.ID, ContractStatePending); err != nil {
				return fmt.Errorf("failed to update contract state for %v: %w", diff.V2FileContractElement.ID, err)
			}
		case diff.Resolution != nil:
			// contract is now active again
			updated = append(updated, diff.V2FileContractElement)
			if err := tx.UpdateContractState(diff.V2FileContractElement.ID, ContractStateActive); err != nil {
				return fmt.Errorf("failed to update contract state for %v: %w", diff.V2FileContractElement.ID, err)
			} else if _, ok := diff.Resolution.(*types.V2FileContractRenewal); ok {
				// clear renewed to for renewals
				if err := tx.UpdateContractRenewedTo(diff.V2FileContractElement.ID, nil); err != nil {
					return fmt.Errorf("failed to null renewed to for %v: %w", diff.V2FileContractElement.ID, err)
				}
			}
		default:
			updated = append(updated, diff.V2FileContractElement)
		}
	}

	if len(reverted) > 0 {
		// remove reverted contract elements
		if err := tx.DeleteContractElements(reverted...); err != nil {
			return fmt.Errorf("failed to revert deleted contract elements: %w", err)
		}
	}

	if len(updated) > 0 {
		// revert updated contract elements
		if err := tx.UpdateContractElements(updated...); err != nil {
			return fmt.Errorf("failed to revert updated contract elements: %w", err)
		}
	}
	return nil
}

func (m *ContractManager) revertChainUpdate(tx *updateTx, cru chain.RevertUpdate) error {
	if err := m.revertV2ContractDiffs(tx, cru.V2FileContractElementDiffs()); err != nil {
		return fmt.Errorf("failed to revert contract diffs: %w", err)
	}
	// update state element proofs
	return updateContractElementProofs(tx, cru)
}

func (m *ContractManager) broadcastExpiredContracts() error {
	expiredFCEs, err := m.store.ContractElementsForBroadcast(m.expiredContractBroadcastBuffer)
	if err != nil {
		return fmt.Errorf("failed to get expired contracts for broadcast: %w", err)
	}
	for _, fce := range expiredFCEs {
		log := m.log.With(zap.Stringer("contractID", fce.ID)).
			With(zap.Uint64("expirationHeight", fce.V2FileContract.ExpirationHeight))

		const contractResolutionTxnWeight = 1000
		txn := types.V2Transaction{
			MinerFee: m.wallet.RecommendedFee().Mul64(contractResolutionTxnWeight),
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &types.V2FileContractExpiration{},
				},
			},
		}

		// fund and sign txn
		basis, toSign, err := m.wallet.FundV2Transaction(&txn, txn.MinerFee, true)
		if err != nil {
			log.Error("failed to fund contract expiration txn", zap.Error(err))
			continue
		}
		m.wallet.SignV2Inputs(&txn, toSign)

		// fetch potential parents and basis for broadcasting the txn set
		basis, txnSet, err := m.chain.V2TransactionSet(basis, txn)
		if err != nil {
			log.Error("failed to retrieve txn set for broadcasting", zap.Error(err))
			m.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			continue
		}

		// verify txn and broadcast it
		if err = m.wallet.BroadcastV2TransactionSet(basis, txnSet); err != nil {
			m.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			log.Error("failed to broadcast contract expiration txn", zap.Error(err))
			continue
		}
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
