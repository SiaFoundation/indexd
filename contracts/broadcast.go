package contracts

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

func (cm *ContractManager) performBroadcastContractRevisions(ctx context.Context, log *zap.Logger) error {
	broadcastLog := log.Named("broadcast")
	minBroadcast := time.Now().Add(-cm.revisionBroadcastInterval)

	var exhausted bool
	for !exhausted {
		contracts, err := cm.store.ContractsForBroadcasting(minBroadcast, 10)
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for broadcasting: %w", err)
		} else if len(contracts) < 10 {
			exhausted = true
		}

		for _, contractID := range contracts {
			err := cm.broadcastContractRevision(ctx, contractID, log)
			if err != nil {
				broadcastLog.Error("failed to broadcast contract revision", zap.Error(err))
			}

			err = cm.store.MarkBroadcastAttempt(contractID)
			if err != nil {
				broadcastLog.Error("failed to mark broadcast attempt", zap.Error(err))
			}
		}
	}

	return nil
}

func (cm *ContractManager) broadcastContractRevision(ctx context.Context, contractID types.FileContractID, log *zap.Logger) error {
	// apply sane timeout
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch contract element
	fce, err := cm.store.ContractElement(contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract element: %w", err)
	}

	// fetch latest revision
	contract, _, err := cm.store.ContractRevision(contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract revision: %w", err)
	}

	// create the transaction
	const stdTxnSize = 1000
	fee := cm.wallet.RecommendedFee().Mul64(stdTxnSize)
	txn := types.V2Transaction{
		MinerFee: fee,
		FileContractRevisions: []types.V2FileContractRevision{
			{
				Parent:   fce,
				Revision: contract.Revision,
			},
		},
	}

	// fund the transaction (only the fee) and sign it
	basis, toSign, err := cm.wallet.FundV2Transaction(&txn, fee, true)
	if err != nil {
		log.Debug("failed to fund transaction", zap.Error(err))
		return nil
	}
	cm.wallet.SignV2Inputs(&txn, toSign)

	// broadcast the transaction
	txnSet := []types.V2Transaction{txn}
	if err = cm.wallet.BroadcastV2TransactionSet(basis, txnSet); err != nil {
		cm.wallet.ReleaseInputs(nil, txnSet)
		log.Debug("failed to add transaction set to the pool", zap.Error(err))
		return nil
	}
	return nil
}
