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
		contracts, err := cm.store.ContractsForBroadcasting(ctx, minBroadcast, 10)
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

			err = cm.store.MarkBroadcastAttempt(ctx, contractID)
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
	fce, err := cm.store.ContractElement(ctx, contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract element: %w", err)
	}

	// fetch the host
	host, err := cm.store.Host(ctx, fce.V2FileContract.HostPublicKey)
	if err != nil {
		return fmt.Errorf("failed to fetch host: %w", err)
	}

	// fetch the latest revision
	client, err := cm.dialer.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		return fmt.Errorf("failed to dial host: %w", err)
	}
	defer client.Close()

	resp, err := client.LatestRevision(ctx, contractID)
	if err != nil {
		log.Warn("failed to fetch latest revision", zap.Error(err))
		return nil
	}

	// create the transaction
	const stdTxnSize = 1000
	fee := cm.cm.RecommendedFee().Mul64(stdTxnSize)
	txn := types.V2Transaction{
		MinerFee: fee,
		FileContractRevisions: []types.V2FileContractRevision{
			{
				Parent:   fce,
				Revision: resp.Contract,
			},
		},
	}

	// fund the transaction (only the fee) and sign it
	basis, toSign, err := cm.w.FundV2Transaction(&txn, fee, true)
	if err != nil {
		log.Debug("failed to fund transaction", zap.Error(err))
		return nil
	}
	cm.w.SignV2Inputs(&txn, toSign)

	// verify the transaction and add it to the transaction pool
	txnSet := []types.V2Transaction{txn}
	_, err = cm.cm.AddV2PoolTransactions(basis, txnSet)
	if err != nil {
		cm.w.ReleaseInputs(nil, txnSet)
		log.Debug("failed to add transaction set to the pool", zap.Error(err))
		return nil
	}

	// broadcast the transaction
	cm.s.BroadcastV2TransactionSet(basis, txnSet)
	return nil
}
