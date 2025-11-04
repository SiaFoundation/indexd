package contracts

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractRenewals(ctx context.Context, period, renewWindow uint64, log *zap.Logger) error {
	bh := cm.chain.TipState().Index.Height
	minProofHeight := bh + renewWindow
	newProofHeight := bh + period + renewWindow

	batchSize := 50
	for offset := 0; ; offset += batchSize {
		contracts, err := cm.store.Contracts(ctx, offset, batchSize, WithGood(true), WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for renewal: %w", err)
		}

		for _, contract := range contracts {
			if contract.ProofHeight > minProofHeight {
				continue // too early to renew
			} else if !contract.Good {
				continue // contract is bad
			}

			if err := cm.renewContract(ctx, contract, newProofHeight, period, log); err != nil {
				log.Error("failed to renew contract", zap.Stringer("contractID", contract.ID), zap.Error(err))
			}
		}

		if len(contracts) < batchSize {
			break
		}
	}

	return nil
}

func (cm *ContractManager) renewContract(ctx context.Context, contract Contract, proofHeight, period uint64, log *zap.Logger) error {
	log = log.With(zap.Stringer("hostKey", contract.HostKey), zap.Stringer("contractID", contract.ID))

	return cm.hosts.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		// scale funding by active account count
		target, err := cm.accounts.FundTarget(ctx, minAllowance)
		if err != nil {
			return fmt.Errorf("failed to get fund target: %w", err)
		}

		allowance, collateral := contractFunding(host.Settings, contract.Size, target, period)
		renewCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		hc, err := cm.dialer.DialHost(renewCtx, host.PublicKey, host.RHP4Addrs())
		if err != nil {
			log.Debug("failed to dial host", zap.Error(err))
			return nil
		}
		defer hc.Close()

		res, err := hc.RenewContract(renewCtx, host.Settings, rhp.RPCRenewContractParams{
			Allowance:   allowance,
			Collateral:  collateral,
			ContractID:  contract.ID,
			ProofHeight: proofHeight,
		})
		if err != nil {
			return fmt.Errorf("failed to renew contract: %w", err)
		}
		log := log.With(zap.Stringer("newContractID", res.Contract.ID))
		if err := cm.wallet.BroadcastV2TransactionSet(res.RenewalSet.Basis, res.RenewalSet.Transactions); err != nil {
			// error is ignored as it is assumed the host has validated the transaction set.
			// It will eventually be mined or rejected. This is to prevent minor synchronization
			// differences from causing a renewal to not be registered in the database but later
			// confirmed.
			log.Warn("failed to broadcast contract renewal transaction set", zap.Error(err))
		}

		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, res.Usage); err != nil {
			return fmt.Errorf("failed to store renewed contract %q: %w", renewed.ID, err)
		}

		log.Info("successfully renewed contract",
			zap.Stringer("computedAllowance", allowance),
			zap.Stringer("computedCollateral", collateral),
			zap.Stringer("newRemainingAllowance", renewed.Revision.RemainingAllowance()),
			zap.Stringer("newRemainingCollateral", renewed.Revision.RemainingCollateral()))
		return nil
	})
}
