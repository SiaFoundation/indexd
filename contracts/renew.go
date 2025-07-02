package contracts

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractRenewals(ctx context.Context, period, renewWindow uint64, log *zap.Logger) error {
	renewalLog := log.Named("renewal")

	bh := cm.cm.TipState().Index.Height
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

			if err := cm.renewContract(ctx, contract, newProofHeight, renewalLog); err != nil {
				renewalLog.Error("failed to renew contract",
					zap.Stringer("contractID", contract.ID),
					zap.Error(err),
				)
			}
		}

		if len(contracts) < batchSize {
			break
		}
	}

	return nil
}

func (cm *ContractManager) renewContract(ctx context.Context, contract Contract, proofHeight uint64, log *zap.Logger) error {
	contractLog := log.With(zap.Stringer("hostKey", contract.HostKey), zap.Stringer("contractID", contract.ID))

	return cm.hm.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		hc, err := cm.dialer.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
		if err != nil {
			contractLog.Debug("failed to dial host", zap.Error(err))
			return nil
		}
		defer hc.Close()
		res, err := hc.RenewContract(ctx, host.Settings, contract.ID, proofHeight)
		if err != nil {
			contractLog.Debug("failed to renew", zap.Error(err))
			return nil
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, types.ZeroCurrency); err != nil {
			return fmt.Errorf("failed to store renewed contract: %w", err)
		}
		return nil
	})
}
