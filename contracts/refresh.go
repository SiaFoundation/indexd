package contracts

import (
	"context"
	"fmt"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractRefreshes(ctx context.Context, period uint64, log *zap.Logger) error {
	refreshLog := log.Named("refresh")

	batchSize := 50
	for offset := 0; ; offset += batchSize {
		contracts, err := cm.store.Contracts(ctx, offset, batchSize, WithGood(true), WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for refreshing: %w", err)
		}

		for _, contract := range contracts {
			if !contract.NeedsRefresh() {
				continue
			}

			if err := cm.refreshContract(ctx, contract, period, refreshLog); err != nil {
				refreshLog.Error("failed to refresh contract",
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

func (cm *ContractManager) refreshContract(ctx context.Context, contract Contract, period uint64, log *zap.Logger) error {
	contractLog := log.With(zap.Stringer("hostKey", contract.HostKey),
		zap.Stringer("contractID", contract.ID),
		zap.Stringer("remainingAllowance", contract.RemainingAllowance),
		zap.Stringer("initialAllowance", contract.InitialAllowance),
		zap.Stringer("usedCollateral", contract.UsedCollateral),
		zap.Stringer("totalCollateral", contract.TotalCollateral),
		zap.Bool("outOfFunds", contract.OutOfFunds()),
		zap.Bool("outOfCollateral", contract.OutOfCollateral()),
	)

	return cm.hosts.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		hc, err := cm.dialer.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
		if err != nil {
			contractLog.Debug("failed to dial host", zap.Error(err))
			return nil
		}
		defer hc.Close()

		// TODO: Right now this isn't quite correct since allowance and
		// collateral are added on top of the existing one. This should fix
		// itself once the new version of the refresh RPC is used. The problems
		// are:
		// 1. If the contract is out of funds we also add collateral and vice versa
		// 2. The total collateral might exceed the host's maximum collateral
		//    since 'contractFunding' doesn't take into account existing collateral
		allowance, collateral := contractFunding(host.Settings, contract.Size, minAllowance, minHostCollateral, period)
		res, err := hc.RefreshContract(ctx, host.Settings, proto.RPCRefreshContractParams{
			Allowance:  allowance,
			Collateral: collateral,
			ContractID: contract.ID,
		})
		if err != nil {
			contractLog.Debug("failed to renew", zap.Error(err))
			return nil
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee); err != nil {
			return fmt.Errorf("failed to store renewed contract: %w", err)
		}
		return nil
	})
}
