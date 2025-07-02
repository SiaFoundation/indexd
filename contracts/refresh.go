package contracts

import (
	"context"
	"fmt"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractRefreshes(ctx context.Context, log *zap.Logger) error {
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

			if err := cm.refreshContract(ctx, contract, refreshLog); err != nil {
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

func (cm *ContractManager) refreshContract(ctx context.Context, contract Contract, log *zap.Logger) error {
	contractLog := log.With(zap.Stringer("hostKey", contract.HostKey),
		zap.Stringer("contractID", contract.ID),
		zap.Stringer("remainingAllowance", contract.RemainingAllowance),
		zap.Stringer("initialAllowance", contract.InitialAllowance),
		zap.Stringer("usedCollateral", contract.UsedCollateral),
		zap.Stringer("totalCollateral", contract.TotalCollateral),
		zap.Bool("outOfFunds", contract.OutOfFunds()),
		zap.Bool("outOfCollateral", contract.OutOfCollateral()),
	)

	return cm.hm.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		var additionalAllowance, additionalCollateral types.Currency
		if contract.OutOfFunds() {
			additionalAllowance = contract.InitialAllowance.Mul64(11).Div64(10) // add 10%
			additionalCollateral = types.ZeroCurrency                           // don't need additional collateral
		} else if contract.OutOfCollateral() {
			additionalCollateral = contract.TotalCollateral.Mul64(11).Div64(10) // add 10%
			if contract.TotalCollateral.Add(additionalCollateral).Cmp(host.Settings.MaxCollateral) > 0 {
				var underflow bool
				additionalCollateral, underflow = host.Settings.MaxCollateral.SubWithUnderflow(contract.TotalCollateral)
				if underflow {
					additionalCollateral = types.ZeroCurrency
				}
				contractLog.Debug("capping additional collateral since total would exceed max collateral of host",
					zap.Stringer("additionalCollateral", additionalCollateral))
			}
			additionalAllowance = proto.MinRenterAllowance(host.Settings.Prices, additionalCollateral) // min possible
		}

		// only refresh if either allowance or collateral increases
		if additionalAllowance.IsZero() && additionalCollateral.IsZero() {
			contractLog.Debug("not refreshing contract since resulting contract would have the same allowance and collateral")
			return nil
		}

		hc, err := cm.dialer.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
		if err != nil {
			contractLog.Debug("failed to dial host", zap.Error(err))
			return nil
		}
		defer hc.Close()
		res, err := hc.RefreshContract(ctx, host.Settings, proto.RPCRefreshContractParams{
			Allowance:  additionalAllowance,
			Collateral: additionalCollateral,
			ContractID: contract.ID,
		})
		if err != nil {
			contractLog.Debug("failed to renew", zap.Error(err))
			return nil
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, renewed.Revision.MissedHostValue); err != nil {
			return fmt.Errorf("failed to store renewed contract: %w", err)
		}
		return nil
	})
}
