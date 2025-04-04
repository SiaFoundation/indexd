package contracts

import (
	"context"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

func (cf *contractor) RefreshContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	t, err := siamux.Dial(dialCtx, addr, hk)
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to dial host: %w", err)
	}
	defer t.Close()

	rev, err := rhp.RPCLatestRevision(ctx, t, params.ContractID)
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if rev.Renewed {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("contract already renewed")
	} else if !rev.Revisable {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("contract not revisable")
	}

	res, err := rhp.RPCRefreshContract(ctx, t, cf.cm, cf.signer, cf.cm.TipState(), settings.Prices, rev.Contract, proto.RPCRefreshContractParams{
		Allowance:  rev.Contract.RenterOutput.Value,
		Collateral: rev.Contract.MissedHostValue,
	})
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

func (cm *ContractManager) performContractRefreshes(ctx context.Context, log *zap.Logger) error {
	renewalLog := log.Named("refresh")
	contracts, err := cm.store.Contracts(ctx, WithGood(true), WithRevisable(true))
	if err != nil {
		return fmt.Errorf("failed to fetch contracts for refreshing: %w", err)
	}

	for _, contract := range contracts {
		if !contract.NeedsRefresh() {
			continue
		}
		contractLog := renewalLog.With(zap.Stringer("hostKey", contract.HostKey),
			zap.Stringer("contractID", contract.ID),
			zap.Stringer("remainingAllowance", contract.RemainingAllowance),
			zap.Stringer("initialAllowance", contract.InitialAllowance),
			zap.Stringer("usedCollateral", contract.UsedCollateral),
			zap.Stringer("totalCollateral", contract.TotalCollateral),
			zap.Bool("outOfFunds", contract.OutOfFunds()),
			zap.Bool("outOfCollateral", contract.OutOfCollateral()),
		)

		// fetch corresponding host and check if it's theoretically usable
		host, err := cm.store.Host(ctx, contract.HostKey)
		if err != nil {
			contractLog.Debug("failed to fetch host", zap.Error(err))
			continue
		} else if !host.Usability.Usable() {
			contractLog.Debug("host is not usable")
			continue
		}

		// scan host for valid price settings and make sure it's still usable
		scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		host, err = cm.scanner.ScanHost(scanCtx, host.PublicKey)
		cancel()
		if err != nil {
			contractLog.Warn("failed to scan host", zap.Error(err))
			continue
		} else if !host.Usability.Usable() {
			contractLog.Debug("host is not usable after scan")
			continue
		}

		var additionalAllowance, additionalCollateral types.Currency

		if contract.OutOfFunds() {
			additionalAllowance = contract.InitialAllowance.Mul64(11).Div64(10) // add 10%
		}
		if contract.OutOfCollateral() {
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
		}

		// only refresh if either allowance or collateral increases
		if additionalAllowance.IsZero() && additionalCollateral.IsZero() {
			contractLog.Debug("not refreshing contract since resulting contract would have the same allowance and collateral")
			continue
		}

		res, err := cm.contractor.RefreshContract(ctx, contract.HostKey, host.SiamuxAddr(), host.Settings, proto.RPCRefreshContractParams{
			Allowance:  additionalAllowance,
			Collateral: additionalCollateral,
			ContractID: contract.ID,
		})
		if err != nil {
			contractLog.Debug("failed to renew", zap.Error(err))
			continue
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		err = cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision.ProofHeight, renewed.Revision.ExpirationHeight, host.Settings.Prices.ContractPrice, renewed.Revision.RenterOutput.Value, minerFee, renewed.Revision.TotalCollateral)
		if err != nil {
			contractLog.Error("failed to store renewed contract", zap.Error(err))
			continue
		}
	}

	return nil
}
