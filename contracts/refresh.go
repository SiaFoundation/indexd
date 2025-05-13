package contracts

import (
	"context"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

func (c *contractor) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	rev, err := rhp.RPCLatestRevision(ctx, c.client, params.ContractID)
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if rev.Renewed {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("contract already renewed")
	} else if !rev.Revisable {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("contract not revisable")
	}

	res, err := rhp.RPCRefreshContract(ctx, c.client, c.cm, c.signer, c.cm.TipState(), settings.Prices, rev.Contract, proto.RPCRefreshContractParams{
		Allowance:  rev.Contract.RenterOutput.Value,
		Collateral: rev.Contract.MissedHostValue,
	})
	if err != nil {
		return rhp.RPCRefreshContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

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

	// fetch corresponding host and check if it's theoretically usable
	host, err := cm.store.Host(ctx, contract.HostKey)
	if err != nil {
		return fmt.Errorf("failed to fetch host: %w", err)
	} else if !host.Usability.Usable() {
		contractLog.Debug("host is not usable")
		return nil
	}

	// scan host for valid price settings and make sure it's still usable
	scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	host, err = cm.scanner.ScanHost(scanCtx, host.PublicKey)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to scan host: %w", err)
	} else if !host.Usability.Usable() {
		contractLog.Debug("host is not usable after scan")
		return nil
	}

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

	contractor, err := cm.dialer.NewContractor(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		contractLog.Debug("failed to dial contractor", zap.Error(err))
		return nil
	}
	defer contractor.Close()
	res, err := contractor.RefreshContract(ctx, host.Settings, proto.RPCRefreshContractParams{
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

	if err := cm.store.AddRenewedContract(ctx, AddRenewedContractParams{
		RenewedFrom:      contract.ID,
		RenewedTo:        renewed.ID,
		ProofHeight:      renewed.Revision.ProofHeight,
		ExpirationHeight: renewed.Revision.ExpirationHeight,
		ContractPrice:    host.Settings.Prices.ContractPrice,
		Allowance:        renewed.Revision.RenterOutput.Value,
		MinerFee:         minerFee,
		UsedCollateral:   res.Contract.Revision.MissedHostValue,
		TotalCollateral:  renewed.Revision.TotalCollateral,
	}); err != nil {
		return fmt.Errorf("failed to store renewed contract: %w", err)
	}

	return nil
}
