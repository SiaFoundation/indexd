package contracts

import (
	"context"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractRefreshes(ctx context.Context, period uint64, log *zap.Logger) error {
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

			if err := cm.refreshContract(ctx, contract, period, log); err != nil {
				log.Error("failed to refresh contract", zap.Stringer("contractID", contract.ID), zap.Error(err))
			}
		}

		if len(contracts) < batchSize {
			break
		}
	}

	return nil
}

func (cm *ContractManager) refreshContract(ctx context.Context, contract Contract, period uint64, log *zap.Logger) error {
	log = log.With(zap.Stringer("hostKey", contract.HostKey),
		zap.Stringer("contractID", contract.ID),
		zap.Stringer("remainingAllowance", contract.RemainingAllowance),
		zap.Stringer("initialAllowance", contract.InitialAllowance),
		zap.Stringer("usedCollateral", contract.UsedCollateral),
		zap.Stringer("totalCollateral", contract.TotalCollateral),
		zap.Bool("outOfFunds", contract.OutOfFunds()),
		zap.Bool("outOfCollateral", contract.OutOfCollateral()),
	)

	return cm.hosts.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		// calculate funding target
		minAllowance, err := cm.accounts.ContractFundTarget(ctx, host, minAllowance)
		if err != nil {
			return fmt.Errorf("failed to get fund target: %w", err)
		}

		// TODO: Right now this isn't quite correct since allowance and
		// collateral are added on top of the existing one. This should fix
		// itself once the new version of the refresh RPC is used. The problems
		// are:
		// 1. If the contract is out of funds we also add collateral and vice versa
		// 2. The total collateral might exceed the host's maximum collateral
		//    since 'contractFunding' doesn't take into account existing collateral
		allowance, collateral := contractFunding(host.Settings, contract.Size, minAllowance, minHostCollateral, period)

		refreshCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		hc, err := cm.dialer.DialHost(refreshCtx, host.PublicKey, host.RHP4Addrs())
		if err != nil {
			log.Debug("failed to dial host", zap.Error(err))
			return nil
		}
		defer hc.Close()

		res, err := hc.RefreshContract(refreshCtx, host.Settings, proto.RPCRefreshContractParams{
			Allowance:  allowance,
			Collateral: collateral,
			ContractID: contract.ID,
		})
		if err != nil {
			return fmt.Errorf("failed to refresh contract: %w", err)
		}
		log := log.With(zap.Stringer("newContractID", res.Contract.ID))
		if err := cm.wallet.BroadcastV2TransactionSet(res.RenewalSet.Basis, res.RenewalSet.Transactions); err != nil {
			// error is ignored as it is assumed the host has validated the transaction set.
			// It will eventually be mined or rejected. This is to prevent minor synchronization
			// differences from causing a renewal to not be registered in the database but later
			// confirmed.
			log.Warn("failed to broadcast contract refresh transaction set", zap.Error(err))
		}
		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(ctx, contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, res.Usage); err != nil {
			return fmt.Errorf("failed to store refreshed contract %q: %w", renewed.ID, err)
		}

		log.Info("successfully refreshed contract",
			zap.Stringer("computedAllowance", allowance),
			zap.Stringer("computedCollateral", collateral),
			zap.Stringer("newRemainingAllowance", renewed.Revision.RemainingAllowance()),
			zap.Stringer("newRemainingCollateral", renewed.Revision.RemainingCollateral()))
		return nil
	})
}
