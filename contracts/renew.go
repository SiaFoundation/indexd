package contracts

import (
	"context"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractRenewals(ctx context.Context, period, renewWindow uint64, log *zap.Logger) error {
	bh := cm.chain.TipState().Index.Height
	minProofHeight := bh + renewWindow
	newProofHeight := bh + period

	batchSize := 50
	for offset := 0; ; offset += batchSize {
		contracts, err := cm.store.Contracts(offset, batchSize, WithGood(true), WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for renewal: %w", err)
		}

		for _, contract := range contracts {
			if contract.ProofHeight > minProofHeight {
				continue // too early to renew
			} else if !contract.Good {
				continue // contract is bad
			}

			log := log.With(zap.Stringer("contractID", contract.ID), zap.Stringer("host", contract.HostKey))
			if err := cm.renewContract(ctx, contract, newProofHeight, log); err != nil {
				log.Error("failed to renew contract", zap.Error(err))
			}
		}

		if len(contracts) < batchSize {
			break
		}
	}

	return nil
}

func (cm *ContractManager) renewContract(ctx context.Context, contract Contract, proofHeight uint64, log *zap.Logger) error {
	return cm.hosts.WithScannedHost(ctx, contract.HostKey, func(host hosts.Host) error {
		// calculate funding target
		minAllowance, err := cm.ContractFundTarget(ctx, host, minAllowance)
		if err != nil {
			return fmt.Errorf("failed to get fund target: %w", err)
		}
		settings := host.Settings
		if settings.Prices.TipHeight > proofHeight {
			return fmt.Errorf("cannot renew contract with proof height %d before tip height %d", proofHeight, settings.Prices.TipHeight)
		}
		duration := proofHeight + proto.ProofWindow - settings.Prices.TipHeight

		// allowance is doubled to allow for two account funding cycles before next refresh
		allowance, collateral := contractFunding(settings, contract.Size, minAllowance.Mul64(2), duration)
		renewCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		lc, unlock := cm.cl.LockContract(contract.ID)
		defer unlock()

		var res rhp.RPCRenewContractResult
		err = cm.rev.WithRevision(renewCtx, lc, func(rev rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
			cappedCollateral := collateral
			estimatedRenewal, _ := proto.RenewContract(rev.Revision, settings.Prices, proto.RPCRenewContractParams{
				Allowance:   allowance,
				Collateral:  cappedCollateral,
				ContractID:  contract.ID,
				ProofHeight: proofHeight,
			})
			if estimatedRenewal.NewContract.TotalCollateral.Cmp(settings.MaxCollateral) > 0 {
				capped, underflow := settings.MaxCollateral.SubWithUnderflow(estimatedRenewal.NewContract.RiskedCollateral())
				if underflow {
					capped = types.ZeroCurrency
				}
				cappedCollateral = capped
			}

			var err error
			res, err = cm.client.RenewContract(renewCtx, cm.chain, cm.signer, client.RenewContractParams{
				Contract:    rev,
				Allowance:   allowance,
				Collateral:  cappedCollateral,
				ProofHeight: proofHeight,
			})
			if err != nil {
				return rhp.ContractRevision{}, proto.Usage{}, err
			}

			// renewals return the old (or 'renewed') revision, the revision of the
			// renewal will be persisted in the database when the renewed contract
			// is added
			return rev, res.Usage, nil
		})
		if err != nil {
			return fmt.Errorf("failed to renew contract: %w", err)
		}

		log = log.With(zap.Stringer("newContractID", res.Contract.ID))
		if err := cm.wallet.BroadcastV2TransactionSet(res.RenewalSet.Basis, res.RenewalSet.Transactions); err != nil {
			// error is ignored as it is assumed the host has validated the transaction set.
			// It will eventually be mined or rejected. This is to prevent minor synchronization
			// differences from causing a renewal to not be registered in the database but later
			// confirmed.
			log.Warn("failed to broadcast contract renewal transaction set", zap.Error(err))
		}

		renewed := res.Contract
		minerFee := res.RenewalSet.Transactions[len(res.RenewalSet.Transactions)-1].MinerFee

		if err := cm.store.AddRenewedContract(contract.ID, renewed.ID, renewed.Revision, host.Settings.Prices.ContractPrice, minerFee, res.Usage); err != nil {
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
