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

	var eligible, attempted, successful int
	// renew at most one empty contract per host to keep a single empty contract
	// for potential uploads instead of locking up collateral on several. emptiness is
	// determined by Size rather than Capacity: a renewal sets the new contract's
	// capacity to the old contract's size, so a contract with no stored data
	// always renews into a capacity-0 contract. empty contracts are deferred and
	// renewed after the scan, once every active empty contract is known.
	var emptyEligible []Contract
	hostsWithActiveEmptyContract := make(map[types.PublicKey]struct{})
	batchSize := 50
	for offset := 0; ; offset += batchSize {
		contracts, err := cm.store.Contracts(offset, batchSize, WithGood(true), WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for renewal: %w", err)
		}
		eligible += len(contracts)
		for _, contract := range contracts {
			if contract.ProofHeight > minProofHeight {
				// too early to renew; an empty one is the host's retained empty contract
				if contract.Size == 0 {
					hostsWithActiveEmptyContract[contract.HostKey] = struct{}{}
				}
				continue
			} else if contract.Size == 0 {
				// defer empty contracts so at most one per host is renewed
				emptyEligible = append(emptyEligible, contract)
				continue
			}
			attempted++
			log := log.With(zap.Stringer("contractID", contract.ID), zap.Stringer("host", contract.HostKey))
			if err := cm.renewContract(ctx, contract, newProofHeight, log); err != nil {
				log.Error("failed to renew contract", zap.Error(err))
			} else {
				successful++
			}
		}

		if len(contracts) < batchSize {
			break
		}
	}

	// renew at most one empty contract per host, skipping hosts that already have
	// an active empty contract. the rest are left to expire.
	for _, contract := range emptyEligible {
		log := log.With(zap.Stringer("contractID", contract.ID), zap.Stringer("host", contract.HostKey))
		if _, ok := hostsWithActiveEmptyContract[contract.HostKey]; ok {
			log.Debug("skipping empty contract renewal, host already has an active empty contract")
			continue
		}
		attempted++
		if err := cm.renewContract(ctx, contract, newProofHeight, log); err != nil {
			log.Error("failed to renew contract", zap.Error(err))
		} else {
			// the renewal is also empty and is now the host's retained empty contract
			hostsWithActiveEmptyContract[contract.HostKey] = struct{}{}
			successful++
		}
	}

	log.Debug("renewals finished", zap.Int("eligible", eligible), zap.Int("attempted", attempted), zap.Int("successful", successful))
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
			estimatedRenewal, _ := proto.RenewContract(rev.Revision, settings.Prices, settings.WalletAddress, proto.RPCRenewContractParams{
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
