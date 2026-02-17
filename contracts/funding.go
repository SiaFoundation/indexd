package contracts

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	// serviceAccountFundTargetBytes is the number of bytes used to calculate
	// the fund target for a host's service account. We fund accounts to cover
	// this amount of read and write usage. It roughly comes down to uploading
	// and downloading to and from a host at ~1Gbps for a period of 2 minutes.
	// With 30 good hosts, this results in about 30Gbps of maximum theoretical
	// throughput.
	serviceAccountFundTargetBytes = uint64(16 << 30) // 16 GiB
)

// FundAccounts attempts to fund all accounts for the given host key. It does so
// using the provided contract IDs, which are used in the order they're given.
func (cm *ContractManager) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, force bool, log *zap.Logger) error {
	// sanity check input
	if len(contractIDs) == 0 {
		log.Debug("no contracts provided")
		return nil
	} else if host.Blocked {
		log.Debug("host is blocked")
		return nil
	} else if !host.Usability.Usable() {
		log.Debug("host is not usable")
		return nil
	}

	// if we want to force a refill on all accounts, we need to manually set the
	// next fund time, we do this to avoid having to fetch (and update) all
	// accounts at once
	if force {
		if err := cm.accounts.ScheduleAccountsForFunding(host.PublicKey); err != nil {
			return fmt.Errorf("failed to schedule accounts for funding: %w", err)
		}
	}

	quotas, err := cm.accounts.Quotas(ctx, 0, math.MaxInt)
	if err != nil {
		return fmt.Errorf("failed to fetch quotas: %w", err)
	}

	serviceAccounts := cm.accounts.ServiceAccounts(host.PublicKey)
	if len(serviceAccounts) > 0 {
		fundTarget := accounts.HostFundTarget(host, serviceAccountFundTargetBytes)
		// fund them
		funded, _, err := cm.accountFunder.FundAccounts(ctx, host, contractIDs, serviceAccounts, fundTarget, log)
		if err != nil {
			return fmt.Errorf("failed to fund service accounts: %w", err)
		}

		// update service account balances
		if err := cm.accounts.UpdateServiceAccounts(ctx, serviceAccounts[:funded], fundTarget); err != nil {
			cm.log.Warn("failed to update service account balances", zap.Error(err))
		}
	}

	threshold := time.Now().Add(-accounts.AccountActivityThreshold)
OUTER:
	for _, quota := range quotas {
		fundTarget := accounts.HostFundTarget(host, quota.FundTargetBytes)
		if fundTarget.IsZero() {
			continue
		}

		var exhausted bool
		for !exhausted {
			accs, err := cm.accounts.AccountsForFunding(host.PublicKey, quota.Key, threshold, accounts.AccountFundBatch)
			if err != nil {
				return fmt.Errorf("failed to fetch accounts for funding: %w", err)
			} else if len(accs) < accounts.AccountFundBatch {
				exhausted = true
			}
			if len(accs) == 0 {
				break
			}

			// fund accounts
			funded, drained, err := cm.accountFunder.FundAccounts(ctx, host, contractIDs, accs, fundTarget, log)
			if err != nil {
				return fmt.Errorf("failed to fund accounts: %w", err)
			}

			// update funded accounts
			accounts.UpdateFundedAccounts(accs, funded)
			err = cm.accounts.UpdateHostAccounts(accs)
			if err != nil {
				return fmt.Errorf("failed to update accounts: %w", err)
			}

			contractIDs = contractIDs[drained:]
			if len(contractIDs) == 0 {
				log.Debug("not all accounts could be funded, no more contracts available", zap.String("quota", quota.Key))
				break OUTER
			}
		}
	}

	return nil
}

// ContractFundTarget calculates the fund target for a contract on the given
// host. We scale the fund target by the number of active accounts per quota.
func (cm *ContractManager) ContractFundTarget(ctx context.Context, host hosts.Host, minAllowance types.Currency) (types.Currency, error) {
	quotaInfos, err := cm.accounts.AccountFundingInfo(time.Now().Add(-accounts.AccountActivityThreshold))
	if err != nil {
		return types.ZeroCurrency, err
	}

	// user accounts
	var target types.Currency
	for _, qi := range quotaInfos {
		t := accounts.HostFundTarget(host, qi.FundTargetBytes).Mul64(qi.ActiveAccounts)
		target = target.Add(t)
	}

	// service accounts
	target = target.Add(accounts.HostFundTarget(host, serviceAccountFundTargetBytes).Mul64(uint64(len(cm.accounts.ServiceAccounts(host.PublicKey)))))

	// ensure target is at least minAllowance
	if target.Cmp(minAllowance) < 0 {
		target = minAllowance
	}

	return target, nil
}
