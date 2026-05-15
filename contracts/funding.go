package contracts

import (
	"context"
	"fmt"
	"math"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	// attachValidity is the replay window for pool attachment signatures.
	attachValidity = 5 * time.Minute

	// serviceAccountFundTargetBytes is the number of bytes used to calculate
	// the fund target for a host's service account. We fund accounts to cover
	// this amount of read and write usage. It roughly comes down to uploading
	// and downloading to and from a host at ~1Gbps for a period of 2 minutes.
	// With 30 good hosts, this results in about 30Gbps of maximum theoretical
	// throughput.
	serviceAccountFundTargetBytes = uint64(16 << 30) // 16 GiB
)

// FundAccounts funds service accounts for the given host. User accounts draw
// from balance pools instead, see ContractManager.FundPools.
func (cm *ContractManager) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, log *zap.Logger) error {
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

	serviceAccounts := cm.accounts.ServiceAccounts(host.PublicKey)
	if len(serviceAccounts) == 0 {
		return nil
	}

	fundTarget := accounts.HostFundTarget(host, serviceAccountFundTargetBytes)
	funded, _, err := cm.accountFunder.FundAccounts(ctx, host, contractIDs, serviceAccounts, fundTarget, log)
	if err != nil {
		return fmt.Errorf("failed to fund service accounts: %w", err)
	}

	if err := cm.accounts.UpdateServiceAccounts(serviceAccounts[:funded], fundTarget); err != nil {
		cm.log.Warn("failed to update service account balances", zap.Error(err))
	}

	return nil
}

// ContractFundTarget calculates the fund target for a contract on the given
// host. It sums the pool fund targets for each individual pool, scaled by
// active accounts, plus the service account fund target.
func (cm *ContractManager) ContractFundTarget(ctx context.Context, host hosts.Host, minAllowance types.Currency) (types.Currency, error) {
	poolInfos, err := cm.accounts.PoolFundingInfo()
	if err != nil {
		return types.ZeroCurrency, err
	}

	var target types.Currency
	for _, pi := range poolInfos {
		target = target.Add(accounts.PoolFundTarget(host, pi.FundTargetBytes, pi.ActiveAccounts))
	}

	// service accounts
	target = target.Add(accounts.HostFundTarget(host, serviceAccountFundTargetBytes).Mul64(uint64(len(cm.accounts.ServiceAccounts(host.PublicKey)))))

	if target.Cmp(minAllowance) < 0 {
		target = minAllowance
	}

	return target, nil
}

// FundPools attempts to fund all pools for the given host key. It does so
// using the provided contract IDs, which are used in the order they're given.
// After funding, it attaches pools to any accounts that are not yet attached.
func (cm *ContractManager) FundPools(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, force bool, log *zap.Logger) error {
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

	if force {
		if err := cm.accounts.SchedulePoolsForFunding(host.PublicKey); err != nil {
			return fmt.Errorf("failed to schedule pools for funding: %w", err)
		}
	}

	quotas, err := cm.accounts.Quotas(ctx, 0, math.MaxInt)
	if err != nil {
		return fmt.Errorf("failed to fetch quotas: %w", err)
	}

OUTER:
	for _, quota := range quotas {
		if accounts.HostFundTarget(host, quota.FundTargetBytes).IsZero() {
			continue
		}

		var exhausted bool
		for !exhausted {
			pools, err := cm.accounts.PoolsForFunding(host.PublicKey, quota.Key, proto.MaxAccountBatchSize)
			if err != nil {
				return fmt.Errorf("failed to fetch pools for funding: %w", err)
			} else if len(pools) < proto.MaxAccountBatchSize {
				exhausted = true
			}
			if len(pools) == 0 {
				break
			}

			// fund each pool individually since each has a different target
			// based on its number of active accounts
			for i := range pools {
				target := accounts.PoolFundTarget(host, quota.FundTargetBytes, pools[i].ActiveAccounts)
				funded, drained, err := cm.accountFunder.FundPools(ctx, host, contractIDs, pools[i:i+1], target, log)
				if err != nil {
					return fmt.Errorf("failed to fund pools: %w", err)
				}

				accounts.UpdateFundedPools(pools[i:i+1], funded, cm.maxAccountFundingBackoff)

				contractIDs = contractIDs[drained:]
				if len(contractIDs) == 0 {
					log.Debug("not all pools could be funded, no more contracts available", zap.String("quota", quota.Key))
					// update the pools we processed so far
					if err := cm.accounts.UpdateHostPools(pools[:i+1]); err != nil {
						return fmt.Errorf("failed to update pools: %w", err)
					}
					break OUTER
				}
			}

			if err := cm.accounts.UpdateHostPools(pools); err != nil {
				return fmt.Errorf("failed to update pools: %w", err)
			}
		}
	}

	if err := cm.attachPools(ctx, host.PublicKey, log); err != nil {
		return fmt.Errorf("failed to attach pools: %w", err)
	}

	return nil
}

func (cm *ContractManager) attachPools(ctx context.Context, hostKey types.PublicKey, log *zap.Logger) error {
	for {
		pending, err := cm.accounts.PendingPoolAttachments(hostKey, proto.MaxAccountBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch pending attachments: %w", err)
		} else if len(pending) == 0 {
			return nil
		}

		inputs := make([]rhp.PoolAttachInput, len(pending))
		for i, p := range pending {
			inputs[i] = rhp.PoolAttachInput{
				Account: p.AccountKey,
				PoolKey: p.PoolKey,
			}
		}

		if err := cm.accountFunder.AttachPools(ctx, hostKey, inputs, attachValidity); err != nil {
			return fmt.Errorf("failed to attach pools: %w", err)
		}

		if err := cm.accounts.InsertPoolAttachments(hostKey, pending); err != nil {
			return fmt.Errorf("failed to record attachments: %w", err)
		}

		log.Debug("attached pools to accounts", zap.Int("count", len(pending)))

		if len(pending) < proto.MaxAccountBatchSize {
			return nil
		}
	}
}
