package contracts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performAccountFunding(ctx context.Context, force bool, log *zap.Logger) error {
	start := time.Now()
	log = log.Named("accounts")

	// fund accounts on usable hosts with active contracts
	opts := []hosts.HostQueryOpt{
		hosts.WithUsable(true),
		hosts.WithBlocked(false),
		hosts.WithActiveContracts(true),
	}

	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		// fetch hosts
		hostsToFund, err := cm.hosts.Hosts(ctx, offset, batchSize, opts...)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts for account funding: %w", err)
		}

		// fund accounts on all hosts
		var wg sync.WaitGroup
		for _, host := range hostsToFund {
			wg.Add(1)
			go func(ctx context.Context, host hosts.Host, log *zap.Logger) {
				ctx, cancel := context.WithTimeout(ctx, fundTimeout)
				defer func() {
					wg.Done()
					cancel()
				}()

				contractIDs, err := cm.store.ContractsForFunding(ctx, host.PublicKey, 10)
				if err != nil {
					log.Error("failed to fetch contracts for funding", zap.Error(err))
					return
				} else if len(contractIDs) == 0 {
					log.Debug("no contracts for funding")
					return
				}

				err = cm.accounts.FundAccounts(ctx, host, contractIDs, force, log)
				if err != nil {
					log.Debug("failed to fund accounts", zap.Error(err))
					return
				}
			}(ctx, host, log.With(zap.Stringer("hostKey", host.PublicKey)))
		}
		wg.Wait()

		if len(hostsToFund) < batchSize {
			break
		}
	}

	log.Debug("funding finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performContractMaintenance(ctx context.Context, log *zap.Logger) error {
	log.Debug("performing contract maintenance")

	// fetch settings and determine if maintenance is supposed to run
	settings, err := cm.store.MaintenanceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch settings for contract maintenance: %w", err)
	} else if !settings.Enabled {
		log.Debug("contract maintenance is disabled, skipping")
		return nil
	}

	blockHeight := cm.chain.TipState().Index.Height

	// block bad hosts we have contracts with
	if err := cm.blockBadHosts(ctx); err != nil {
		return fmt.Errorf("failed to block bad hosts: %w", err)
	}

	// renew any good contracts within their renew window
	if err := cm.performContractRenewals(ctx, settings.Period, settings.RenewWindow, log); err != nil {
		return fmt.Errorf("failed to renew contracts: %w", err)
	}

	// refresh any good contracts that are either out of collateral or funds
	if err := cm.performContractRefreshes(ctx, settings.Period, log); err != nil {
		return fmt.Errorf("failed to perform contract refreshes: %w", err)
	}

	// mark any contracts too close to their expiration height as bad
	if err := cm.store.MarkUnrenewableContractsBad(ctx, blockHeight+settings.RenewWindow/2); err != nil {
		return fmt.Errorf("failed to mark unrenewable contracts bad: %w", err)
	}

	// form new contracts until there are enough good contracts to use
	if err := cm.performContractFormation(ctx, settings.Period, int64(settings.WantedContracts), log); err != nil {
		return fmt.Errorf("failed to form contracts: %w", err)
	}

	// rebroadcast revisions for all good contracts
	if err := cm.performBroadcastContractRevisions(ctx, log); err != nil {
		return fmt.Errorf("failed to broadcast contract revisions: %w", err)
	}

	return nil
}

// maintenanceLoop performs any background tasks that the contract manager needs
// to perform on contracts
func (cm *ContractManager) maintenanceLoop(ctx context.Context) {
	log := cm.log.Named("maintenance")

	ticker := time.NewTicker(cm.maintenanceFrequency)
	defer ticker.Stop()

	for {
		if !cm.waitUntilSynced(ctx, log) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case force := <-cm.triggerFundingChan:
			log.Debug("triggering account funding", zap.Bool("force", force))
			if err := cm.performAccountFunding(ctx, force, log); err != nil {
				log.Error("account funding failed", zap.Error(err))
			}
			continue
		case <-cm.triggerPruningChan:
			log.Debug("triggering contract pruning")
			if err := cm.performContractPruning(ctx, true, log); err != nil {
				log.Error("contract pruning failed", zap.Error(err))
			}
		case <-cm.triggerMaintenanceChan:
			// reset ticker
			ticker.Stop()
			ticker = time.NewTicker(cm.maintenanceFrequency)

			log.Debug("triggering maintenance")
		case <-ticker.C:
		}

		if !performMaintenanceJob("contract maintenance", func() error {
			return cm.performContractMaintenance(ctx, log)
		}, log) {
			return
		}

		if !performMaintenanceJob("account funding", func() error {
			return cm.performAccountFunding(ctx, false, log)
		}, log) {
			return
		}

		if !performMaintenanceJob("contract pruning", func() error {
			return cm.performContractPruning(ctx, false, log)
		}, log) {
			return
		}

		if !performMaintenanceJob("sector pinning", func() error {
			return cm.performSectorPinning(ctx, log)
		}, log) {
			return
		}

		threshold := time.Now().Add(-pruneUnpinnableThreshold)
		if !performMaintenanceJob("pruning unpinnable sectors", func() error {
			return cm.store.PruneUnpinnableSectors(ctx, threshold)
		}, log) {
			return
		}
	}
}

func performMaintenanceJob(descr string, fn func() error, log *zap.Logger) bool {
	if err := fn(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		log.Error(descr+" failed", zap.Error(err))
	}
	return true
}
