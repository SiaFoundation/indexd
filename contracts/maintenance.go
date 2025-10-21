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
	if err := cm.performContractRenewals(ctx, settings.Period, settings.RenewWindow, log.Named("renew")); err != nil {
		return fmt.Errorf("failed to renew contracts: %w", err)
	}

	// refresh any good contracts that are either out of collateral or funds
	if err := cm.performContractRefreshes(ctx, settings.Period, log.Named("refresh")); err != nil {
		return fmt.Errorf("failed to perform contract refreshes: %w", err)
	}

	// mark any contracts too close to their expiration height as bad
	if err := cm.store.MarkUnrenewableContractsBad(ctx, blockHeight+settings.RenewWindow/2); err != nil {
		return fmt.Errorf("failed to mark unrenewable contracts bad: %w", err)
	}

	// form new contracts until there are enough good contracts to use
	if err := cm.performContractFormation(ctx, settings.Period, int64(settings.WantedContracts), log.Named("formation")); err != nil {
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
	t := time.NewTimer(cm.maintenanceFrequency)
	defer t.Stop()

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
			continue
		case <-cm.triggerMaintenanceChan:
			log.Debug("triggering maintenance")
		case <-t.C:
			log.Debug("starting scheduled maintenance")
		}

		contractMaintenanceLog := log.Named("contracts")
		logError(cm.performContractMaintenance(ctx, contractMaintenanceLog), contractMaintenanceLog)
		fundingLog := log.Named("accounts")
		logError(cm.performAccountFunding(ctx, false, fundingLog), fundingLog)
		pruningLog := log.Named("pruning")
		logError(cm.performContractPruning(ctx, false, pruningLog), pruningLog)
		pinningLog := log.Named("pinning")
		logError(cm.performSectorPinning(ctx, pinningLog), pinningLog)

		unpinnableLog := log.Named("unpinnable")
		threshold := time.Now().Add(-unpinnableSectorThreshold)
		logError(cm.store.MarkSectorsUnpinnable(ctx, threshold), unpinnableLog)
		t.Reset(cm.maintenanceFrequency)
		log.Debug("maintenance complete")
	}
}

func logError(err error, log *zap.Logger) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	log.Error("maintenance failed", zap.Error(err))
}
