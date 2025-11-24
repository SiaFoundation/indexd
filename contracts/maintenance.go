package contracts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performAccountFunding(ctx context.Context, force bool, log *zap.Logger) error {
	start := time.Now()

	// fetch hosts
	hostsToFund, err := cm.hosts.HostsForFunding(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts for account funding: %w", err)
	}

	// fund accounts on all hosts
	var wg sync.WaitGroup
	for _, hk := range hostsToFund {
		wg.Add(1)
		go func(ctx context.Context, hostKey types.PublicKey, log *zap.Logger) {
			ctx, cancel := context.WithTimeout(ctx, fundTimeout)
			defer func() {
				wg.Done()
				cancel()
			}()

			contractIDs, err := cm.store.ContractsForFunding(hostKey, 10)
			if err != nil {
				log.Error("failed to fetch contracts for funding", zap.Error(err))
				return
			} else if len(contractIDs) == 0 {
				log.Debug("no contracts for funding")
				return
			}

			if err := cm.hosts.WithScannedHost(ctx, hostKey, func(host hosts.Host) error {
				return cm.accounts.FundAccounts(ctx, host, contractIDs, force, log)
			}); err != nil {
				log.Debug("failed to fund accounts", zap.Error(err))
			}
		}(ctx, hk, log.With(zap.Stringer("hostKey", hk)))
	}
	wg.Wait()

	log.Debug("funding finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performContractMaintenance(ctx context.Context, log *zap.Logger) error {
	// fetch settings and determine if maintenance is supposed to run
	settings, err := cm.store.MaintenanceSettings()
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

	// mark any contracts too close to their expiration height as bad
	if err := cm.store.MarkUnrenewableContractsBad(blockHeight + settings.RenewWindow/2); err != nil {
		return fmt.Errorf("failed to mark unrenewable contracts bad: %w", err)
	}

	// form new contracts until there are enough good contracts to use
	if err := cm.performContractFormation(ctx, settings, blockHeight, log.Named("maintenance")); err != nil {
		return fmt.Errorf("failed to form contracts: %w", err)
	}

	// rebroadcast revisions for all good contracts
	if err := cm.performBroadcastContractRevisions(log); err != nil {
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
			log.Debug("shutting down maintenance loop")
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

		// this is done first so that fragmenting the wallet can be prioritized before
		// being used for other maintenance tasks
		walletLog := log.Named("wallet")
		if err := cm.performWalletMaintenance(ctx, walletLog); err != nil {
			log.Debug("maintenance failed", zap.Error(err)) // wallet maintenance is best-effort
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
		logError(cm.store.MarkSectorsUnpinnable(threshold), unpinnableLog)
		t.Reset(cm.maintenanceFrequency)
		log.Debug("maintenance complete")
	}
}

func (cm *ContractManager) performWalletMaintenance(ctx context.Context, log *zap.Logger) error {
	const maxUTXOs = 250 // cap at 250 UTXOs

	settings, err := cm.store.MaintenanceSettings()
	if err != nil {
		return fmt.Errorf("failed to fetch maintenance settings: %w", err)
	}

	// estimate the number of UTXOs needed per block based
	// on the number of hosts we have a contract with since
	// each contract potentially requires maintenance (renewal, funding, etc).
	hosts, err := cm.hosts.Hosts(ctx, 0, maxUTXOs, hosts.WithActiveContracts(true), hosts.WithUsable(true))
	if err != nil {
		return fmt.Errorf("failed to fetch active contracts: %w", err)
	}

	utxoCount := min(max(len(hosts), int(settings.WantedContracts), 1), maxUTXOs)

	// note: 1KS is arbitrary, but it's a minimum. The actual value depends on
	// the largest UTXO the wallet has. It might be better to make it configurable
	// in a follow-up, but we should see how this performs first.
	//
	// These values mean that only a UTXO >= wanted contracts * 1KS will be
	// split.
	if txn, err := cm.wallet.SplitUTXO(utxoCount, types.Siacoins(1000)); err != nil {
		return fmt.Errorf("failed to split UTXOs: %w", err)
	} else if txn.ID() == (types.TransactionID{}) || len(txn.SiacoinInputs) == 0 || len(txn.SiacoinOutputs) == 0 {
		log.Debug("enough UTXOs present, no split needed")
	} else {
		input := txn.SiacoinInputs[0].Parent.SiacoinOutput.Value
		output := txn.SiacoinOutputs[0].Value
		log.Info("split UTXO for contract funding", zap.Stringer("txnID", txn.ID()), zap.Stringer("fee", txn.MinerFee), zap.Stringer("input", input), zap.Stringer("output", output), zap.Int("created", len(txn.SiacoinOutputs)))
	}

	return nil
}

func logError(err error, log *zap.Logger) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	log.Error("maintenance failed", zap.Error(err))
}
