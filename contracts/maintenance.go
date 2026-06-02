package contracts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

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
	if err := cm.performBroadcastContractRevisions(ctx, log); err != nil {
		return fmt.Errorf("failed to broadcast contract revisions: %w", err)
	}

	return nil
}

func (cm *ContractManager) performAccountFunding(ctx context.Context, log *zap.Logger) error {
	start := time.Now()

	// fetch hosts
	hostsToFund, err := cm.hosts.HostsForFunding(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts for account funding: %w", err)
	}

	// fetch quotas
	quotas, err := cm.accounts.Quotas(ctx, 0, math.MaxInt)
	if err != nil {
		return fmt.Errorf("failed to fetch quotas: %w", err)
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

			// fetch contracts for funding
			contractIDs, err := cm.store.ContractsForFunding(hostKey, 10)
			if err != nil {
				log.Error("failed to fetch contracts for funding", zap.Error(err))
				return
			}

			// fund accounts
			if len(contractIDs) > 0 {
				err := cm.hosts.WithScannedHost(ctx, hostKey, func(host hosts.Host) error {
					// fund service accounts
					err := cm.FundServiceAccounts(ctx, host, contractIDs, log)
					if err != nil {
						log.Debug("failed to fund service accounts", zap.Error(err))
					}

					// fund account pools
					err = cm.FundPools(ctx, host, contractIDs, quotas, log)
					if err != nil {
						log.Debug("failed to fund account pools", zap.Error(err))
					}

					// fund legacy accounts
					err = cm.FundAccounts(ctx, host, contractIDs, quotas, log)
					if err != nil {
						log.Debug("failed to fund accounts", zap.Error(err))
					}

					return nil
				})
				if err != nil {
					log.Debug("failed to scan host for funding", zap.Error(err))
				}
			} else {
				log.Debug("no contracts for funding")
			}

			// always attach pools, even without contracts
			if err := cm.AttachPools(ctx, hostKey, log); err != nil {
				log.Debug("failed to attach pools", zap.Error(err))
			}
		}(ctx, hk, log.With(zap.Stringer("hostKey", hk)))
	}
	wg.Wait()

	log.Debug("funding finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

// maintenanceLoop performs any background tasks that the contract manager needs
// to perform on contracts
func (cm *ContractManager) maintenanceLoop(ctx context.Context) {
	log := cm.log.Named("maintenance")
	var wg sync.WaitGroup
	defer wg.Wait()

	// account funding loop
	wg.Go(func() {
		t := time.NewTicker(cm.maintenanceFrequency)
		defer t.Stop()
		for {
			if !cm.waitUntilSynced(ctx, log) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-cm.triggerFundingChan:
				log.Debug("triggering scheduled account funding")
			case <-t.C:
				log.Debug("starting scheduled funding")
			}
			fundingLog := log.Named("funding")
			logError(cm.performAccountFunding(ctx, fundingLog), fundingLog)
		}
	})

	// contract maintenance loop
	wg.Go(func() {
		t := time.NewTicker(cm.maintenanceFrequency)
		defer t.Stop()
		for {
			if !cm.waitUntilSynced(ctx, log) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Debug("starting scheduled contract maintenance")
			}
			walletLog := log.Named("wallet")
			if err := cm.performWalletMaintenance(ctx, walletLog); err != nil {
				log.Debug("maintenance failed", zap.Error(err)) // wallet maintenance is best-effort
			}
			maintenanceLog := log.Named("contracts")
			logError(cm.performContractMaintenance(ctx, maintenanceLog), maintenanceLog)
		}
	})

	// remaining maintenance loop
	wg.Go(func() {
		t := time.NewTicker(cm.maintenanceFrequency)
		defer t.Stop()
		for {
			if !cm.waitUntilSynced(ctx, log) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Debug("starting scheduled maintenance")
			}

			pruningLog := log.Named("pruning")
			logError(cm.performContractPruning(ctx, pruningLog), pruningLog)
			pinningLog := log.Named("pinning")
			logError(cm.performSectorPinning(ctx, pinningLog), pinningLog)

			unpinnableLog := log.Named("unpinnable")
			threshold := time.Now().Add(-unpinnableSectorThreshold)
			logError(cm.store.MarkSectorsUnpinnable(threshold), unpinnableLog)
			log.Debug("maintenance complete")
		}
	})
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
