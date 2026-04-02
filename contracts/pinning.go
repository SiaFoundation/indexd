package contracts

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performSectorPinning(ctx context.Context, log *zap.Logger) error {
	start := time.Now()

	// fetch hosts for pinning, a host is eligible for pinning if it is not
	// blocked, has unpinned sectors and has an active contract
	hfp, err := cm.hosts.HostsForPinning(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts for pinning: %w", err)
	} else if len(hfp) == 0 {
		log.Debug("no hosts for pinning")
		return nil
	}

	var wg sync.WaitGroup
	sema := make(chan struct{}, 50)
	defer close(sema)

loop:
	for _, hostKey := range hfp {
		select {
		case <-ctx.Done():
			break loop
		case sema <- struct{}{}:
		}

		wg.Add(1)
		go func(ctx context.Context, hostKey types.PublicKey, hostLog *zap.Logger) {
			ctx, cancel := context.WithTimeoutCause(ctx, pinTimeout, client.ErrAbortedRPC)
			defer func() {
				<-sema
				wg.Done()
				cancel()
			}()

			if err := cm.hosts.WithScannedHost(ctx, hostKey, func(host hosts.Host) error {
				return cm.performSectorPinningOnHost(ctx, host, hostLog)
			}); err != nil {
				hostLog.Debug("failed to pin sectors", zap.Error(err))
				return
			}
		}(ctx, hostKey, log.With(zap.Stringer("hostKey", hostKey)))
	}

	wg.Wait()

	log.Debug("pinning finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performSectorPinningOnHost(ctx context.Context, host hosts.Host, log *zap.Logger) error {
	// check host is good
	if !host.IsGood() {
		return fmt.Errorf("host is bad: blocked=%t, usable=%t", host.Blocked, host.Usability.Usable())
	}

	ms, err := cm.store.MaintenanceSettings()
	if err != nil {
		return fmt.Errorf("failed to fetch maintenance settings for sector pinning: %w", err)
	}
	maxRenewableSize := maxRenewableContractSize(host.Settings, ms.Period)

	// fetch contract ids
	contractIDs, err := cm.store.ContractsForPinning(host.PublicKey, maxRenewableSize)
	if err != nil {
		return fmt.Errorf("failed to fetch contracts for pinning: %w", err)
	} else if len(contractIDs) == 0 {
		return errors.New("no contracts for pinning")
	}

	var exhausted bool
	for !exhausted && ctx.Err() == nil {
		roots, err := cm.store.UnpinnedSectors(host.PublicKey, proto.MaxSectorBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch unpinned sectors: %w", err)
		} else if len(roots) < proto.MaxSectorBatchSize {
			exhausted = true
		}

		if err := cm.pinSectors(ctx, host.PublicKey, contractIDs, roots, maxRenewableSize, log); err != nil {
			return fmt.Errorf("failed to pin sectors: %w", err)
		}
	}

	return ctx.Err()
}

// pinSectors pins a set of sectors using the given set of contracts. It will
// attempt to pin all sectors, but may not be able to if the contracts run out of
// space. It will try to pin sectors using the contracts in the order they are
// provided. If the host refuses to pin a sector, it will be marked as lost.
func (cm *ContractManager) pinSectors(ctx context.Context, hostKey types.PublicKey, contractIDs []types.FileContractID, sectors []types.Hash256, maxSize uint64, log *zap.Logger) error {
	if len(sectors) > proto.MaxSectorBatchSize {
		return fmt.Errorf("too many sectors, %d > %d", len(sectors), proto.MaxSectorBatchSize) // developer error
	}

	// NOTE: this is necessary to avoid looping forever
	// when [AppendSectors] returns an error for all contracts.
	var success bool
	for _, contractID := range contractIDs {
		if len(sectors) == 0 {
			break
		}

		if err := func() error {
			log := log.With(zap.Stringer("contractID", contractID))

			lc, unlock := cm.cl.LockContract(contractID)
			defer unlock()

			prices, err := cm.client.Prices(ctx, hostKey)
			if err != nil {
				log.Debug("failed to get host prices", zap.Error(err))
				return nil
			}

			var res rhp.RPCAppendSectorsResult
			var attempted int
			err = cm.rev.WithRevision(ctx, lc, func(contract rhp.ContractRevision) (_ rhp.ContractRevision, _ proto.Usage, err error) {
				if contract.Revision.Filesize >= maxSize {
					return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("contract is too large, %d > %d: %w", contract.Revision.Filesize, maxSize, ErrContractMaxSize)
				}

				// calculate the maximum number of sectors we can append based on the
				// contract's remaining capacity and collateral
				maxRemainingSectors := (maxSize - contract.Revision.Filesize) / proto.SectorSize
				maxAppendSectors := (contract.Revision.Capacity - contract.Revision.Filesize) / proto.SectorSize

				if contract.Revision.ExpirationHeight <= prices.TipHeight {
					return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("contract has expired at height %d, current height is %d", contract.Revision.ExpirationHeight, prices.TipHeight)
				}

				duration := contract.Revision.ExpirationHeight - prices.TipHeight
				appendSectorCost := prices.RPCAppendSectorsCost(1, duration)

				sectorCollateralCost := appendSectorCost.RiskedCollateral
				if sectorCollateralCost.IsZero() {
					sectorCollateralCost = types.NewCurrency64(1) // avoid division by zero
				}
				sectorRenterCost := appendSectorCost.RenterCost()
				if sectorRenterCost.IsZero() {
					sectorRenterCost = types.NewCurrency64(1) // avoid division by zero
				}

				maxSectorsByCollateral := contract.Revision.RemainingCollateral().Div(sectorCollateralCost).Big().Uint64()
				maxSectorsByAllowance := contract.Revision.RemainingAllowance().Div(sectorRenterCost).Big().Uint64()
				maxAppendSectors += min(maxSectorsByAllowance, maxSectorsByCollateral)

				// ensure the maximum contract size is not exceeded
				maxAppendSectors = min(maxAppendSectors, maxRemainingSectors)

				if maxAppendSectors == 0 {
					switch {
					case maxSectorsByAllowance == 0:
						return rhp.ContractRevision{}, proto.Usage{}, ErrContractOutOfFunds
					case maxSectorsByCollateral == 0:
						return rhp.ContractRevision{}, proto.Usage{}, ErrContractOutOfCollateral
					case maxRemainingSectors == 0:
						return rhp.ContractRevision{}, proto.Usage{}, ErrContractMaxSize
					}
					return rhp.ContractRevision{}, proto.Usage{}, errors.New("maxAppendSectors is zero for unknown reason") // unreachable
				}

				// only attempt to append up to the calculated maximum number of sectors
				attemptedSectors := sectors
				if uint64(len(attemptedSectors)) > maxAppendSectors {
					attemptedSectors = attemptedSectors[:maxAppendSectors]
				}
				res, err = cm.client.AppendSectors(ctx, cm.signer, cm.chain, contract, attemptedSectors)
				if err != nil {
					return rhp.ContractRevision{}, proto.Usage{}, fmt.Errorf("failed to append sectors: %w", err)
				}
				attempted = len(attemptedSectors)
				contract.Revision = res.Revision
				return contract, res.Usage, nil
			})
			if err != nil {
				log.Debug("failed to pin sectors", zap.Error(err))
				return nil
			}
			// if the RPC returned, regardless of how many sectors were pinned,
			// consider it a success as we made progress towards pinning unpinned
			// sectors.
			success = true

			// Only the sectors that were attempted should be marked
			// as missing. So sectors that were not part of the append
			// call can be pinned to other contracts.
			if len(res.Sectors) != attempted {
				lookup := make(map[types.Hash256]struct{}, attempted)
				for _, sector := range sectors[:attempted] {
					lookup[sector] = struct{}{}
				}
				for _, sector := range res.Sectors {
					delete(lookup, sector)
				}
				missing := slices.Collect(maps.Keys(lookup))
				if err := cm.store.MarkSectorsLost(hostKey, missing); err != nil {
					return fmt.Errorf("failed to mark sectors as lost: %w", err)
				}
				log = log.With(zap.Int("missing", len(missing)))
			}

			if err := cm.store.PinSectors(contractID, res.Sectors); err != nil {
				// log unexpected database error
				if !errors.Is(err, ErrNotFound) {
					log.Error("failed to pin sectors", zap.Stringer("contractID", contractID), zap.Error(err))
				}
				return fmt.Errorf("failed to pin sectors: %w", err)
			}

			sectors = sectors[attempted:] // pin the remaining sectors
			log.Debug("pinned sectors", zap.Int("pinned", len(res.Sectors)), zap.Int("attempted", attempted))
			return nil
		}(); err != nil {
			return err
		}
	}
	if !success {
		return errors.New("all contracts failed to pin sectors")
	}
	return nil
}
