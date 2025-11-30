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
		log.Warn("no hosts for pinning")
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
			defer func() {
				<-sema
				wg.Done()
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

	// dial the host
	client, err := cm.dialer.DialHost(ctx, host.PublicKey, host.RHP4Addrs())
	if err != nil {
		return fmt.Errorf("failed to dial host: %w", err)
	}
	defer client.Close()

	// fetch contract ids
	contractIDs, err := cm.store.ContractsForPinning(host.PublicKey, maxContractSize)
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

		if err := cm.pinSectors(ctx, client, host.PublicKey, host.Settings.Prices, contractIDs, roots, log); err != nil {
			return fmt.Errorf("failed to pin sectors: %w", err)
		}
	}

	return ctx.Err()
}

// pinSectors pins a set of sectors using the given set of contracts. It will
// attempt to pin all sectors, but may not be able to if the contracts run out of
// space. It will try to pin sectors using the contracts in the order they are
// provided. If the host refuses to pin a sector, it will be marked as lost.
func (cm *ContractManager) pinSectors(ctx context.Context, client HostClient, hostKey types.PublicKey, hostPrices proto.HostPrices, contractIDs []types.FileContractID, sectors []types.Hash256, log *zap.Logger) error {
	// NOTE: this is necessary to avoid looping forever
	// when [AppendSectors] returns an error for all contracts.
	var success bool
	for _, contractID := range contractIDs {
		if len(sectors) == 0 {
			break
		}
		log := log.With(zap.Stringer("contractID", contractID))

		// try to pin sectors to the contract
		res, attempted, err := client.AppendSectors(ctx, hostPrices, contractID, sectors)
		if err != nil {
			log.Debug("failed to pin sectors", zap.Error(err))
			continue
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
			return fmt.Errorf("failed to pin sectors: %w", err)
		}

		sectors = sectors[attempted:] // pin the remaining sectors
		log.Debug("pinned sectors", zap.Int("pinned", len(res.Sectors)), zap.Int("attempted", attempted))
	}
	if !success {
		return errors.New("all contracts failed to pin sectors")
	}
	return nil
}
