package contracts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	pruneIntervalSuccess = 24 * time.Hour
	pruneIntervalFailure = 3 * time.Hour
)

func (cm *ContractManager) performContractPruning(ctx context.Context, force bool, log *zap.Logger) error {
	start := time.Now()

	// if force is true, schedule all (active and good) contracts for pruning
	if force {
		err := cm.store.ScheduleContractsForPruning(ctx)
		if err != nil {
			return fmt.Errorf("failed to schedule contracts for pruning: %w", err)
		}
	}

	// fetch hosts for pruning, a host is eligible for pruning if it is not
	// blocked and has active contracts that haven't been pruned in the last 24
	// hours
	hfp, err := cm.hosts.HostsForPruning(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts for pruning: %w", err)
	} else if len(hfp) == 0 {
		log.Warn("no hosts for pruning")
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
				hostLog = hostLog.With(zap.Stringer("protocolVersion", host.Settings.ProtocolVersion))
				return cm.performContractPruningOnHost(ctx, host, hostLog)
			}); err != nil {
				hostLog.Debug("failed to prune contracts", zap.Error(err))
			}
		}(ctx, hostKey, log.With(zap.Stringer("hostKey", hostKey)))
	}

	wg.Wait()

	log.Debug("pruning finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performContractPruningOnHost(ctx context.Context, host hosts.Host, hostLog *zap.Logger) error {
	// dial the host
	client, err := cm.dialer.DialHost(ctx, host.PublicKey, host.RHP4Addrs())
	if err != nil {
		return fmt.Errorf("failed to dial host: %w", err)
	}
	defer client.Close()

	// fetch contract ids
	contracts, err := cm.store.ContractsForPruning(ctx, host.PublicKey)
	if err != nil {
		return fmt.Errorf("failed to fetch contracts for pruning: %w", err)
	} else if len(contracts) == 0 {
		hostLog.Debug("no contracts for pruning")
		return nil
	}

	hostLog.Debug("pruning contracts on host", zap.Int("contracts", len(contracts)))
loop:
	for _, contract := range contracts {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		n, err := cm.pruneContract(ctx, client, host.Settings.Prices, contract)
		if err != nil {
			if updateErr := cm.store.UpdateNextPrune(ctx, contract, time.Now().Add(pruneIntervalFailure)); updateErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to update contract: %w", updateErr))
			}
			hostLog.Debug("failed to prune contract", zap.Error(err))
			continue
		} else if n > 0 {
			hostLog.Debug("pruned contract", zap.Stringer("contractID", contract), zap.Int("sectors", n), zap.Int("bytes", n*proto.SectorSize))
		}

		err = cm.store.UpdateNextPrune(ctx, contract, time.Now().Add(pruneIntervalSuccess))
		if err != nil {
			hostLog.Debug("failed to update contract", zap.Error(err))
		}
	}

	return nil
}

func (cm *ContractManager) pruneContract(ctx context.Context, client HostClient, hostPrices proto.HostPrices, contractID types.FileContractID) (int, error) {
	const (
		oneTB          = 1 << 40
		sectorsPerTB   = oneTB / proto.SectorSize
		rootsBatchSize = 10000
	)

	contract, renewed, err := cm.store.ContractRevision(ctx, contractID)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		cm.log.Debug("skipping pruning of renewed contract", zap.Stringer("contractID", contractID))
		return 0, nil
	}
	contractSectors := contract.Revision.Filesize / proto.SectorSize

	var pruned int
	for offset := uint64(0); offset < contractSectors; offset += sectorsPerTB {
		length := min(sectorsPerTB, contractSectors-offset)
		res, err := client.SectorRoots(ctx, hostPrices, contractID, offset, length)
		if err != nil {
			return pruned, fmt.Errorf("failed to fetch contract sectors: %w", err)
		} else if len(res.Roots) == 0 {
			continue
		}

		prunable := make(map[types.Hash256]struct{}, len(res.Roots))
		for start := 0; start < len(res.Roots); start += rootsBatchSize {
			end := min(start+rootsBatchSize, len(res.Roots))
			batch, err := cm.store.PrunableContractRoots(ctx, contractID, res.Roots[start:end])
			if err != nil {
				return pruned, fmt.Errorf("failed to fetch prunable contract roots: %w", err)
			}
			for _, root := range batch {
				prunable[root] = struct{}{}
			}
		}

		var indices []uint64
		for i, root := range res.Roots {
			if _, found := prunable[root]; found {
				indices = append(indices, offset+uint64(i))
			}
		}
		if len(indices) == 0 {
			continue
		}

		pruned += len(indices)
		_, err = client.FreeSectors(ctx, hostPrices, contractID, indices)
		if err != nil {
			return pruned, fmt.Errorf("failed to prune contract sectors: %w", err)
		}
	}

	return pruned, nil
}
