package contracts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (cm *ContractManager) performContractPruning(ctx context.Context, log *zap.Logger) error {
	start := time.Now()

	// fetch hosts for pruning, a host is eligible for pruning if it is not
	// blocked and has active contracts that haven't been pruned in the last 24
	// hours
	hfp, err := cm.hosts.HostsForPruning(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch hosts for pruning: %w", err)
	} else if len(hfp) == 0 {
		log.Debug("no hosts for pruning")
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
			ctx, cancel := context.WithTimeoutCause(ctx, pruneTimeout, client.ErrAbortedRPC)
			defer func() {
				<-sema
				wg.Done()
				cancel()
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
	// fetch contract ids
	contracts, err := cm.store.ContractsForPruning(host.PublicKey)
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

		n, err := cm.pruneContract(ctx, contract)
		if err != nil {
			if updateErr := cm.store.UpdateNextPrune(contract, time.Now().Add(cm.pruneIntervalFailure)); updateErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to update contract: %w", updateErr))
			}
			hostLog.Debug("failed to prune contract", zap.Error(err))
			continue
		} else if n > 0 {
			hostLog.Debug("pruned contract", zap.Stringer("contractID", contract), zap.Int("sectors", n), zap.Int("bytes", n*proto.SectorSize))
		}

		err = cm.store.UpdateNextPrune(contract, time.Now().Add(cm.pruneIntervalSuccess))
		if err != nil {
			hostLog.Debug("failed to update contract", zap.Error(err))
		}
	}

	return nil
}

func (cm *ContractManager) pruneContract(ctx context.Context, contractID types.FileContractID) (int, error) {
	const dbRootsBatchSize = 10000

	// lock contract first before doing anything else to avoid anything else
	// modifying the contract while we loop over offsets and roots
	lc, unlock := cm.cl.LockContract(contractID)
	defer unlock()

	contract, renewed, err := cm.store.ContractRevision(contractID)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch contract revision: %w", err)
	} else if renewed {
		cm.log.Debug("skipping pruning of renewed contract", zap.Stringer("contractID", contractID))
		return 0, nil
	}
	contractSectors := contract.Revision.Filesize / proto.SectorSize

	var pruned int
	for offset := uint64(0); offset < contractSectors; offset += cm.sectorRootsBatchSize {
		length := min(cm.sectorRootsBatchSize, contractSectors-offset)

		var roots []types.Hash256
		err = cm.rev.WithRevision(ctx, lc, func(rev rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
			res, err := cm.client.SectorRoots(ctx, cm.signer, cm.chain, rev, offset, length)
			if err != nil {
				return rhp.ContractRevision{}, proto.Usage{}, err
			}
			roots = res.Roots
			rev.Revision = res.Revision
			return rev, res.Usage, nil
		})
		if err != nil {
			return pruned, fmt.Errorf("failed to fetch contract sectors: %w", err)
		} else if len(roots) == 0 {
			continue
		}

		prunable := make(map[types.Hash256]struct{}, len(roots))
		for start := 0; start < len(roots); start += dbRootsBatchSize {
			end := min(start+dbRootsBatchSize, len(roots))
			batch, err := cm.store.PrunableContractRoots(contractID, roots[start:end])
			if err != nil {
				return pruned, fmt.Errorf("failed to fetch prunable contract roots: %w", err)
			}
			for _, root := range batch {
				prunable[root] = struct{}{}
			}
		}

		var indices []uint64
		for i, root := range roots {
			if _, found := prunable[root]; found {
				indices = append(indices, offset+uint64(i))
			}
		}
		if len(indices) == 0 {
			continue
		}

		err = cm.rev.WithRevision(ctx, lc, func(rev rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
			res, err := cm.client.FreeSectors(ctx, cm.signer, cm.chain, rev, indices)
			if err != nil {
				return rhp.ContractRevision{}, proto.Usage{}, err
			}
			rev.Revision = res.Revision
			return rev, res.Usage, nil
		})
		if err != nil {
			return pruned, fmt.Errorf("failed to prune contract sectors: %w", err)
		}

		pruned += len(indices)
		contractSectors -= uint64(len(indices))
	}

	return pruned, nil
}
