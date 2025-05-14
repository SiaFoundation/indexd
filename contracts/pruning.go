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
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// ContractPruner defines an interface to prune a contract with given idgit .
	ContractPruner interface {
		PruneContract(ctx context.Context, contractID types.FileContractID) error
	}

	contractPruner struct {
		contractor Contractor
		store      Store
		tc         rhp.TransportClient
		hp         proto.HostPrices
		hk         types.PublicKey
	}
)

func newContractPruner(ctx context.Context, contractor Contractor, hostAddr string, hostKey types.PublicKey, hostPrices proto.HostPrices) (*contractPruner, error) {
	tc, err := siamux.Dial(ctx, hostAddr, hostKey)
	if err != nil {
		return nil, err
	}
	return &contractPruner{
		contractor: contractor,
		tc:         tc,
		hp:         hostPrices,
		hk:         hostKey,
	}, nil
}

func (cp *contractPruner) PruneContract(ctx context.Context, contractID types.FileContractID) error {
	const (
		oneTB        = 1 << 40
		sectorsPerTB = oneTB / proto.SectorSize
	)

	contract, err := cp.store.ContractElement(ctx, contractID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract: %w", err)
	}

	for offset := uint64(0); offset < contract.V2FileContract.Filesize; offset += sectorsPerTB {
		res, err := cp.contractor.ContractSectors(ctx, cp.tc, cp.hp, contractID, uint64(offset), sectorsPerTB)
		if err != nil {
			return fmt.Errorf("failed to fetch contract sectors: %w", err)
		} else if len(res.Roots) == 0 {
			continue
		}

		// TODO: handle usage

		indices, err := cp.store.PrunableContractRoots(ctx, contract.V2FileContract.HostPublicKey, contractID, res.Roots)
		if err != nil {
			return fmt.Errorf("failed to fetch prunable contract roots: %w", err)
		} else if len(indices) == 0 {
			continue
		}
		for i := range indices {
			indices[i] += offset
		}

		_, err = cp.contractor.PruneSectors(ctx, cp.tc, cp.hp, contractID, indices)
		if err != nil {
			return fmt.Errorf("failed to prune contract sectors: %w", err)
		}

		// TODO: handle usage
	}

	return nil
}

func (cp *contractPruner) Close() error {
	return cp.tc.Close()
}

func (c *contractor) ContractSectors(ctx context.Context, tc rhp.TransportClient, hostPrices proto.HostPrices, contractID types.FileContractID, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	// fetch revision and check if it meets the requirements
	rev, err := rhp.RPCLatestRevision(ctx, tc, contractID)
	if err != nil {
		return rhp.RPCSectorRootsResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !rev.Revisable {
		return rhp.RPCSectorRootsResult{}, errors.New("contract is not revisable")
	} else if rev.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCSectorRootsResult{}, errors.New("contract is out of funds")
	}

	// fetch contract sectors
	revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
	return rhp.RPCSectorRoots(ctx, tc, c.cm.TipState(), hostPrices, c.signer, revision, offset, length)
}

func (c *contractor) PruneSectors(ctx context.Context, tc rhp.TransportClient, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	// fetch revision and check if it meets the requirements
	rev, err := rhp.RPCLatestRevision(ctx, tc, contractID)
	if err != nil {
		return rhp.RPCFreeSectorsResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !rev.Revisable {
		return rhp.RPCFreeSectorsResult{}, errors.New("contract is not revisable")
	} else if rev.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCFreeSectorsResult{}, errors.New("contract is out of funds")
	}

	// free sectors
	revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
	return rhp.RPCFreeSectors(ctx, tc, c.cm.TipState(), hostPrices, c.sk, revision, indices)
}

func (cm *ContractManager) performContractPruning(ctx context.Context, log *zap.Logger) error {
	start := time.Now()
	log = log.Named("contractpruning")

	// prune sectors on usable hosts with active contracts
	opts := []hosts.HostQueryOpt{
		hosts.WithUsable(true),
		hosts.WithBlocked(false),
		hosts.WithActiveContracts(true),
	}

	const (
		batchSize        = 50
		sectorsBatchSize = (1 << 40) / proto.SectorSize
	)

	var wg sync.WaitGroup
	sema := make(chan struct{}, 50)
	defer close(sema)

	for offset := 0; ctx.Err() == nil; offset += batchSize {
		// fetch hosts
		batch, err := cm.store.Hosts(ctx, offset, batchSize, opts...)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts for pruning: %w", err)
		}

		// prune contracts in
		for _, h := range batch {
			select {
			case <-ctx.Done():
				break
			case sema <- struct{}{}:
			}

			wg.Add(1)
			go func(ctx context.Context, host hosts.Host, hostLog *zap.Logger) {
				defer func() {
					<-sema
					wg.Done()
				}()

				pruner, err := newContractPruner(ctx, cm.contractor, host.SiamuxAddr(), host.PublicKey, host.Settings.Prices)
				if err != nil {
					hostLog.Debug("failed to create sector pinner", zap.Error(err))
					return
				}
				defer pruner.Close()

				err = cm.performContractPruningOnHost(ctx, pruner, host, hostLog)
				if err != nil {
					hostLog.Debug("failed to prune contracts", zap.Error(err))
					return
				}
			}(ctx, h, log.With(zap.Stringer("hostKey", h.PublicKey)))
		}

		// break if hosts are exhausted
		if len(batch) < batchSize {
			break
		}
	}

	wg.Wait()

	log.Debug("pruning finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performContractPruningOnHost(ctx context.Context, pruner ContractPruner, host hosts.Host, log *zap.Logger) error {
	contracts, err := cm.store.ContractsForPruning(ctx, host.PublicKey, time.Now().Add(-time.Hour*24))
	if err != nil {
		return fmt.Errorf("failed to fetch contracts for pruning: %w", err)
	}

	// prune sectors
	for _, contract := range contracts {
		select {
		case <-ctx.Done():
			break
		default:
		}

		err = pruner.PruneContract(ctx, contract)
		if err != nil {
			log.Debug("failed to prune contract", zap.Error(err))
			continue
		}

		err = cm.store.MarkPruned(ctx, contract)
		if err != nil {
			log.Debug("failed to mark contract as pruned", zap.Error(err))
		}
	}

	return nil
}
