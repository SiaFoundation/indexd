package contracts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// SectorPinner defines an interface to pin sectors to a set of contracts.
	// It returns the ID of the contract that ended up being used, as well as a
	// list of roots that were reported as missing by the host.
	SectorPinner interface {
		PinSectors(ctx context.Context, contractIDs []types.FileContractID, sectors []types.Hash256, log *zap.Logger) (types.FileContractID, []types.Hash256, error)
	}

	sectorPinner struct {
		host   HostClient
		prices proto.HostPrices
	}
)

// PinSectors pins a set of sectors using the given set of contracts The
// contracts are tried in order, the contract ID that ends up being used is
// returned, alongside with a list of missing sectors if any.
func (p *sectorPinner) PinSectors(ctx context.Context, contractIDs []types.FileContractID, sectors []types.Hash256, log *zap.Logger) (usedContractID types.FileContractID, missing []types.Hash256, _ error) {
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		// try to pin sectors to the contract
		res, err := p.host.AppendSectors(ctx, p.prices, contractID, sectors)
		if err != nil {
			contractLog.Debug("failed to pin sectors", zap.Error(err))
			continue
		} else if len(res.Sectors) == 0 {
			contractLog.Debug("no sectors were pinned")
			continue
		}

		// figure out which sectors were missing if necessary
		if len(res.Sectors) != len(sectors) {
			lookup := make(map[types.Hash256]struct{}, len(sectors))
			for _, sector := range sectors {
				lookup[sector] = struct{}{}
			}
			for _, sector := range res.Sectors {
				delete(lookup, sector)
			}
			for sector := range lookup {
				missing = append(missing, sector)
			}

			contractLog.Debug("some sectors were not pinned", zap.Int("pinned", len(res.Sectors)), zap.Int("missing", len(missing)))
		}

		// TODO: handle usage

		usedContractID = contractID
		return
	}
	return types.FileContractID{}, nil, errors.New("no usable contract found")
}

func (c *hostClient) AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	// sanity check
	if len(sectors) > proto.MaxSectorBatchSize {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("too many sectors, %d > %d", len(sectors), proto.MaxSectorBatchSize) // developer error
	}

	// fetch revision and check if it meets the requirements
	rev, err := rhp.RPCLatestRevision(ctx, c.client, contractID)
	if err != nil {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !rev.Revisable {
		return rhp.RPCAppendSectorsResult{}, errors.New("contract is not revisable")
	} else if rev.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCAppendSectorsResult{}, errors.New("contract is out of funds")
	} else if rev.Contract.Filesize > maxContractSize {
		return rhp.RPCAppendSectorsResult{}, fmt.Errorf("contract is too large, %d > %d", rev.Contract.Filesize, maxContractSize)
	}

	// append sectors
	revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
	return rhp.RPCAppendSectors(ctx, c.client, c.cm.TipState(), hostPrices, c.ownKey, revision, sectors)
}

func (cm *ContractManager) performSectorPinning(ctx context.Context, log *zap.Logger) error {
	start := time.Now()
	log = log.Named("sectorpinning")

	// pin sectors on usable hosts with active contracts
	opts := []hosts.HostQueryOpt{
		hosts.WithUsable(true),
		hosts.WithBlocked(false),
		hosts.WithActiveContracts(true),
	}

	const (
		batchSize        = 50
		sectorsBatchSize = (1 << 40) / proto.SectorSize // 1TB of sectors
	)

	var wg sync.WaitGroup
	sema := make(chan struct{}, 50)
	defer close(sema)

	for offset := 0; ctx.Err() == nil; offset += batchSize {
		// fetch hosts
		batch, err := cm.store.Hosts(ctx, offset, batchSize, opts...)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts for pinning: %w", err)
		}

		// pin sectors on each host in parallel
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

				client, err := cm.dialer.Dial(ctx, host.PublicKey, host.SiamuxAddr())
				if err != nil {
					hostLog.Debug("failed to create contractor", zap.Error(err))
					return
				}
				defer client.Close()

				err = cm.performSectorPinningOnHost(ctx, &sectorPinner{host: client, prices: host.Settings.Prices}, host, hostLog)
				if err != nil {
					hostLog.Debug("failed to pin sectors", zap.Error(err))
				}
			}(ctx, h, log.With(zap.Stringer("hostKey", h.PublicKey)))
		}

		// break if hosts are exhausted
		if len(batch) < batchSize {
			break
		}
	}

	wg.Wait()

	log.Debug("pinning finished", zap.Duration("duration", time.Since(start)))
	return ctx.Err()
}

func (cm *ContractManager) performSectorPinningOnHost(ctx context.Context, pinner SectorPinner, host hosts.Host, hostLog *zap.Logger) error {
	contractIDs, err := cm.store.ContractsForPinning(ctx, host.PublicKey, maxContractSize)
	if err != nil {
		return fmt.Errorf("failed to fetch contracts for pinning: %w", err)
	} else if len(contractIDs) == 0 {
		return errors.New("no contracts for pinning")
	}

	var nPinned, nMissing uint64
	defer func() {
		if nPinned+nMissing > 0 {
			hostLog.Debug(
				"pinned sectors",
				zap.Uint64("bytesPinned", nPinned*proto.SectorSize),
				zap.Uint64("sectorsMissing", nMissing),
			)
		}
	}()

	const (
		dbBatchSize      = 1000
		sectorsBatchSize = (1 << 40) / proto.SectorSize // 1TB of sectors
	)

	var exhausted bool
	for !exhausted && ctx.Err() == nil {
		roots, err := cm.store.UnpinnedSectors(ctx, host.PublicKey, sectorsBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch unpinned sectors: %w", err)
		} else if len(roots) < sectorsBatchSize {
			exhausted = true
		}

		contractID, missing, err := pinner.PinSectors(ctx, contractIDs, roots, hostLog)
		if err != nil {
			return fmt.Errorf("failed to pin sectors: %w", err)
		}

		if len(missing) > 0 {
			lookup := make(map[types.Hash256]struct{}, len(missing))
			for _, sector := range missing {
				lookup[sector] = struct{}{}
			}

			filtered := roots[:0]
			for _, root := range roots {
				if _, missing := lookup[root]; !missing {
					filtered = append(filtered, root)
				}
			}
			roots = slices.Clone(filtered)

			for i := 0; i < len(missing); i += dbBatchSize {
				end := min(i+dbBatchSize, len(missing))
				if err := cm.store.MarkSectorsLost(ctx, host.PublicKey, missing[i:end]); err != nil {
					return fmt.Errorf("failed to mark sectors as lost: %w", err)
				}
			}
			nMissing += uint64(len(missing))
		}

		if len(roots) > 0 {
			err = cm.store.PinSectors(ctx, contractID, roots)
			if err != nil {
				return fmt.Errorf("failed to pin sectors: %w", err)
			}
			nPinned += uint64(len(roots))
		}
	}

	return ctx.Err()
}
