package contracts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

// PinSectors pins a set of sectors on a host using the given set of contracts
// The contracts are tried in order, the contract ID that ends up being used is
// returned, alongside with a list of missing sectors if any.
func (c *contractor) PinSectors(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, sectors []types.Hash256, log *zap.Logger) (types.FileContractID, []types.Hash256, error) {
	// sanity check
	if len(sectors) > proto.MaxSectorBatchSize {
		return types.FileContractID{}, nil, errors.New("too many sectors") // developer error
	}

	// dial host
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	t, err := siamux.Dial(dialCtx, host.SiamuxAddr(), host.PublicKey)
	cancel()
	if err != nil {
		return types.FileContractID{}, nil, fmt.Errorf("failed to dial host: %w", err)
	}
	defer t.Close()

	// fetch settings
	settings, err := rhp.RPCSettings(ctx, t)
	if err != nil {
		return types.FileContractID{}, nil, fmt.Errorf("failed to fetch host settings: %w", err)
	}

	// iterate over contracts
	var usedContractID types.FileContractID
	var missing []types.Hash256
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		// fetch revision and check if it meets the requirements
		rev, err := rhp.RPCLatestRevision(ctx, t, contractID)
		if err != nil {
			contractLog.Debug("failed to fetch latest revision", zap.Error(err))
			continue
		} else if !rev.Revisable {
			contractLog.Debug("contract is not revisable") // sanity check
			continue
		} else if rev.Contract.RenterOutput.Value.IsZero() {
			contractLog.Debug("contract is out of funds")
			continue
		} else if rev.Contract.Filesize > maxContractSize {
			contractLog.Debug("contract is too large")
			continue
		}

		// append sectors
		revision := rhp.ContractRevision{ID: contractID, Revision: rev.Contract}
		res, err := rhp.RPCAppendSectors(ctx, t, c.cm.TipState(), settings.Prices, c.sk, revision, sectors)
		if err != nil {
			contractLog.Debug("failed to pin sectors", zap.Error(err))
			continue
		} else if len(res.Sectors) == 0 {
			contractLog.Debug("no sectors were pinned")
			continue
		}

		// TODO: handle usage

		// reaching this point means sectors were appended to the contract
		// successfully, if all sectors were pinned we can return early, but
		// otherwise we have to figure out which sectors were missing
		usedContractID = contractID
		if len(res.Sectors) == len(sectors) {
			break
		}

		// figure out which sectors were missing
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

	if usedContractID == (types.FileContractID{}) {
		return types.FileContractID{}, nil, errors.New("no usable contract found")
	}
	return usedContractID, missing, nil
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
		sectorsBatchSize = (1 << 40) / proto.SectorSize
	)

	var sectorsPinned, sectorsMissing uint64

	for offset := 0; ctx.Err() == nil; offset += batchSize {
		// fetch hosts
		batch, err := cm.store.Hosts(ctx, offset, batchSize, opts...)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts for pinning: %w", err)
		}

		// pin sectors on each host in parallel
		var wg sync.WaitGroup
		for _, h := range batch {
			wg.Add(1)
			go func(ctx context.Context, host hosts.Host, log *zap.Logger) {
				defer wg.Done()

				contractIDs, err := cm.store.ContractsForPinning(ctx, h.PublicKey, maxContractSize)
				if err != nil {
					log.Debug("failed to fetch contracts for pinning", zap.Error(err))
					return
				}

				var exhausted bool
				for !exhausted && ctx.Err() == nil {
					roots, err := cm.store.UnpinnedSectors(ctx, h.PublicKey, sectorsBatchSize)
					if err != nil {
						log.Debug("failed to fetch unpinned sectors", zap.Error(err))
						return
					} else if len(roots) < sectorsBatchSize {
						exhausted = true
					}

					contractID, missing, err := cm.contractor.PinSectors(ctx, host, contractIDs, roots, log)
					if err != nil {
						log.Debug("failed to pin sectors", zap.Error(err))
						return
					}

					if len(missing) > 0 {
						filtered := roots[:0]
						for _, root := range roots {
							if !slices.Contains(missing, root) {
								filtered = append(filtered, root)
							}
						}
						roots = slices.Clone(filtered)

						const batchSize = 1000
						for i := 0; i < len(missing); i += batchSize {
							end := min(i+batchSize, len(missing))
							err = cm.store.MarkSectorsLost(ctx, h.PublicKey, missing[i:end])
							if err != nil {
								log.Debug("failed to mark sectors as lost", zap.Error(err))
								return
							}
						}
						atomic.AddUint64(&sectorsMissing, uint64(len(missing)))
					}

					if len(roots) > 0 {
						err = cm.store.PinSectors(ctx, contractID, roots)
						if err != nil {
							log.Debug("failed to pin sectors", zap.Error(err))
							return
						}
						atomic.AddUint64(&sectorsPinned, uint64(len(roots)))
					}
				}
			}(ctx, h, log.With(zap.Stringer("hostKey", h.PublicKey)))
		}
		wg.Wait()

		// break if hosts are exhausted
		if len(batch) < batchSize {
			break
		}
	}

	log.Debug("pinning finished", zap.Duration("duration", time.Since(start)), zap.Uint64("sectorsMissing", sectorsMissing), zap.Uint64("bytesPinned", sectorsPinned*proto.SectorSize))
	return ctx.Err()
}
