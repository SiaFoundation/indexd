package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
)

var errNotEnoughShards = errors.New("not enough shards")

type slabDownload struct {
	root  types.Hash256
	index int
}

// downloadShards downloads at least the minimum number of shards required to
// recover the slab.
func (m *SlabManager) downloadShards(ctx context.Context, slab Slab, log *zap.Logger) ([][]byte, error) {
	initialCtx, initialCancel := context.WithCancel(ctx)
	overdriveCtx, overdriveCancel := context.WithCancelCause(ctx)
	cancel := func() {
		initialCancel()
		overdriveCancel(client.ErrAbortedRPC)
	}
	defer cancel()

	shards := make([][]byte, len(slab.Sectors))
	var downloaded atomic.Uint32

	slabHosts := make(map[types.PublicKey]slabDownload)
	candidates := make([]types.PublicKey, 0, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		if sector.HostKey == nil {
			continue
		} else if _, exists := slabHosts[*sector.HostKey]; exists {
			continue // prevent duplicates
		}
		candidates = append(candidates, *sector.HostKey)
		slabHosts[*sector.HostKey] = slabDownload{
			root:  sector.Root,
			index: i,
		}
	}
	candidates = m.hosts.Prioritize(candidates)

	var wg sync.WaitGroup
	sema := make(chan struct{}, slab.MinShards)

	// helper to download a shard from a host
	downloadShard := func(ctx context.Context, hostKey types.PublicKey, sector slabDownload, log *zap.Logger) error {
		defer func() {
			<-sema
		}()
		ctx, timeoutCancel := context.WithTimeout(ctx, m.shardTimeout)
		defer timeoutCancel()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		prices, err := m.hosts.Prices(ctx, hostKey)
		if err != nil {
			return fmt.Errorf("failed to fetch host prices: %w", err)
		}

		// debit the service account for the read since the host may charge for it
		// even if it is cancelled quickly. This is best effort, it's fine to
		// log the error and continue on failure.
		cost := prices.RPCReadSectorCost(proto.SectorSize).RenterCost()
		if err = m.am.DebitServiceAccount(context.Background(), hostKey, m.migrationAccount, cost); err != nil {
			log.Warn("failed to debit service account for read sector", zap.Error(err))
		}

		start := time.Now()
		buf := bytes.NewBuffer(make([]byte, 0, proto.SectorSize))
		if _, err := m.hosts.ReadSector(ctx, m.migrationAccountKey, hostKey, sector.root, buf, 0, proto.SectorSize); err != nil {
			if isErrLostSector(err) {
				log.Debug("host reports sector lost", zap.Duration("elapsed", time.Since(start)))
				if err := m.store.MarkSectorsLost(hostKey, []types.Hash256{sector.root}); err != nil {
					log.Error("failed to mark sector as lost", zap.Error(err))
				}
			} else if !errors.Is(err, mux.ErrClosedStream) && !errors.Is(err, ctx.Err()) {
				log.Debug("failed to download shard", zap.Duration("elapsed", time.Since(start)), zap.Error(err))
			}
			return err
		}

		shards[sector.index] = buf.Bytes()
		if n := downloaded.Add(1); n >= uint32(slab.MinShards) {
			cancel()
		}
		return nil
	}

	if len(candidates) < int(slab.MinShards) {
		return nil, fmt.Errorf("only %d available sectors, minimum required: %d: %w", len(candidates), slab.MinShards, errNotEnoughShards)
	}

	// start initial shards
	failedCh := make(chan struct{}, slab.MinShards)
initialLoop:
	for _, hostKey := range candidates[:int(slab.MinShards)] {
		select {
		case <-ctx.Done():
			break initialLoop
		case sema <- struct{}{}:
		}

		sector := slabHosts[hostKey]
		log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("sectorRoot", sector.root))
		wg.Go(func() {
			if err := downloadShard(initialCtx, hostKey, slabHosts[hostKey], log); err != nil {
				log.Debug("shard download failed", zap.Error(err))
				// non-blocking send to indicate a failure
				select {
				case failedCh <- struct{}{}:
				default:
				}
			}
		})
	}

	t := time.NewTicker(m.shardTimeout / 4)
	defer t.Stop()
raceLoop:
	for i := int(slab.MinShards); downloaded.Load() < uint32(slab.MinShards) && i < len(candidates); i++ {
		hostKey := candidates[i]
		select {
		case <-initialCtx.Done():
			break raceLoop
		case <-failedCh:
			// a download has failed
		case <-t.C:
			// hedge against slow shards
			log.Debug("racing slow shards", zap.Uint32("downloaded", downloaded.Load()), zap.Uint32("required", uint32(slab.MinShards)))
		}

		// wait for an available slot
		select {
		case sema <- struct{}{}:
			sector := slabHosts[hostKey]
			log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("sectorRoot", sector.root))
			wg.Go(func() {
				if err := downloadShard(overdriveCtx, hostKey, slabHosts[hostKey], log); err != nil {
					log.Debug("shard download failed", zap.Error(err))
					// non-blocking send to indicate a failure
					select {
					case failedCh <- struct{}{}:
					default:
					}
				}
			})
		case <-initialCtx.Done():
			break raceLoop
		}
	}

	wg.Wait()

	if downloaded.Load() < uint32(slab.MinShards) {
		return nil, fmt.Errorf("downloaded %d sectors, minimum required: %d: %w", downloaded.Load(), slab.MinShards, errNotEnoughShards)
	}
	return shards, nil
}

func isErrLostSector(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrSectorNotFound.Error())
}

func isErrNotEnoughFunds(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrNotEnoughFunds.Error())
}
