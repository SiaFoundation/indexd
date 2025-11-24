package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	shards := make([][]byte, len(slab.Sectors))
	var downloaded atomic.Uint32

	slabHosts := make(map[types.PublicKey]slabDownload)
	candidates := make([]types.PublicKey, 0, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		if sector.HostKey != nil {
			candidates = append(candidates, *sector.HostKey)
			slabHosts[*sector.HostKey] = slabDownload{
				root:  sector.Root,
				index: i,
			}
		}
	}
	candidates = m.hosts.Prioritize(candidates)

	var wg sync.WaitGroup
	sema := make(chan struct{}, slab.MinShards)

outer:
	for _, hostKey := range candidates {
		select {
		case <-ctx.Done():
			// context cancelled, either due to timeout or enough shards downloaded
			break outer
		case sema <- struct{}{}:
		}

		sector := slabHosts[hostKey]
		log := log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("sectorRoot", sector.root))

		wg.Add(1)
		go func() {
			defer wg.Done()

			if ctx.Err() != nil {
				// context already cancelled
				return
			}

			prices, err := m.hosts.Prices(ctx, hostKey)
			if err != nil {
				log.Debug("failed to fetch host prices", zap.Error(err))
				// only release the semaphore if the download failed so that another shard can be started
				<-sema
				return
			}

			// debit the service account for the read since the host may charge for it
			// even if it is cancelled quickly. This is best effort, it's fine to
			// log the error and continue on failure.
			cost := prices.RPCReadSectorCost(proto.SectorSize).RenterCost()
			if err = m.am.DebitServiceAccount(context.Background(), hostKey, m.migrationAccount, cost); err != nil {
				log.Debug("failed to debit service account for sector read", zap.Error(err))
			}

			start := time.Now()
			buf := bytes.NewBuffer(make([]byte, 0, proto.SectorSize))
			if _, err := m.downloadShard(ctx, hostKey, buf, sector.root); err != nil {
				if isErrLostSector(err) {
					log.Debug("host reports sector lost", zap.Duration("elapsed", time.Since(start)))
					if err := m.store.MarkSectorsLost(hostKey, []types.Hash256{sector.root}); err != nil {
						log.Error("failed to mark sector as lost", zap.Error(err))
					}
				} else if !errors.Is(err, mux.ErrClosedStream) && !errors.Is(err, ctx.Err()) {
					log.Debug("failed to download shard", zap.Duration("elapsed", time.Since(start)), zap.Error(err))
				}
				// only release the semaphore if the download failed so that another shard can be started
				<-sema
				return
			}

			shards[sector.index] = buf.Bytes()
			if n := downloaded.Add(1); n >= uint32(slab.MinShards) {
				cancel()
			}
		}()
	}

	wg.Wait()

	if downloaded.Load() < uint32(slab.MinShards) {
		return nil, fmt.Errorf("downloaded %d sectors, minimum required: %d: %w", downloaded.Load(), slab.MinShards, errNotEnoughShards)
	}
	return shards, nil
}

func (m *SlabManager) downloadShard(ctx context.Context, hostKey types.PublicKey, w io.Writer, root types.Hash256) (rhp.RPCReadSectorResult, error) {
	ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
	defer cancel()

	return m.hosts.ReadSector(ctx, m.migrationAccountKey, hostKey, root, w, 0, proto.SectorSize)
}

func isErrLostSector(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrSectorNotFound.Error())
}
