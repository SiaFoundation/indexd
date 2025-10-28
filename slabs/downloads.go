package slabs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var errNotEnoughShards = errors.New("not enough shards")

type downloadCandidates struct {
	hosts   []hosts.Host
	indices map[types.PublicKey]int
}

func newDownloadCandidates(allHosts []hosts.Host, slab Slab) downloadCandidates {
	// build host lookup
	lookup := make(map[types.PublicKey]struct{}, len(allHosts))
	for _, h := range allHosts {
		lookup[h.PublicKey] = struct{}{}
	}

	// build indices lookup
	indices := make(map[types.PublicKey]int, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		if sector.HostKey == nil {
			continue
		}
		hk := *sector.HostKey
		if _, ok := lookup[hk]; !ok {
			continue
		}
		indices[hk] = i
	}

	// build list of hosts, randomize order to avoid bias
	hosts := make([]hosts.Host, 0, len(lookup))
	for _, h := range allHosts {
		if _, ok := indices[h.PublicKey]; ok {
			hosts = append(hosts, h)
		}
	}
	frand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	return downloadCandidates{hosts: hosts, indices: indices}
}

func (dc *downloadCandidates) next() (hosts.Host, bool) {
	if len(dc.hosts) == 0 {
		return hosts.Host{}, false
	}

	host := dc.hosts[0]
	dc.hosts = dc.hosts[1:]
	return host, true
}

// downloadShards downloads at least the minimum number of shards required to
// recover the slab.
func (m *SlabManager) downloadShards(ctx context.Context, slab Slab, allHosts []hosts.Host, pool *connPool, logger *zap.Logger) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	shards := make([][]byte, len(slab.Sectors))
	var downloaded atomic.Uint32

	candidates := newDownloadCandidates(allHosts, slab)
	if uint(len(candidates.indices)) < slab.MinShards {
		return nil, fmt.Errorf("%w: only %d sectors available, minimum required: %d", errNotEnoughShards, len(candidates.indices), slab.MinShards)
	}

	var wg sync.WaitGroup
	sema := make(chan struct{}, slab.MinShards)
	var downloadErr error

outer:
	for {
		select {
		case <-ctx.Done():
			if n := downloaded.Load(); uint(n) < slab.MinShards {
				downloadErr = fmt.Errorf("downloaded %d out of %d shards: %w", downloaded.Load(), slab.MinShards, ctx.Err())
			}
			break outer
		case sema <- struct{}{}:
		}

		host, ok := candidates.next()
		if !ok {
			downloadErr = fmt.Errorf("downloaded %d out of %d shards: %w", downloaded.Load(), slab.MinShards, errNotEnoughHosts)
			break outer
		}

		wg.Add(1)
		go func(host hosts.Host, sectorIdx int, logger *zap.Logger) {
			var err error
			defer func() {
				if err != nil {
					<-sema // only release next candidate on failure
				}
				wg.Done()
			}()

			start := time.Now()
			var usage proto.Usage
			usage, shards[sectorIdx], err = m.downloadShard(ctx, host, slab.Sectors[sectorIdx], pool)
			if isErrLostSector(err) {
				m.markSectorLost(ctx, host, slab.Sectors[sectorIdx].Root, logger)
				return
			} else if err != nil {
				logger.Debug("failed to download shard",
					zap.Bool("timeout", time.Since(start) > m.shardTimeout),
					zap.Error(err),
				)
				return
			}

			err = m.am.DebitServiceAccount(ctx, host.PublicKey, m.migrationAccount, usage.RenterCost())
			if err != nil {
				logger.Debug("failed to debit service account for sector read", zap.Error(err))
			}

			if n := downloaded.Add(1); n >= uint32(slab.MinShards) {
				cancel()
			}
		}(host, candidates.indices[host.PublicKey], logger.With(zap.Stringer("hostKey", host.PublicKey)))
	}

	wg.Wait()
	if downloadErr != nil {
		return nil, downloadErr
	}
	return shards, nil
}

func (m *SlabManager) downloadShard(ctx context.Context, h hosts.Host, sector Sector, pool *connPool) (proto.Usage, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
	defer cancel()

	return pool.downloadShard(ctx, h, m.migrationToken(h), sector)
}

func (m *SlabManager) markSectorLost(ctx context.Context, host hosts.Host, root types.Hash256, log *zap.Logger) {
	err := m.store.MarkSectorsLost(ctx, host.PublicKey, []types.Hash256{root})
	if err != nil {
		log.Debug("failed to mark sector as lost", zap.Error(err))
		return
	}
	m.log.Debug("marked sector as lost")
}

func isErrLostSector(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrSectorNotFound.Error())
}
