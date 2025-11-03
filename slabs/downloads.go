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
	lookup := make(map[types.PublicKey]hosts.Host, len(allHosts))
	for _, h := range allHosts {
		lookup[h.PublicKey] = h
	}

	hosts := make([]hosts.Host, 0, len(slab.Sectors))
	indices := make(map[types.PublicKey]int, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		if sector.HostKey == nil {
			continue
		}
		hk := *sector.HostKey
		h, ok := lookup[hk]
		if !ok {
			continue
		}
		indices[hk] = i
		hosts = append(hosts, h)
		delete(lookup, hk) // avoid duplicate candidates
	}

	// shuffle hosts to avoid always downloading from the same host first
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
func (m *SlabManager) downloadShards(ctx context.Context, slab Slab, allHosts []hosts.Host, pool *connPool, log *zap.Logger) ([][]byte, error) {
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

outer:
	for {
		select {
		case <-ctx.Done():
			// context cancelled, either due to timeout or enough shards downloaded
			break outer
		case sema <- struct{}{}:
		}

		host, ok := candidates.next()
		if !ok {
			// no more candidates, but there may be in-flight downloads
			break outer
		}

		wg.Add(1)
		go func(host hosts.Host, sectorIdx int) {
			defer func() {
				<-sema
				wg.Done()
			}()

			if ctx.Err() != nil {
				// context already cancelled
				return
			}

			sector := slab.Sectors[sectorIdx]
			log := log.With(zap.Stringer("hostKey", host.PublicKey), zap.Stringer("sectorRoot", sector.Root))
			start := time.Now()
			usage, data, err := m.downloadShard(ctx, host, sector, pool)
			if err != nil {
				lost := isErrLostSector(err)
				log.Debug("failed to download shard",
					zap.Bool("timeout", time.Since(start) > m.shardTimeout),
					zap.Bool("lost", lost),
					zap.Error(err),
				)
				if lost {
					if err := m.store.MarkSectorsLost(ctx, host.PublicKey, []types.Hash256{sector.Root}); err != nil {
						log.Error("failed to mark sector as lost", zap.Error(err))
					}
				}
				return
			}
			shards[sectorIdx] = data

			err = m.am.DebitServiceAccount(ctx, host.PublicKey, m.migrationAccount, usage.RenterCost())
			if err != nil {
				log.Debug("failed to debit service account for sector read", zap.Error(err))
			}

			if n := downloaded.Add(1); n >= uint32(slab.MinShards) {
				cancel()
			}
		}(host, candidates.indices[host.PublicKey])
	}

	wg.Wait()

	if downloaded.Load() < uint32(slab.MinShards) {
		return nil, fmt.Errorf("downloaded %d sectors, minimum required: %d: %w", downloaded.Load(), slab.MinShards, errNotEnoughShards)
	}
	return shards, nil
}

func (m *SlabManager) downloadShard(ctx context.Context, h hosts.Host, sector Sector, pool *connPool) (proto.Usage, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
	defer cancel()

	return pool.downloadShard(ctx, h, m.migrationToken(h), sector)
}

func isErrLostSector(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrSectorNotFound.Error())
}
