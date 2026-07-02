package slabs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// Shard represents a sector present on a host.
type Shard struct {
	Root    types.Hash256   `json:"root"`
	HostKey types.PublicKey `json:"hostKey"`
}

// uploadShards uploads the shards to the given hosts. If any shards were migrated,
// no error is returned. The given shards must not be nil and
// the given hosts must all be good and be sufficiently spaced apart. It returns
// the (root, host) pairs that were successfully migrated; the caller is
// responsible for persisting them.
func (m *Migrator) uploadShards(ctx context.Context, slab Slab, shards [][]byte, available []types.PublicKey, log *zap.Logger) ([]Shard, error) {
	if len(slab.Sectors) != len(shards) {
		panic(fmt.Sprintf("slab %s has %d sectors but %d shards", slab.ID, len(slab.Sectors), len(shards))) // developer error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// defensive programming to ensure shards are
	// never uploaded to the same host twice.
	var used sync.Map
	for _, sector := range slab.Sectors {
		if sector.HostKey != nil {
			used.Store(*sector.HostKey, struct{}{})
		}
	}

	// Prioritize filters unusable hosts; PickWrite re-scores on each
	// call so concurrent shard goroutines see each other's inflight
	// reservations and disperse across less-busy hosts. poolMu
	// serializes access to the shared `available` slice header.
	available = m.hosts.Prioritize(available)
	var poolMu sync.Mutex

	pickHost := func() (types.PublicKey, func(), bool) {
		poolMu.Lock()
		defer poolMu.Unlock()
		host, release, remaining, ok := m.hosts.PickWrite(available)
		available = remaining
		return host, release, ok
	}

	var wg sync.WaitGroup
	sema := make(chan struct{}, 10)
	var migratedMu sync.Mutex
	migrated := make([]Shard, 0, len(shards))
top:
	for i := range shards {
		if shards[i] == nil {
			continue
		}

		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			shard := shards[i]
			shardRoot := slab.Sectors[i].Root
			log := log.With(zap.Stringer("sectorRoot", shardRoot))
			wg.Go(func() {
				defer func() {
					<-sema
				}()
				for {
					if ctx.Err() != nil {
						return
					}
					hostKey, release, ok := pickHost()
					if !ok {
						log.Debug("no more hosts for migration")
						return
					}

					log := log.With(zap.Stringer("hostKey", hostKey))
					// upload the shard. The candidates were vetted when the
					// migration was prepared, so there is no per-upload
					// usability re-check; a host that went bad since simply
					// fails the RPC and the shard is retried on the next
					// candidate.
					start := time.Now()
					timeoutCtx, timeoutCancel := context.WithTimeout(ctx, m.shardTimeout)
					result, err := m.hosts.WriteSector(timeoutCtx, m.migrationAccountKey, hostKey, shard)
					timedOut := timeoutCtx.Err() != nil
					timeoutCancel()
					release()
					if err != nil {
						log.Debug("failed to upload shard", zap.Duration("elapsed", time.Since(start)), zap.Error(err))
						// demote the host if it hit the per-shard timeout while
						// the overall migration is still in progress
						if timedOut && ctx.Err() == nil {
							log.Debug("demoting host for failed upload", zap.Error(err))
							m.hosts.AddFailedRPC(hostKey)
						}
						continue
					} else if result.Root != shardRoot {
						// note: since the RHP verifies that the root returned by the host
						// matches the data, this will only happen if the roots pinned
						// by the client were incorrect.
						//
						// since there is no way to verify, log and stop migration
						cancel()
						log.Error("shard root mismatch after upload, user data corrupt", zap.Stringer("expected", shardRoot), zap.Stringer("actual", result.Root))
						return
					} else if _, existed := used.Swap(hostKey, struct{}{}); existed {
						log.Panic("host already used for another shard", zap.Stringer("hostKey", hostKey)) // developer error
					}

					migratedMu.Lock()
					migrated = append(migrated, Shard{Root: shardRoot, HostKey: hostKey})
					migratedMu.Unlock()
					return
				}
			})
		}
	}
	wg.Wait() // wait for all inflight uploads to finish
	if len(migrated) == 0 {
		return nil, fmt.Errorf("no shards were uploaded during migration")
	}
	return migrated, nil
}
