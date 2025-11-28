package slabs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/client/v2"
	"go.uber.org/zap"
)

// Shard represents a sector present on a host.
type Shard struct {
	Root    types.Hash256
	HostKey types.PublicKey
}

// uploadShards uploads the shards to the given hosts. If any shards were migrated,
// no error is returned. The given shards must not be nil and
// the given hosts must all be good and be sufficiently spaced apart.
func (m *SlabManager) uploadShards(ctx context.Context, slab Slab, shards [][]byte, available []types.PublicKey, log *zap.Logger) (int, error) {
	if len(slab.Sectors) != len(shards) {
		panic(fmt.Sprintf("slab %s has %d sectors but %d shards", slab.ID, len(slab.Sectors), len(shards))) // developer error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// prioritize available candidates based on latest reliability and performance
	candidates := client.NewCandidates(m.hosts.Prioritize(available))

	var wg sync.WaitGroup
	sema := make(chan struct{}, 10)
	var migrated int32
top:
	for i := range shards {
		if shards[i] == nil {
			continue
		}

		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			if candidates.Available() == 0 {
				log.Debug("no more candidates for migration")
				break top
			}
			shard := shards[i]
			shardRoot := slab.Sectors[i].Root
			log := log.With(zap.Stringer("sectorRoot", shardRoot))
			wg.Go(func() {
				defer func() {
					<-sema
				}()
				for hostKey := range candidates.Iter() {
					if ctx.Err() != nil {
						// context already cancelled
						return
					}

					log := log.With(zap.Stringer("hostKey", hostKey))
					// upload the shard
					start := time.Now()
					result, err := m.uploadShard(ctx, hostKey, shard)
					if err != nil {
						log.Debug("failed to upload shard", zap.Duration("elapsed", time.Since(start)), zap.Error(err))
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
					}

					// debit service account
					err = m.am.DebitServiceAccount(context.Background(), hostKey, m.migrationAccount, result.Usage.RenterCost())
					if err != nil {
						log.Debug("failed to debit service account for sector write", zap.Error(err))
					}

					if _, err := m.store.MigrateSector(shardRoot, hostKey); err != nil {
						log.Error("failed to record migrated sector", zap.Error(err))
						return
					}
					atomic.AddInt32(&migrated, 1)
					return
				}
			})
		}
	}
	wg.Wait() // wait for all inflight uploads to finish
	if migrated == 0 {
		return 0, fmt.Errorf("no shards were uploaded during migration")
	}
	return int(migrated), nil
}

func (m *SlabManager) uploadShard(ctx context.Context, hostKey types.PublicKey, shard []byte) (rhp.RPCWriteSectorResult, error) {
	ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
	defer cancel()

	usable, err := m.hm.Usable(ctx, hostKey)
	if err != nil {
		return rhp.RPCWriteSectorResult{}, fmt.Errorf("failed to check if host is usable: %w", err)
	} else if !usable {
		return rhp.RPCWriteSectorResult{}, fmt.Errorf("host is no longer usable")
	}
	return m.hosts.WriteSector(ctx, m.migrationAccountKey, hostKey, shard)
}
