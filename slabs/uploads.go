package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

var (
	errNotEnoughHosts = errors.New("not enough hosts")
	errRootMismatch   = errors.New("sector root of shard doesn't match expected root")
)

type (
	// Shard represents a sector present on a host.
	Shard struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}
)

// uploadShards uploads the shards to the given hosts. If not all shards were
// migrated, an error is returned but any finished shards will still be returned
// and should be tracked in the database. The given shards must not be nil and
// the given hosts must all be good and be sufficiently spaced apart. Note that
// the shards returned are not necessarily in the same order as the input
// shards.
func (m *SlabManager) uploadShards(ctx context.Context, slab Slab, shards [][]byte, candidates []hosts.Host, pool *connPool, logger *zap.Logger) ([]Shard, error) {
	if len(slab.Sectors) != len(shards) {
		panic(fmt.Sprintf("slab %s has %d sectors but %d shards", slab.ID, len(slab.Sectors), len(shards))) // developer error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var uploadMu sync.Mutex
	var uploadErr error
	var uploaded []Shard

	var wg sync.WaitGroup
	sema := make(chan struct{}, 10)
	defer close(sema)

loop:
	for i, shard := range shards {
		if shard == nil {
			continue
		}

		select {
		case <-ctx.Done():
			break loop
		case sema <- struct{}{}:
		}

		wg.Add(1)
		go func(ctx context.Context, shard []byte, shardIndex int, shardRoot types.Hash256) {
			defer func() {
				<-sema
				wg.Done()
			}()

			for ctx.Err() == nil {
				// grab next candidate
				uploadMu.Lock()
				if len(candidates) == 0 {
					uploadErr = errNotEnoughHosts
					uploadMu.Unlock()
					cancel()
					return
				}
				host := candidates[0]
				candidates = candidates[1:]
				uploadMu.Unlock()

				// upload the shard
				start := time.Now()
				usage, root, err := m.uploadShard(ctx, host, bytes.NewReader(shard), pool)
				if err != nil {
					logger.Debug("failed to upload shard",
						zap.Bool("timeout", time.Since(start) > m.shardTimeout),
						zap.Stringer("hostKey", host.PublicKey),
						zap.Error(err),
					)
					continue
				} else if root != shardRoot {
					uploadMu.Lock()
					uploadErr = fmt.Errorf("failed to upload shard %d: %w", shardIndex, errRootMismatch)
					uploadMu.Unlock()
					cancel()
					return
				}

				// debit service account
				err = m.am.DebitServiceAccount(context.Background(), host.PublicKey, m.migrationAccount, usage.RenterCost())
				if err != nil {
					logger.Debug("failed to debit service account for sector write", zap.Error(err))
				}

				// set shard
				uploadMu.Lock()
				uploaded = append(uploaded, Shard{
					Root:    root,
					HostKey: host.PublicKey,
				})
				uploadMu.Unlock()

				break
			}
		}(ctx, shard, i, slab.Sectors[i].Root)
	}

	wg.Wait()

	if uploadErr == nil {
		uploadErr = ctx.Err()
	}
	if uploadErr != nil {
		uploadErr = fmt.Errorf("uploaded %d out of %d shards: %w", len(uploaded), len(shards), uploadErr)
	}

	return uploaded, uploadErr
}

func (m *SlabManager) uploadShard(ctx context.Context, h hosts.Host, shard io.Reader, pool *connPool) (proto.Usage, types.Hash256, error) {
	ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
	defer cancel()

	return pool.uploadShard(ctx, h, m.migrationToken(h), shard)
}

func (m *SlabManager) migrationToken(h hosts.Host) proto.AccountToken {
	return proto.NewAccountToken(m.migrationAccountKey, h.PublicKey)
}
