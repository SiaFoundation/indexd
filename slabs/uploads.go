package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

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
// the given hosts must all be good and be sufficiently spaced apart.
func (m *SlabManager) uploadShards(ctx context.Context, slab Slab, shards [][]byte, uploadCandidates []hosts.Host, logger *zap.Logger) ([]Shard, error) {
	uploaded := make([]Shard, 0, len(shards))

	if len(slab.Sectors) != len(shards) {
		panic(fmt.Sprintf("slab %s has %d sectors but %d shards", slab.ID, len(slab.Sectors), len(shards))) // developer error
	}

	var uploadErr error
	for i, shard := range shards {
		if shard == nil {
			continue
		}

	nextCandidate:
		if len(uploadCandidates) == 0 {
			uploadErr = fmt.Errorf("uploaded %d out of %d shards: %w", len(uploaded), len(shards), errNotEnoughHosts)
			break
		}
		host := uploadCandidates[0]
		uploadCandidates = uploadCandidates[1:]

		if ctx.Err() != nil {
			uploadErr = fmt.Errorf("uploaded %d out of %d shards: %w", len(uploaded), len(shards), ctx.Err())
			break
		}

		usage, root, err := m.uploadShard(ctx, host, bytes.NewReader(shard))
		if err != nil {
			logger.Debug("failed to upload shard", zap.Stringer("hostKey", host.PublicKey), zap.Error(err))
			goto nextCandidate
		}

		if root != slab.Sectors[i].Root {
			uploadErr = fmt.Errorf("uploaded %d out of %d shards: %w", len(uploaded), len(shards), errRootMismatch)
			break
		}

		err = m.am.DebitServiceAccount(ctx, host.PublicKey, m.migrationAccount, usage.RenterCost())
		if err != nil {
			logger.Debug("failed to debit service account for sector write", zap.Error(err))
		}

		uploaded = append(uploaded, Shard{
			Root:    root,
			HostKey: host.PublicKey,
		})
	}

	return uploaded, uploadErr
}

func (m *SlabManager) uploadShard(ctx context.Context, h hosts.Host, shard io.Reader) (proto.Usage, types.Hash256, error) {
	ctx, cancel := context.WithTimeout(ctx, m.shardTimeout)
	defer cancel()

	client, err := m.dialer.DialHost(ctx, h.PublicKey, h.SiamuxAddr())
	if err != nil {
		return proto.Usage{}, types.Hash256{}, fmt.Errorf("failed to dial host: %w", err)
	}

	settings, err := client.Settings(ctx)
	if err != nil {
		return proto.Usage{}, types.Hash256{}, fmt.Errorf("failed to fetch host settings: %w", err)
	}

	result, err := client.WriteSector(ctx, settings.Prices, m.migrationToken(h), shard, proto.SectorSize)
	if err != nil {
		return proto.Usage{}, types.Hash256{}, fmt.Errorf("failed to write sector: %w", err)
	}

	return result.Usage, result.Root, nil
}

func (m *SlabManager) migrationToken(h hosts.Host) proto.AccountToken {
	return proto.NewAccountToken(m.migrationAccountKey, h.PublicKey)
}
