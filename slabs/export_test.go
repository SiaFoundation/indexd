package slabs

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const MaxTotalShards = maxTotalShards

var (
	ErrNotEnoughShards                   = errNotEnoughShards
	ErrInsufficientServiceAccountBalance = errInsufficientServiceAccountBalance
)

var (
	SectorsToMigrate    = sectorsToMigrate
	NewSlabManager      = newSlabManager
	NewLostSectorsAlert = newLostSectorsAlert
)

func (m *SlabManager) UploadShards(ctx context.Context, slab Slab, shards [][]byte, available []types.PublicKey, log *zap.Logger) (int, error) {
	return m.uploadShards(ctx, slab, shards, available, log)
}

func (m *SlabManager) DownloadShards(ctx context.Context, slab Slab, log *zap.Logger) ([][]byte, error) {
	return m.downloadShards(ctx, slab, log)
}

func (m *SlabManager) MigrateSlabs(ctx context.Context, slabIDs []SlabID, log *zap.Logger) error {
	allHosts, goodContracts, err := m.migrationCandidates()
	if err != nil {
		return err
	}
	for _, slabID := range slabIDs {
		m.migrateSlab(ctx, slabID, allHosts, goodContracts, log.With(zap.Stringer("slab", slabID)))
	}
	return nil
}

func (m *SlabManager) PerformIntegrityChecksForHost(ctx context.Context, hostKey types.PublicKey, logger *zap.Logger) {
	m.performIntegrityChecksForHost(ctx, hostKey, logger)
}

func (m *SlabManager) PerformIntegrityChecks(ctx context.Context) error {
	return m.performIntegrityChecks(ctx)
}

func (m *SlabManager) SetShardTimeout(t time.Duration) {
	m.shardTimeout = t
}

func (m *SlabManager) SetVerifySectorsTimeout(d time.Duration) {
	m.verifyTimeout = d
}

func (m *SlabManager) MigrationAccount() proto.Account {
	return m.migrationAccount
}

func (m *SlabManager) MaxFailedIntegrityChecks() uint {
	return m.maxFailedIntegrityChecks
}
