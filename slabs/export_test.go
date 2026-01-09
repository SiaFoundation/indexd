package slabs

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const MaxTotalShards = maxTotalShards

var ErrNotEnoughShards = errNotEnoughShards
var ErrInsufficientServiceAccountBalance = errInsufficientServiceAccountBalance

//nolint:revive
func (m *SlabManager) UploadShards(ctx context.Context, slab Slab, shards [][]byte, available []types.PublicKey, log *zap.Logger) (int, error) {
	return m.uploadShards(ctx, slab, shards, available, log)
}

//nolint:revive
func (m *SlabManager) DownloadShards(ctx context.Context, slab Slab, log *zap.Logger) ([][]byte, error) {
	return m.downloadShards(ctx, slab, log)
}

//nolint:revive
func (m *SlabManager) MigrateSlabs(ctx context.Context, slabIDs []SlabID, log *zap.Logger) error {
	return m.migrateSlabs(ctx, slabIDs, log)
}

//nolint:revive
func (m *SlabManager) PerformIntegrityChecksForHost(ctx context.Context, hostKey types.PublicKey, logger *zap.Logger) {
	m.performIntegrityChecksForHost(ctx, hostKey, logger)
}

//nolint:revive
func (m *SlabManager) PerformIntegrityChecks(ctx context.Context) error {
	return m.performIntegrityChecks(ctx)
}

//nolint:revive
func SectorsToMigrate(slab Slab, allHosts []hosts.Host, goodContracts []contracts.Contract, minHostDistanceKm float64) ([]int, []types.PublicKey) {
	return sectorsToMigrate(slab, allHosts, goodContracts, minHostDistanceKm)
}

//nolint:revive
func NewSlabManager(chain ChainManager, am AccountManager, cm ContractManager, hm HostManager, store Store, hosts HostClient, alerter AlertsManager, migrationAccount, integrityAccount types.PrivateKey, opts ...Option) *SlabManager {
	return newSlabManager(chain, am, cm, hm, store, hosts, alerter, migrationAccount, integrityAccount, opts...)
}

//nolint:revive
func NewLostSectorsAlert(hks []types.PublicKey) alerts.Alert {
	return newLostSectorsAlert(hks)
}

func (m *SlabManager) SetShardTimeout(t time.Duration) {
	m.shardTimeout = t
}

func (m *SlabManager) MigrationAccount() proto.Account {
	return m.migrationAccount
}

func (m *SlabManager) MaxFailedIntegrityChecks() uint {
	return m.maxFailedIntegrityChecks
}
