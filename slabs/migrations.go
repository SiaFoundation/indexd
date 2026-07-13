package slabs

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

// migrationSlabsPerWorker is the number of unhealthy slabs fetched per
// migration worker when batching migrations, ensuring the workers don't finish
// their work before the next batch is ready. It is shared by the local
// migration loop and remote migration workers.
const migrationSlabsPerWorker = 10

// MigrationState holds the hosts, contracts, and chain state needed to
// determine which sectors of a slab require migration and which hosts can
// receive them. It is shipped to remote nodes alongside a batch of unhealthy
// slabs so they can make the same decisions the local migration loop makes.
type MigrationState struct {
	Height              uint64                        `json:"height"`
	HealthyContracts    []contracts.Contract          `json:"healthyContracts"`
	Hosts               []hosts.Host                  `json:"hosts"`
	MaintenanceSettings contracts.MaintenanceSettings `json:"maintenanceSettings"`
	// MinHostDistanceKm is the primary node's minimum distance between hosts
	// storing sectors of the same slab, so remote nodes apply the same
	// placement policy.
	MinHostDistanceKm float64 `json:"minHostDistanceKm"`
}

type (
	// Migrator performs the network-heavy part of a slab migration: it recovers
	// a slab's missing shards from hosts and uploads the re-encrypted shards to
	// new hosts. It holds only what recovery and upload require and is used both
	// by the SlabManager (full node) and directly by remote migration workers.
	// It trusts the candidate hosts it is given — they were vetted when the
	// migration was prepared.
	Migrator struct {
		hosts               HostClient
		migrationAccountKey types.PrivateKey
		shardTimeout        time.Duration

		// recoveryChunkSize is the size of the segment-aligned byte range
		// requested from each host during slab recovery. Smaller chunks
		// spread a recovery across more hosts (more parallel pipes) at the
		// cost of more RPCs. Must be a multiple of proto.LeafSize.
		recoveryChunkSize int

		log *zap.Logger
	}

	// MigrationBatch is a batch of unhealthy slabs together with the migration
	// state a remote node needs to determine which of their sectors to migrate
	// and which hosts can receive them. A NextCursor of 0 means there are no
	// more unhealthy slabs.
	MigrationBatch struct {
		Slabs      []Slab         `json:"slabs"`
		State      MigrationState `json:"state"`
		NextCursor int64          `json:"nextCursor"`
	}

	// MigrationResult is the outcome of migrating a single slab, reported back
	// by a remote node for the primary to persist.
	MigrationResult struct {
		SlabID   SlabID  `json:"slabID"`
		Migrated []Shard `json:"migrated"`
		Lost     []Shard `json:"lost"`
		// Recovered reports whether the slab's shards were successfully
		// recovered. If false, the repair state is left untouched.
		Recovered bool `json:"recovered"`
		// Success reports whether every required sector was migrated. Only
		// meaningful when Recovered is true.
		Success bool `json:"success"`
	}
)

// fetchMigrationState fetches everything needed to decide which of a slab's
// sectors to migrate and where: all unblocked hosts with active contracts, the
// healthy contracts, maintenance settings, chain height, and the configured
// minimum host distance.
func (m *SlabManager) fetchMigrationState() (state MigrationState, err error) {
	state.MinHostDistanceKm = m.minHostDistanceKm
	const batchSize = 500
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Hosts(offset, batchSize,
			hosts.WithBlocked(false),
			hosts.WithActiveContracts(true))
		if err != nil {
			return state, fmt.Errorf("failed to fetch hosts: %w", err)
		}

		state.Hosts = append(state.Hosts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// fetch healthy contracts
	state.HealthyContracts, err = m.cm.HealthyContracts()
	if err != nil {
		return state, fmt.Errorf("failed to fetch healthy contracts: %w", err)
	}

	// fetch maintenance settings
	state.MaintenanceSettings, err = m.store.MaintenanceSettings()
	if err != nil {
		return state, fmt.Errorf("failed to fetch maintenance settings: %w", err)
	}

	// fetch chain height
	tip, err := m.store.Tip()
	if err != nil {
		return state, fmt.Errorf("failed to fetch consensus tip: %w", err)
	}
	state.Height = tip.Height
	return state, nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slabID SlabID, state MigrationState, log *zap.Logger) {
	slab, err := m.store.Slab(slabID)
	if err != nil {
		log.Error("failed to fetch slab", zap.Error(err))
		return
	}
	if res, attempted := m.migrator.MigrateSlab(ctx, slab, state); attempted {
		m.applyMigrationResult(res, log)
	}
}

// A MigratorOption configures an optional Migrator setting.
type MigratorOption func(*Migrator)

// WithRecoveryChunkSize sets the size of the segment-aligned byte range
// requested from each host during slab recovery. Smaller chunks spread a
// recovery across more hosts at the cost of more RPCs. The value is clamped
// to [proto.LeafSize, proto.SectorSize] and rounded down to a multiple of
// proto.LeafSize when used. The default is 1 MiB.
func WithRecoveryChunkSize(size int) MigratorOption {
	return func(m *Migrator) {
		if size <= 0 {
			panic("recovery chunk size must be positive") // developer error
		}
		m.recoveryChunkSize = size
	}
}

// NewMigrator creates a Migrator that recovers a slab's shards from hosts and
// uploads the re-encrypted shards to new hosts. It is what a remote migration
// worker runs directly (a remote node fetches batches of unhealthy slabs from
// the primary, migrates them via MigrateSlab and reports the results back);
// the full SlabManager uses one internally.
func NewMigrator(hosts HostClient, migrationAccount types.PrivateKey, log *zap.Logger, opts ...MigratorOption) *Migrator {
	if log == nil {
		log = zap.NewNop()
	}
	m := &Migrator{
		hosts:               hosts,
		migrationAccountKey: migrationAccount,
		shardTimeout:        2 * time.Minute,
		recoveryChunkSize:   defaultRecoveryChunkSize,
		log:                 log,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// executeMigration recovers the required shards of a slab, re-encrypts them and
// uploads them to the candidate hosts. The migrated and lost (root, host) pairs
// are returned for the caller to persist. A non-nil error indicates recovery
// failed, in which case the slab's repair state should be left untouched; lost
// is still populated so the caller can persist any sectors discovered lost
// during the failed recovery.
func (m *Migrator) executeMigration(ctx context.Context, slab Slab, indices []int, candidates []types.PublicKey, log *zap.Logger) (migrated, lost []Shard, downloadElapsed, uploadElapsed time.Duration, err error) {
	// indicate what shards are required
	required := make([]bool, len(slab.Sectors))
	for _, i := range indices {
		required[i] = true
	}

	// recover the required shards by downloading segment-aligned chunks spread
	// across all available hosts and reconstructing them in plaintext.
	// note: timeouts are set within recoverShards to avoid timing
	// out the database
	downloadStart := time.Now()
	shards, lost, err := m.recoverShards(ctx, slab, required, log.Named("recover"))
	downloadElapsed = time.Since(downloadStart)
	if err != nil {
		return nil, lost, downloadElapsed, 0, err
	}

	// re-encrypt the recovered shards for upload
	nonce := make([]byte, 24)
	for i, req := range required {
		if !req {
			shards[i] = nil
			continue
		}

		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(slab.EncryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])
	}

	// migrate the shards
	// note: timeouts are set within uploadShards to avoid timing out the database
	uploadStart := time.Now()
	migrated, err = m.uploadShards(ctx, slab, shards, candidates, log.Named("migrate"))
	uploadElapsed = time.Since(uploadStart)
	if err != nil {
		log.Warn("failed to upload migrated shards",
			zap.Duration("downloadElapsed", downloadElapsed),
			zap.Duration("uploadElapsed", uploadElapsed),
			zap.Int("migrated", len(migrated)), zap.Error(err))
	}
	return migrated, lost, downloadElapsed, uploadElapsed, nil
}

// applyMigrationResult persists the outcome of migrating a single slab: it
// records lost sectors, the new locations of migrated sectors and updates the
// slab's repair state. It is shared by the local migration loop and the remote
// result-reporting endpoint.
func (m *SlabManager) applyMigrationResult(res MigrationResult, log *zap.Logger) {
	// group lost sectors by host so each host is marked in a single call
	if len(res.Lost) > 0 {
		lostByHost := make(map[types.PublicKey][]types.Hash256)
		for _, s := range res.Lost {
			lostByHost[s.HostKey] = append(lostByHost[s.HostKey], s.Root)
		}
		for hk, roots := range lostByHost {
			if err := m.store.MarkSectorsLost(hk, roots); err != nil {
				log.Error("failed to mark sectors as lost", zap.Stringer("host", hk), zap.Error(err))
			}
		}
	}

	// if recovery failed, leave the repair state untouched so the slab is
	// retried without incurring a repair-failure backoff.
	if !res.Recovered {
		return
	}

	// a migrated sector only counts as repaired once its new location is
	// persisted; a store failure or no-op here must not mark the slab
	// successfully repaired or the uploaded sector is orphaned and the slab's
	// failure counter is reset even though it still needs repair.
	persisted := 0
	for _, s := range res.Migrated {
		if migrated, err := m.store.MigrateSector(s.Root, s.HostKey); err != nil {
			log.Error("failed to record migrated sector", zap.Stringer("root", s.Root), zap.Error(err))
			continue
		} else if !migrated {
			// the sector or the destination host no longer exists
			log.Warn("migrated sector no longer applicable", zap.Stringer("root", s.Root), zap.Stringer("host", s.HostKey))
			continue
		}
		persisted++
	}
	if persisted > 0 {
		// record the slab got migrated so object events get updated
		if err := m.store.RecordSlabMigrated(res.SlabID); err != nil {
			log.Debug("failed to record slab migration", zap.Error(err))
		}
	}
	success := res.Success && persisted == len(res.Migrated)
	if err := m.store.MarkSlabRepaired(res.SlabID, success); err != nil {
		log.Error("failed to mark slab repaired", zap.Error(err))
	}
}

// PrepareMigrationBatch selects a batch of unhealthy slabs and bundles them
// with the migration state a remote node needs to migrate them, mirroring the
// decisions the local migration loop makes. The batch's cursor is passed to
// the next call to continue paging; a cursor of 0 means there are no more
// unhealthy slabs.
func (m *SlabManager) PrepareMigrationBatch(cursor int64, limit int) (MigrationBatch, error) {
	ids, nextCursor, err := m.store.UnhealthySlabs(cursor, limit)
	if err != nil {
		return MigrationBatch{}, fmt.Errorf("failed to fetch unhealthy slabs: %w", err)
	} else if len(ids) == 0 {
		return MigrationBatch{NextCursor: nextCursor}, nil
	}

	state, err := m.fetchMigrationState()
	if err != nil {
		return MigrationBatch{}, fmt.Errorf("failed to fetch migration state: %w", err)
	}

	log := m.log.Named("migrations")
	batchSlabs := make([]Slab, 0, len(ids))
	for _, id := range ids {
		slab, err := m.store.Slab(id)
		if err != nil {
			// tolerate per-slab failures (e.g. the slab was pruned after being
			// claimed) so one bad slab doesn't abort a batch of already-claimed
			// slabs, mirroring the local migration loop.
			log.Error("failed to fetch slab", zap.Stringer("slab", id), zap.Error(err))
			continue
		}
		batchSlabs = append(batchSlabs, slab)
	}

	return MigrationBatch{
		Slabs:      batchSlabs,
		State:      state,
		NextCursor: nextCursor,
	}, nil
}

// ApplyMigrationResults persists the outcomes of migrations reported by a
// remote node. Per-result store failures are logged and skipped, matching the
// local migration loop. A destination host that was blocked after the batch
// was prepared is not re-validated here: the sector is recorded and the next
// health check re-flags the slab, re-migrating it away at most one batch later.
func (m *SlabManager) ApplyMigrationResults(results []MigrationResult) {
	log := m.log.Named("migrations")
	for _, res := range results {
		m.applyMigrationResult(res, log.With(zap.Stringer("slab", res.SlabID)))
	}
}

// MigrateSlab determines which of the slab's sectors require migration and
// which hosts can receive them — the same decisions the local migration loop
// makes — then performs the shard recovery and upload. It performs no store
// writes; the returned result is persisted by the primary node. attempted
// reports whether a migration was actually performed: a slab that needs no
// migration or has no candidate hosts is skipped and its result must not be
// applied.
func (m *Migrator) MigrateSlab(ctx context.Context, slab Slab, state MigrationState) (res MigrationResult, attempted bool) {
	res = MigrationResult{SlabID: slab.ID}
	log := m.log.With(zap.Stringer("slab", slab.ID))

	indices, candidates := sectorsToMigrate(slab, state)
	if len(indices) == 0 {
		log.Debug("tried to migrate slab but no indices require migration")
		return res, false
	} else if len(candidates) == 0 {
		log.Warn("tried to migrate slab but no hosts are available for migration")
		return res, false
	}
	log = log.With(zap.Int("toMigrate", len(indices)), zap.Int("uploadCandidates", len(candidates)))

	migrated, lost, downloadElapsed, uploadElapsed, err := m.executeMigration(ctx, slab, indices, candidates, log)
	res.Migrated, res.Lost = migrated, lost
	log = log.With(zap.Duration("downloadElapsed", downloadElapsed), zap.Duration("uploadElapsed", uploadElapsed), zap.Int("migrated", len(migrated)))
	if err != nil {
		if ctx.Err() == nil {
			log.Error("failed to recover slab", zap.Error(err))
		}
		return res, true
	}
	res.Recovered = true
	res.Success = len(migrated) == len(indices)
	if res.Success {
		log.Debug("slab successfully repaired")
	} else {
		log.Debug("slab partially repaired")
	}
	return res, true
}

// sectorsToMigrate filters the sectors of a slab and returns the indices of the
// sectors that require migration together with hosts that can be used to
// migrate bad sectors to. These hosts are guaranteed to be at least
// minHostDistance apart from each other and are returned in random order.
// The subset of healthy contracts eligible for uploading migrated sectors is
// derived by filtering with GoodForAppend.
func sectorsToMigrate(slab Slab, state MigrationState) ([]int, []types.PublicKey) {
	// prepare a map of good hosts
	hostsMap := make(map[types.PublicKey]hosts.Host)
	for _, host := range state.Hosts {
		if host.IsGood() {
			hostsMap[host.PublicKey] = host
		}
	}

	// prepare a map of healthy contracts for sector health checks and track
	// which hosts have migration eligible contracts for candidate selection
	healthyContractMap := make(map[types.FileContractID]contracts.Contract)
	hasAppendContract := make(map[types.PublicKey]struct{})
	for _, contract := range state.HealthyContracts {
		host, ok := hostsMap[contract.HostKey]
		if !ok {
			continue
		}
		healthyContractMap[contract.ID] = contract
		if contract.GoodForAppend(host.Settings, state.MaintenanceSettings.RenewWindow, state.Height, state.MaintenanceSettings.Period) == nil {
			hasAppendContract[contract.HostKey] = struct{}{}
		}
	}

	// keep track of hosts in a spaced set, ensuring we store sectors on hosts
	// that are sufficiently far apart. We don't care if two good sectors on
	// hosts that are too close to one another, but we don't want to migrate bad
	// sectors to hosts that are too close to those same hosts
	set := hosts.NewSpacedSet(state.MinHostDistanceKm)

	// determine whether the sector needs to be migrated. That's the case if
	// one of the following is true:
	// - the sector is stored on a bad host
	// - the sector is lost (host key is nil)
	// - the sector is stored on a bad contract
	var toMigrate []int
	for i, sector := range slab.Sectors {
		if sector.HostKey == nil {
			// sector is lost
			toMigrate = append(toMigrate, i)
			continue
		}

		host, ok := hostsMap[*sector.HostKey]
		if !ok {
			// sector is on a bad host
			toMigrate = append(toMigrate, i)
			continue
		}
		// prevent duplicate hosts
		delete(hostsMap, *sector.HostKey)

		if sector.ContractID != nil {
			if _, ok := healthyContractMap[*sector.ContractID]; !ok {
				// sector is on a bad contract
				toMigrate = append(toMigrate, i)
				continue
			}
			delete(healthyContractMap, *sector.ContractID)
		}

		// sector will not be migrated. Remove it from the hosts map
		// and add it to the spaced set.
		set.Add(host)
	}
	var candidates []types.PublicKey
	for _, host := range hostsMap {
		if _, ok := hasAppendContract[host.PublicKey]; !ok {
			// must have an appendable contract
			continue
		} else if !host.GoodForUpload() {
			// must not be stuck and have storage left
			continue
		}

		// must be sufficiently far apart
		if set.Add(host) {
			candidates = append(candidates, host.PublicKey)
		}
	}
	return toMigrate, candidates
}
