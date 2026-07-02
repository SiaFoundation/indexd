package slabs

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

// extraJobCandidates is the number of upload candidates included in a
// migration job beyond the number of sectors to migrate, giving the remote
// spare hosts to retry failed uploads against.
const extraJobCandidates = 10

// migrationState holds the hosts, contracts, and chain state needed for
// slab migrations.
type migrationState struct {
	Height              uint64
	HealthyContracts    []contracts.Contract
	Hosts               []hosts.Host
	MaintenanceSettings contracts.MaintenanceSettings
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

	// HostConn carries the connection information a remote node needs to reach
	// a host during a migration.
	HostConn struct {
		PublicKey types.PublicKey    `json:"publicKey"`
		Addresses []chain.NetAddress `json:"addresses"`
	}

	// MigrationJob is a fully-prepared unit of migration work handed to a
	// remote node: the slab to repair, which sector indices need migrating and
	// the vetted upload-candidate hosts. The connection info for the hosts
	// involved (download sources and candidates) is carried alongside the jobs,
	// deduplicated across a batch.
	MigrationJob struct {
		Slab       Slab              `json:"slab"`
		Migrate    []int             `json:"migrate"`
		Candidates []types.PublicKey `json:"candidates"`
	}

	// MigrationResult is the outcome of a MigrationJob, reported back by a
	// remote node for the primary to persist.
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

// fetchMigrationState fetches all available hosts, contracts, maintenance settings,
// and chain height needed for slab migrations.
func (m *SlabManager) fetchMigrationState() (state migrationState, err error) {
	// fetch hosts
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

func (m *SlabManager) migrateSlab(ctx context.Context, slabID SlabID, state migrationState, log *zap.Logger) {
	slab, err := m.store.Slab(slabID)
	if err != nil {
		log.Error("failed to fetch slab", zap.Error(err))
		return
	}
	indices, uploadCandidates := sectorsToMigrate(slab, state, m.minHostDistanceKm)
	if len(indices) == 0 {
		log.Debug("tried to migrate slab but no indices require migration")
		return
	} else if len(uploadCandidates) == 0 {
		log.Warn("tried to migrate slab but no hosts are available for migration")
		return
	}

	res := m.migrator.MigrateJob(ctx, MigrationJob{Slab: slab, Migrate: indices, Candidates: uploadCandidates})
	m.applyMigrationResult(res, log)
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
// worker runs directly (a remote node pulls prepared jobs from the primary,
// executes them via MigrateJob and reports the results back); the full
// SlabManager uses one internally.
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
func (m *Migrator) executeMigration(ctx context.Context, slab Slab, indices []int, candidates []types.PublicKey, log *zap.Logger) (migrated, lost []Shard, err error) {
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
	if err != nil {
		return nil, lost, err
	}
	log = log.With(zap.Duration("downloadElapsed", time.Since(downloadStart)))

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
	log = log.With(zap.Duration("uploadElapsed", time.Since(uploadStart)), zap.Int("migrated", len(migrated)))
	if err != nil {
		log.Warn("failed to upload migrated shards", zap.Error(err))
	}
	return migrated, lost, nil
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

// PrepareMigrationJobs selects a batch of unhealthy slabs and prepares
// migration jobs for them, determining which sectors need migrating and the
// candidate hosts to migrate them to. The returned jobs do not include host
// connection info; callers resolve host addresses before handing jobs to a
// remote node. The returned cursor is passed to the next call to continue
// paging; a cursor of 0 means there are no more unhealthy slabs.
func (m *SlabManager) PrepareMigrationJobs(cursor int64, limit int) ([]MigrationJob, int64, error) {
	ids, nextCursor, err := m.store.UnhealthySlabs(cursor, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch unhealthy slabs: %w", err)
	} else if len(ids) == 0 {
		return nil, nextCursor, nil
	}

	state, err := m.fetchMigrationState()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch migration state: %w", err)
	}

	// hosts without a known address can never be dialed by a remote; drop
	// them from the candidate pool before capping so the cap doesn't waste
	// slots on unreachable hosts.
	reachable := make(map[types.PublicKey]struct{}, len(state.Hosts))
	for _, h := range state.Hosts {
		if len(h.Addresses) > 0 {
			reachable[h.PublicKey] = struct{}{}
		}
	}

	log := m.log.Named("migrations")
	jobs := make([]MigrationJob, 0, len(ids))
	for _, id := range ids {
		slab, err := m.store.Slab(id)
		if err != nil {
			// tolerate per-slab failures (e.g. the slab was pruned after being
			// claimed) so one bad slab doesn't abort a batch of already-claimed
			// slabs, mirroring the local migration loop.
			log.Error("failed to fetch slab", zap.Stringer("slab", id), zap.Error(err))
			continue
		}
		indices, candidates := sectorsToMigrate(slab, state, m.minHostDistanceKm)
		viable := candidates[:0]
		for _, hk := range candidates {
			if _, ok := reachable[hk]; ok {
				viable = append(viable, hk)
			}
		}
		candidates = viable
		if len(indices) == 0 {
			log.Debug("skipping slab, no indices require migration", zap.Stringer("slab", id))
			continue
		} else if len(candidates) == 0 {
			// mirror the local loop's warning so a slab that can never be
			// repaired is visible even when migrations are outsourced.
			log.Warn("skipping slab, no hosts are available for migration", zap.Stringer("slab", id))
			continue
		}
		// sectorsToMigrate returns every eligible host in random order; the
		// job only needs enough of them for the required uploads plus spares
		// for failed attempts. Truncating keeps the wire payload proportional
		// to the work instead of the network size.
		candidates = candidates[:min(len(candidates), len(indices)+extraJobCandidates)]
		jobs = append(jobs, MigrationJob{
			Slab:       slab,
			Migrate:    indices,
			Candidates: candidates,
		})
	}
	return jobs, nextCursor, nil
}

// migratableDestinations returns the set of destination hosts from the results
// that are still known and unblocked. A nil map disables filtering; it is
// returned when the results carry no migrated sectors or when the host lookup
// fails (per-sector writes surface persistent store failures themselves).
func (m *SlabManager) migratableDestinations(results []MigrationResult, log *zap.Logger) map[types.PublicKey]struct{} {
	keys := make(map[types.PublicKey]struct{})
	for _, res := range results {
		for _, s := range res.Migrated {
			keys[s.HostKey] = struct{}{}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	hks := make([]types.PublicKey, 0, len(keys))
	for hk := range keys {
		hks = append(hks, hk)
	}
	batch, err := m.store.Hosts(0, len(hks), hosts.WithPublicKeys(hks), hosts.WithBlocked(false))
	if err != nil {
		log.Error("failed to validate migration destinations, applying unfiltered", zap.Error(err))
		return nil
	}
	allowed := make(map[types.PublicKey]struct{}, len(batch))
	for _, h := range batch {
		allowed[h.PublicKey] = struct{}{}
	}
	return allowed
}

// ApplyMigrationResults persists the outcomes of migration jobs reported by a
// remote node. Destination hosts are re-validated so sectors are not recorded
// on hosts that were blocked or pruned after the job was prepared. Per-result
// store failures are logged and skipped, matching the local migration loop.
func (m *SlabManager) ApplyMigrationResults(results []MigrationResult) {
	log := m.log.Named("migrations")
	allowed := m.migratableDestinations(results, log)
	for _, res := range results {
		log := log.With(zap.Stringer("slab", res.SlabID))
		if allowed != nil {
			migrated := res.Migrated[:0]
			for _, s := range res.Migrated {
				if _, ok := allowed[s.HostKey]; ok {
					migrated = append(migrated, s)
					continue
				}
				// keep the failure backoff: the repair is incomplete
				log.Warn("dropping migrated sector, destination host is blocked or unknown", zap.Stringer("root", s.Root), zap.Stringer("host", s.HostKey))
				res.Success = false
			}
			res.Migrated = migrated
		}
		m.applyMigrationResult(res, log)
	}
}

// MigrateJob executes a prepared migration job, performing the shard recovery
// and upload, and returns the result for the primary node to persist. It
// performs no store writes and is used by remote nodes.
func (m *Migrator) MigrateJob(ctx context.Context, job MigrationJob) MigrationResult {
	log := m.log.With(zap.Stringer("slab", job.Slab.ID), zap.Int("toMigrate", len(job.Migrate)), zap.Int("uploadCandidates", len(job.Candidates)))
	migrated, lost, err := m.executeMigration(ctx, job.Slab, job.Migrate, job.Candidates, log)
	res := MigrationResult{SlabID: job.Slab.ID, Migrated: migrated, Lost: lost}
	if err != nil {
		if ctx.Err() == nil {
			log.Error("failed to recover slab", zap.Error(err))
		}
		return res
	}
	res.Recovered = true
	res.Success = len(migrated) == len(job.Migrate)
	if res.Success {
		log.Debug("slab successfully repaired")
	} else {
		log.Debug("slab partially repaired")
	}
	return res
}

// sectorsToMigrate filters the sectors of a slab and returns the indices of the
// sectors that require migration together with hosts that can be used to
// migrate bad sectors to. These hosts are guaranteed to be at least
// minHostDistance apart from each other and are returned in random order.
// The subset of healthy contracts eligible for uploading migrated sectors is
// derived by filtering with GoodForAppend.
func sectorsToMigrate(slab Slab, state migrationState, minHostDistanceKm float64) ([]int, []types.PublicKey) {
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
	set := hosts.NewSpacedSet(minHostDistanceKm)

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
		} else if !host.StuckSince.IsZero() {
			// can't migrate to stuck hosts
			continue
		} else if host.Settings.RemainingStorage == 0 {
			// can't migrate to hosts without storage
			continue
		}

		// must be sufficiently far apart
		if set.Add(host) {
			candidates = append(candidates, host.PublicKey)
		}
	}
	return toMigrate, candidates
}
