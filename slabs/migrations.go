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

// migrationState holds the hosts, contracts, and chain state needed for
// slab migrations.
type migrationState struct {
	Height              uint64
	HealthyContracts    []contracts.Contract
	Hosts               []hosts.Host
	MaintenanceSettings contracts.MaintenanceSettings
}

type (
	// HostConn carries the connection information a remote node needs to reach
	// a host during a migration.
	HostConn struct {
		PublicKey types.PublicKey    `json:"publicKey"`
		Addresses []chain.NetAddress `json:"addresses"`
	}

	// MigrationJob is a fully-prepared unit of migration work handed to a
	// remote node: the slab to repair, which sector indices need migrating, the
	// vetted upload-candidate hosts and the connection info for every host
	// involved (download sources and candidates).
	MigrationJob struct {
		Slab       Slab              `json:"slab"`
		Migrate    []int             `json:"migrate"`
		Candidates []types.PublicKey `json:"candidates"`
		Hosts      []HostConn        `json:"hosts"`
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
	log = log.With(zap.Int("toMigrate", len(indices)), zap.Int("uploadCandidates", len(uploadCandidates)))

	migrated, lost, err := m.executeMigration(ctx, slab, indices, uploadCandidates, log)
	res := MigrationResult{SlabID: slabID, Migrated: migrated, Lost: lost}
	if err != nil {
		if ctx.Err() == nil {
			log.Error("failed to recover slab", zap.Error(err))
		}
	} else {
		res.Recovered = true
		res.Success = len(migrated) == len(indices)
	}
	m.applyMigrationResult(res, log)

	if res.Recovered {
		if res.Success {
			log.Debug("slab successfully repaired")
		} else {
			log.Debug("slab partially repaired")
		}
	}
}

// executeMigration recovers the required shards of a slab, re-encrypts them and
// uploads them to the candidate hosts. The migrated and lost (root, host) pairs
// are returned for the caller to persist. A non-nil error indicates recovery
// failed, in which case the slab's repair state should be left untouched; lost
// is still populated so the caller can persist any sectors discovered lost
// during the failed recovery.
func (m *SlabManager) executeMigration(ctx context.Context, slab Slab, indices []int, candidates []types.PublicKey, log *zap.Logger) (migrated, lost []Shard, err error) {
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
	for _, s := range res.Lost {
		if err := m.store.MarkSectorsLost(s.HostKey, []types.Hash256{s.Root}); err != nil {
			log.Error("failed to mark sector as lost", zap.Stringer("host", s.HostKey), zap.Error(err))
		}
	}

	// if recovery failed, leave the repair state untouched so the slab is
	// retried without incurring a repair-failure backoff.
	if !res.Recovered {
		return
	}

	for _, s := range res.Migrated {
		if _, err := m.store.MigrateSector(s.Root, s.HostKey); err != nil {
			log.Error("failed to record migrated sector", zap.Stringer("root", s.Root), zap.Error(err))
		}
	}
	if len(res.Migrated) > 0 {
		// record the slab got migrated so object events get updated
		if err := m.store.RecordSlabMigrated(res.SlabID); err != nil {
			log.Debug("failed to record slab migration", zap.Error(err))
		}
	}
	if err := m.store.MarkSlabRepaired(res.SlabID, res.Success); err != nil {
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

	jobs := make([]MigrationJob, 0, len(ids))
	for _, id := range ids {
		slab, err := m.store.Slab(id)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to fetch slab %s: %w", id, err)
		}
		indices, candidates := sectorsToMigrate(slab, state, m.minHostDistanceKm)
		if len(indices) == 0 || len(candidates) == 0 {
			// nothing to migrate, or nowhere to migrate to; skip so the slab is
			// retried on a later pass without altering its repair state.
			continue
		}
		jobs = append(jobs, MigrationJob{
			Slab:       slab,
			Migrate:    indices,
			Candidates: candidates,
		})
	}
	return jobs, nextCursor, nil
}

// ApplyMigrationResults persists the outcomes of migration jobs reported by a
// remote node.
func (m *SlabManager) ApplyMigrationResults(results []MigrationResult) error {
	log := m.log.Named("migrations")
	for _, res := range results {
		m.applyMigrationResult(res, log.With(zap.Stringer("slab", res.SlabID)))
	}
	return nil
}

// MigrateJob executes a prepared migration job, performing the shard recovery
// and upload, and returns the result for the primary node to persist. It
// performs no store writes and is used by remote nodes.
func (m *SlabManager) MigrateJob(ctx context.Context, job MigrationJob) MigrationResult {
	log := m.log.Named("migrations").With(zap.Stringer("slab", job.Slab.ID), zap.Int("toMigrate", len(job.Migrate)), zap.Int("uploadCandidates", len(job.Candidates)))
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
