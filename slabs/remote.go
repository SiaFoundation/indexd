package slabs

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	// remoteMigrationInterval is how long a remote node waits between passes
	// once it has worked through all currently-unhealthy slabs.
	remoteMigrationInterval = time.Minute

	// maxBackoffShift bounds the exponent of the exponential backoff applied
	// when consecutive passes migrate nothing, which points at a systemic
	// problem such as a recovery phrase that doesn't match the primary's. It
	// caps the pause at 2^5 = 32x the base interval and keeps the shift from
	// overflowing.
	maxBackoffShift = 5

	// resultReportTimeout bounds the single report of a batch's results. The
	// report runs on its own context so a shutdown mid-report doesn't abort
	// it: the results represent completed downloads and uploads, and dropping
	// them orphans the uploaded sectors and forgets discovered lost sectors.
	resultReportTimeout = 5 * time.Minute

	// remoteRequestTimeout bounds a single request to the primary so a
	// half-open connection cannot stall the worker indefinitely.
	remoteRequestTimeout = 5 * time.Minute
)

type (
	// A Primary is a RemoteMigrator's connection to the primary node: it hands
	// out batches of unhealthy slabs and persists the migration results. It is
	// implemented by the admin API client.
	Primary interface {
		MigrationBatch(ctx context.Context, cursor int64, limit int) (MigrationBatch, error)
		ApplyMigrationResults(ctx context.Context, results []MigrationResult) error
	}

	// A RemoteMigrator helps a primary node migrate unhealthy slabs without
	// access to its database: it fetches batches of unhealthy slabs from the
	// primary, downloads and re-uploads the affected sectors itself, and
	// reports the results back for the primary to persist.
	RemoteMigrator struct {
		primary             Primary
		migrationAccountKey types.PrivateKey

		workers  int
		interval time.Duration

		log *zap.Logger
	}

	// A RemoteMigratorOption configures an optional RemoteMigrator setting.
	RemoteMigratorOption func(*RemoteMigrator)
)

// WithRemoteWorkers sets the number of slabs a RemoteMigrator migrates in
// parallel. The default is runtime.NumCPU().
func WithRemoteWorkers(n int) RemoteMigratorOption {
	return func(rm *RemoteMigrator) {
		if n <= 0 {
			panic("remote workers must be positive") // developer error
		}
		rm.workers = n
	}
}

// WithRemoteInterval sets how long a RemoteMigrator waits between passes. The
// default is one minute.
func WithRemoteInterval(d time.Duration) RemoteMigratorOption {
	return func(rm *RemoteMigrator) {
		if d <= 0 {
			panic("remote interval must be positive") // developer error
		}
		rm.interval = d
	}
}

// NewRemoteMigrator creates a RemoteMigrator that migrates the primary node's
// unhealthy slabs. The migration account key must be derived from the same
// recovery phrase as the primary node's so it lines up with the host-side
// accounts the primary funds; a mismatch is not fatal but every migration will
// fail with insufficient-balance errors.
func NewRemoteMigrator(primary Primary, migrationAccountKey types.PrivateKey, log *zap.Logger, opts ...RemoteMigratorOption) *RemoteMigrator {
	if log == nil {
		log = zap.NewNop()
	}
	rm := &RemoteMigrator{
		primary:             primary,
		migrationAccountKey: migrationAccountKey,
		workers:             runtime.NumCPU(),
		interval:            remoteMigrationInterval,
		log:                 log,
	}
	for _, opt := range opts {
		opt(rm)
	}
	return rm
}

// Run repeatedly works through all currently-unhealthy slabs, pausing between
// passes, until the context is cancelled. When a pass attempts migrations but
// doesn't migrate a single sector the pause grows exponentially: a worker that
// can never succeed (e.g. its recovery phrase doesn't match the primary's, so
// its account is unfunded, or its uploads never reach any host) would otherwise
// claim and burn a fresh batch of slabs every interval, parking them for the
// claim duration with nothing to show.
func (rm *RemoteMigrator) Run(ctx context.Context) {
	store := newCachedHostStore()
	hostClient := client.New(client.NewProvider(store), rm.log.Named("client"))
	defer hostClient.Close()
	migrator := NewMigrator(hostClient, rm.migrationAccountKey, rm.log)

	failedPasses := 0
	for {
		executed, migrated, err := rm.runPass(ctx, store, migrator)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			rm.log.Error("migration pass failed", zap.Error(err))
		}
		// a pass makes progress only when it migrated sectors AND reported them
		// successfully; a failed report (err != nil) means the uploads weren't
		// persisted, so treat it as barren to keep a persistently failing
		// primary from hot-looping.
		if executed > 0 {
			if err == nil && migrated > 0 {
				failedPasses = 0
			} else {
				failedPasses++
			}
		}

		interval := rm.interval
		if failedPasses > 0 {
			interval = rm.interval << min(failedPasses, maxBackoffShift)
			rm.log.Warn("pass didn't migrate a single sector, backing off",
				zap.Int("consecutivePasses", failedPasses), zap.Duration("interval", interval))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// runPass pages through all unhealthy slabs the primary node has, determining
// which of their sectors to migrate from the state shipped with each batch,
// executing the migrations and reporting the results back. It returns how many
// migrations were attempted and how many sectors they migrated in total.
func (rm *RemoteMigrator) runPass(ctx context.Context, store *cachedHostStore, migrator *Migrator) (executed, migrated int, _ error) {
	// fetch a multiple of the number of workers per batch, like the local
	// migration loop does; the primary clamps the limit to its maximum.
	batchSize := migrationSlabsPerWorker * rm.workers

	var cursor int64
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, remoteRequestTimeout)
		batch, err := rm.primary.MigrationBatch(fetchCtx, cursor, batchSize)
		cancel()
		if err != nil {
			return executed, migrated, fmt.Errorf("failed to fetch migration batch: %w", err)
		}
		rm.log.Debug("fetched migration batch", zap.Int("slabs", len(batch.Slabs)), zap.Int64("cursor", cursor), zap.Int64("nextCursor", batch.NextCursor))
		store.reset(batch.State.Hosts)

		var mu sync.Mutex
		results := make([]MigrationResult, 0, len(batch.Slabs))
		var wg sync.WaitGroup
		sema := make(chan struct{}, rm.workers)
	dispatch:
		for _, slab := range batch.Slabs {
			select {
			case <-ctx.Done():
				break dispatch
			case sema <- struct{}{}:
			}
			wg.Go(func() {
				defer func() { <-sema }()
				if res, attempted := migrator.MigrateSlab(ctx, slab, batch.State); attempted {
					mu.Lock()
					results = append(results, res)
					mu.Unlock()
				}
			})
		}
		wg.Wait()

		executed += len(results)
		for _, res := range results {
			migrated += len(res.Migrated)
		}

		if err := rm.reportResults(results); err != nil {
			return executed, migrated, fmt.Errorf("failed to report migration results: %w", err)
		}

		// check if we were interrupted
		if ctx.Err() != nil {
			return executed, migrated, ctx.Err()
		}

		// a next cursor of 0 means there are no more unhealthy slabs
		if batch.NextCursor == 0 {
			return executed, migrated, nil
		}
		cursor = batch.NextCursor
	}
}

// reportResults reports a batch of migration results to the primary node. It
// deliberately runs on its own context rather than the pass context: the
// results represent completed downloads and uploads, so a shutdown that
// interrupts the report would orphan the uploaded sectors on their new hosts
// and lose any lost-sector discoveries. The batch is not retried on failure;
// the affected slabs are simply repaired again once their claim expires.
func (rm *RemoteMigrator) reportResults(results []MigrationResult) error {
	if len(results) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), resultReportTimeout)
	defer cancel()
	return rm.primary.ApplyMigrationResults(ctx, results)
}

// cachedHostStore is the client.Store a RemoteMigrator backs its RHP client
// with. It gets refreshed on every batch fetched for migration.
type cachedHostStore struct {
	mu    sync.RWMutex
	hosts map[types.PublicKey]hosts.Host
}

func newCachedHostStore() *cachedHostStore {
	return &cachedHostStore{hosts: make(map[types.PublicKey]hosts.Host)}
}

// reset replaces the store's contents with the usable hosts carried by a
// migration batch's state.
func (s *cachedHostStore) reset(usableHosts []hosts.Host) {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.hosts)
	for _, host := range usableHosts {
		s.hosts[host.PublicKey] = host
	}
}

// Addresses implements client.Store. It errors for hosts absent from the
// current batch's state — matching hosts.HostStore.Addresses — so a dial
// against an unknown host fails with a clear cause rather than a generic "no
// addresses found". The stored slices are never mutated (reset replaces
// entries wholesale) so the slice is returned directly.
func (s *cachedHostStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.hosts[hostKey]
	if !ok {
		return nil, hosts.ErrNotFound
	}
	return h.Addresses, nil
}

// Usable implements client.Store. It mirrors the primary node's DB-backed
// store: the host must be known (the state only carries unblocked hosts) and
// pass its usability checks, so recovery skips hosts the primary's own
// recovery would skip.
func (s *cachedHostStore) Usable(hostKey types.PublicKey) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.hosts[hostKey]
	return ok && h.IsGood(), nil
}

// UsableHosts implements client.Store.
func (s *cachedHostStore) UsableHosts() ([]hosts.HostInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var infos []hosts.HostInfo
	for _, h := range s.hosts {
		infos = append(infos, hosts.HostInfo{
			PublicKey:     h.PublicKey,
			Addresses:     h.Addresses,
			CountryCode:   h.CountryCode,
			Latitude:      h.Latitude,
			Longitude:     h.Longitude,
			GoodForUpload: h.GoodForUpload(),
		})
	}
	return infos, nil
}
