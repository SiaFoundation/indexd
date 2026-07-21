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

	// resultReportGroupSize is the maximum number of migration results
	// reported to the primary in a single request.
	resultReportGroupSize = 100

	// resultReportInterval is how often buffered migration results are
	// flushed to the primary while migrations are still running, so results
	// are persisted promptly even when slow slabs trickle in.
	resultReportInterval = 30 * time.Second
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
		// a pass makes progress only when at least one migrated sector was
		// durably persisted by the primary. migrated only counts successfully
		// reported sectors, so a late report failure neither masks earlier
		// persisted groups nor lets unpersisted uploads count as progress —
		// a worker that can never persist (failing uploads or a persistently
		// failing primary) backs off either way.
		if executed > 0 {
			if migrated > 0 {
				failedPasses = 0
			} else {
				failedPasses++
			}
		}

		interval := rm.interval
		if failedPasses > 0 {
			interval = rm.interval << min(failedPasses, maxBackoffShift)
			rm.log.Warn("pass didn't persist a single migrated sector, backing off",
				zap.Int("consecutivePasses", failedPasses), zap.Duration("interval", interval))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// runPass pages through all unhealthy slabs the primary node has and migrates
// them. Mirroring the primary's own migration loop, a producer goroutine
// fetches batches from the primary and feeds them to the workers through a
// channel, so fetching the next batch overlaps with ongoing migrations
// instead of waiting behind the slowest slab of a batch. Results are reported
// back in groups as migrations complete, with at most one report in flight so
// a slow primary never stalls the workers. It returns how many migrations
// were attempted and how many migrated sectors were durably reported to the
// primary.
func (rm *RemoteMigrator) runPass(ctx context.Context, store *cachedHostStore, migrator *Migrator) (executed, migrated int, _ error) {
	// fetch a multiple of the number of workers per batch, like the local
	// migration loop does; the primary clamps the limit to its maximum.
	batchSize := migrationSlabsPerWorker * rm.workers

	// the pass starts with nothing in flight, so it is safe to drop hosts
	// cached by previous passes; within a pass the store only ever merges,
	// so in-flight migrations never lose their hosts.
	store.clear()

	// passCtx aborts the producer and in-flight migrations when reporting
	// results fails: without persistence any further migration is wasted
	// work.
	passCtx, cancelPass := context.WithCancel(ctx)
	defer cancelPass()

	// fetching a batch claims its slabs on the primary for the repair
	// backoff, so the job queue is kept shallow: the producer claims the
	// next batch only once the workers are close to running dry, keeping
	// the time a claimed slab sits queued well below the claim window.
	type migrationJob struct {
		slab  Slab
		state MigrationState
	}
	slabCh := make(chan migrationJob, rm.workers)

	// the result channel is buffered so workers can hand off a result and
	// pick up the next slab even while the collector is busy
	resultCh := make(chan MigrationResult, batchSize)
	var wg sync.WaitGroup
	for range rm.workers {
		wg.Go(func() {
			for job := range slabCh {
				if res, attempted := migrator.MigrateSlab(passCtx, job.slab, job.state); attempted {
					resultCh <- res
				}
			}
		})
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// fetch unhealthy slabs and feed them to the workers. producerErr is
	// read only after the collect loop below, which the close chain
	// (slabCh -> workers -> resultCh) sequences after the producer's return.
	var producerErr error
	go func() {
		defer close(slabCh)
		var cursor int64
		for {
			rm.log.Debug("fetching migration batch", zap.Int64("cursor", cursor), zap.Int("limit", batchSize))
			fetchCtx, cancel := context.WithTimeout(passCtx, remoteRequestTimeout)
			batch, err := rm.primary.MigrationBatch(fetchCtx, cursor, batchSize)
			cancel()
			if err != nil {
				if passCtx.Err() == nil {
					producerErr = fmt.Errorf("failed to fetch migration batch: %w", err)
				}
				return
			}
			rm.log.Debug("fetched migration batch", zap.Int("slabs", len(batch.Slabs)), zap.Int64("cursor", cursor), zap.Int64("nextCursor", batch.NextCursor))
			// merge rather than replace: migrations from earlier batches may
			// still be in flight and must not lose their hosts. This also
			// makes batches without claimable slabs — which carry no
			// migration state at all — inherently harmless.
			store.merge(batch.State.Hosts)

			for _, slab := range batch.Slabs {
				select {
				case slabCh <- migrationJob{slab: slab, state: batch.State}:
				case <-passCtx.Done():
					return
				}
			}

			// a next cursor of 0 means there are no more unhealthy slabs
			if batch.NextCursor == 0 {
				return
			}
			cursor = batch.NextCursor
		}
	}()

	// collect results as migrations complete and report them to the primary
	// in groups. Reports run on their own goroutine — at most one in flight —
	// so a slow report never stalls result collection and with it the
	// workers. migrated only counts sectors whose report succeeded.
	var reportErr error
	reporting := false                  // a report goroutine is in flight
	reportDoneCh := make(chan error, 1) // its outcome
	inFlightSectors := 0                // migrated sectors it carries
	pending := make([]MigrationResult, 0, resultReportGroupSize)

	countSectors := func(results []MigrationResult) (n int) {
		for _, res := range results {
			n += len(res.Migrated)
		}
		return
	}
	startReport := func() {
		if len(pending) == 0 || reporting || reportErr != nil {
			return
		}
		group := pending
		pending = nil
		reporting, inFlightSectors = true, countSectors(group)
		go func() {
			reportDoneCh <- rm.reportResults(group)
		}()
	}
	finishReport := func(err error) {
		reporting = false
		if err != nil {
			if reportErr == nil {
				reportErr = fmt.Errorf("failed to report migration results: %w", err)
				cancelPass()
			}
			return
		}
		migrated += inFlightSectors
	}

	flushTicker := time.NewTicker(resultReportInterval)
	defer flushTicker.Stop()
collect:
	for {
		select {
		case res, ok := <-resultCh:
			if !ok {
				break collect
			}
			executed++
			pending = append(pending, res)
			if len(pending) >= resultReportGroupSize {
				startReport()
			}
		case err := <-reportDoneCh:
			finishReport(err)
			startReport() // flush any backlog that accumulated meanwhile
		case <-flushTicker.C:
			startReport()
		}
	}
	// wait for the in-flight report, then flush the remainder. The remainder
	// gets its one attempt even after an earlier report failure: these
	// results were never offered to the primary and carry completed uploads
	// and lost-sector discoveries that would otherwise be orphaned.
	if reporting {
		finishReport(<-reportDoneCh)
	}
	if len(pending) > 0 {
		if err := rm.reportResults(pending); err != nil {
			if reportErr == nil {
				reportErr = fmt.Errorf("failed to report migration results: %w", err)
			} else {
				rm.log.Error("dropping unreported migration results", zap.Int("results", len(pending)), zap.Error(err))
			}
		} else {
			migrated += countSectors(pending)
		}
	}

	if reportErr != nil {
		return executed, migrated, reportErr
	}
	if producerErr != nil {
		return executed, migrated, producerErr
	}
	// surface the interruption if we were cancelled
	return executed, migrated, ctx.Err()
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
// with. Every fetched batch merges its state's hosts in; the store is only
// cleared between passes, when no migrations are in flight, so a host never
// vanishes from under an in-flight migration.
type cachedHostStore struct {
	mu    sync.RWMutex
	hosts map[types.PublicKey]hosts.Host
}

func newCachedHostStore() *cachedHostStore {
	return &cachedHostStore{hosts: make(map[types.PublicKey]hosts.Host)}
}

// merge upserts the usable hosts carried by a migration batch's state. Hosts
// absent from the batch are deliberately kept: migrations from earlier
// batches may still be reading from them. Stale entries are dropped by clear
// between passes.
func (s *cachedHostStore) merge(usableHosts []hosts.Host) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, host := range usableHosts {
		s.hosts[host.PublicKey] = host
	}
}

// clear empties the store. It must only be called while no migrations are in
// flight.
func (s *cachedHostStore) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.hosts)
}

// Addresses implements client.Store. It errors for hosts absent from the
// current batch's state — matching hosts.HostStore.Addresses — so a dial
// against an unknown host fails with a clear cause rather than a generic "no
// addresses found". The stored slices are never mutated (merge replaces
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
