package slabs

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	// remoteMigrationInterval is how long a remote node waits before polling
	// the primary again after a pass that found nothing to migrate or failed.
	remoteMigrationInterval = time.Minute

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
	resultReportInterval = 10 * time.Second
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

// WithRemoteInterval sets how long a RemoteMigrator waits before polling the
// primary again after a pass that found nothing to migrate or failed. The
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

// Run migrates the primary node's unhealthy slabs until the context is
// cancelled. Passes over the unhealthy slabs run back-to-back on a shared
// pool of workers; after a pass that found nothing to migrate or failed, the
// worker waits the configured interval before polling the primary again. If
// reporting results fails, in-flight migrations are aborted and the worker
// waits the configured interval before starting over.
func (rm *RemoteMigrator) Run(ctx context.Context) {
	store := newCachedHostStore()
	hostClient := client.New(client.NewProvider(store), rm.log.Named("client"))
	defer hostClient.Close()
	migrator := NewMigrator(hostClient, rm.migrationAccountKey, rm.log)

	for {
		err := rm.runMigrations(ctx, store, migrator)
		if ctx.Err() != nil {
			return
		}
		rm.log.Error("migrations aborted", zap.Error(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(rm.interval):
		}
	}
}

// queuePass pages through all unhealthy slabs the primary node has and queues
// them for the workers. It returns once the last batch of the pass is queued,
// without waiting for its migrations to finish, so the next pass overlaps
// with the tail of this one.
func (rm *RemoteMigrator) queuePass(ctx context.Context, store *cachedHostStore, batchSize int, slabCh chan<- migrationJob) (queued int, _ error) {
	var cursor int64
	for {
		rm.log.Debug("fetching migration batch", zap.Int64("cursor", cursor), zap.Int("limit", batchSize))
		fetchCtx, cancel := context.WithTimeout(ctx, remoteRequestTimeout)
		batch, err := rm.primary.MigrationBatch(fetchCtx, cursor, batchSize)
		cancel()
		if err != nil {
			return queued, fmt.Errorf("failed to fetch migration batch: %w", err)
		}
		rm.log.Debug("fetched migration batch", zap.Int("slabs", len(batch.Slabs)), zap.Int64("cursor", cursor), zap.Int64("nextCursor", batch.NextCursor))

		if len(batch.Slabs) > 0 {
			bh := store.acquire(batch.State.Hosts, len(batch.Slabs))
			for i, slab := range batch.Slabs {
				select {
				case slabCh <- migrationJob{slab: slab, state: batch.State, hosts: bh}:
					queued++
				case <-ctx.Done():
					bh.done(len(batch.Slabs) - i)
					return queued, ctx.Err()
				}
			}
		}

		if batch.NextCursor == 0 {
			return queued, nil
		}
		cursor = batch.NextCursor
	}
}

func (rm *RemoteMigrator) runMigrations(ctx context.Context, store *cachedHostStore, migrator *Migrator) error {
	batchSize := migrationSlabsPerWorker * rm.workers

	// runCtx aborts the producer and in-flight migrations when reporting
	// results fails: without persistence any further migration is wasted
	// work.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// fetching a batch claims its slabs on the primary for the repair
	// backoff, so the job queue is kept shallow: the producer claims the
	// next batch only once the workers are close to running dry, keeping
	// the time a claimed slab sits queued well below the claim window.
	slabCh := make(chan migrationJob, rm.workers)

	resultCh := make(chan MigrationResult, batchSize)
	var wg sync.WaitGroup
	for range rm.workers {
		wg.Go(func() {
			for job := range slabCh {
				res, attempted := migrator.MigrateSlab(runCtx, job.slab, job.state)
				job.hosts.done(1)
				if attempted {
					resultCh <- res
				}
			}
		})
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	go func() {
		defer close(slabCh)
		for {
			queued, err := rm.queuePass(runCtx, store, batchSize, slabCh)
			if runCtx.Err() != nil {
				return
			} else if err != nil {
				rm.log.Error("migration pass failed", zap.Error(err))
			} else if queued > 0 {
				continue
			}
			select {
			case <-runCtx.Done():
				return
			case <-time.After(rm.interval):
			}
		}
	}()

	// collect results as migrations complete and report them to the primary
	// in groups. Reports run on their own goroutine — at most one in flight —
	// so a slow report never stalls result collection and with it the
	// workers.
	var reportErr error
	reporting := false
	reportDoneCh := make(chan error, 1)
	pending := make([]MigrationResult, 0, resultReportGroupSize)

	startReport := func() {
		if len(pending) == 0 || reporting || reportErr != nil {
			return
		}
		group := pending
		pending = nil
		reporting = true
		go func() {
			reportDoneCh <- rm.reportResults(group)
		}()
	}
	finishReport := func(err error) {
		reporting = false
		if err != nil && reportErr == nil {
			reportErr = fmt.Errorf("failed to report migration results: %w", err)
			cancelRun()
		}
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
			pending = append(pending, res)
			if len(pending) >= resultReportGroupSize {
				startReport()
			}
		case err := <-reportDoneCh:
			finishReport(err)
			startReport()
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
		}
	}

	if reportErr != nil {
		return reportErr
	}
	return ctx.Err()
}

// reportResults reports a batch of migration results to the primary node. It
// deliberately runs on its own context rather than the run context: the
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

// cachedHostStore is the client.Store a RemoteMigrator backs its RHP
// client with. Every fetched batch acquires its state's hosts and
// releases them once all of its slabs are done, so a host never vanishes
// from under an in-flight migration while hosts no in-flight batch
// references are dropped.
type cachedHostStore struct {
	mu    sync.RWMutex
	hosts map[types.PublicKey]cachedHost
}

type cachedHost struct {
	hosts.Host
	refs int
}

type batchHosts struct {
	store     *cachedHostStore
	hosts     []hosts.Host
	remaining atomic.Int64
}

type migrationJob struct {
	slab  Slab
	state MigrationState
	hosts *batchHosts
}

func newCachedHostStore() *cachedHostStore {
	return &cachedHostStore{
		hosts: make(map[types.PublicKey]cachedHost),
	}
}

func (b *batchHosts) done(n int) {
	if b.remaining.Add(-int64(n)) == 0 {
		b.store.release(b.hosts)
	}
}

// acquire upserts the usable hosts carried by a migration batch's state and
// holds them until all of the batch's slabs are done. Hosts absent from the
// batch are deliberately kept while referenced: migrations from earlier
// batches may still be reading from them.
func (s *cachedHostStore) acquire(usableHosts []hosts.Host, slabs int) *batchHosts {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, host := range usableHosts {
		h := s.hosts[host.PublicKey]
		h.Host = host
		h.refs++
		s.hosts[host.PublicKey] = h
	}
	b := &batchHosts{store: s, hosts: usableHosts}
	b.remaining.Store(int64(slabs))
	return b
}

func (s *cachedHostStore) release(usableHosts []hosts.Host) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, host := range usableHosts {
		h := s.hosts[host.PublicKey]
		h.refs--
		if h.refs <= 0 {
			delete(s.hosts, host.PublicKey)
		} else {
			s.hosts[host.PublicKey] = h
		}
	}
}

// Addresses implements client.Store.
// The stored slices are never mutated, so the slice is returned directly.
func (s *cachedHostStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.hosts[hostKey]
	if !ok {
		return nil, hosts.ErrNotFound
	}
	return h.Addresses, nil
}

// Usable implements client.Store.
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
