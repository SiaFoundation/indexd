package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	adminapi "go.sia.tech/indexd/api/admin"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

// cachedHostStore is the client.Store a remote migration worker backs its RHP
// client with. A remote node has no database; the addresses of the hosts it
// must reach are supplied by the primary node with each batch of migration
// jobs. It makes no judgement about host quality — the primary node vetted the
// hosts — so every known host is reported usable.
type cachedHostStore struct {
	mu    sync.RWMutex
	addrs map[types.PublicKey][]chain.NetAddress
}

func newCachedHostStore() *cachedHostStore {
	return &cachedHostStore{addrs: make(map[types.PublicKey][]chain.NetAddress)}
}

// learn records the connection info carried by a batch of migration jobs.
func (s *cachedHostStore) learn(conns []slabs.HostConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range conns {
		if len(c.Addresses) > 0 {
			s.addrs[c.PublicKey] = c.Addresses
		}
	}
}

// reset clears the store. It is called when no jobs are in flight (at the start
// of a migration pass) so entries for hosts that no longer appear in any job
// don't accumulate for the lifetime of the process.
func (s *cachedHostStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.addrs)
}

// Addresses implements client.Store.
func (s *cachedHostStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	addrs, ok := s.addrs[hostKey]
	if !ok {
		return nil, fmt.Errorf("no addresses known for host %v", hostKey)
	}
	return addrs, nil
}

// Usable implements client.Store. The primary node only ever sends hosts it has
// already vetted, so any host the store has addresses for is usable; hosts it
// doesn't know (e.g. pruned download sources) are filtered so the client
// doesn't pick candidates it cannot dial.
func (s *cachedHostStore) Usable(hostKey types.PublicKey) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.addrs[hostKey]
	return ok, nil
}

// UsableHosts implements client.Store. It is never consulted on the migration
// path (which works from the explicit candidate list in each job) and exists
// only to satisfy the interface.
func (s *cachedHostStore) UsableHosts() ([]hosts.HostInfo, error) {
	return nil, nil
}

// remoteJobBatchSize is the number of migration jobs a remote node fetches from
// the primary node per request.
const remoteJobBatchSize = 100

// remoteMigrationInterval is how long a remote node waits between passes once it
// has worked through all currently-unhealthy slabs.
const remoteMigrationInterval = time.Minute

// resultReportAttempts and resultReportRetryInterval control how often a batch
// of migration results is retried before the pass is abandoned. Results
// represent completed downloads and uploads, so dropping them orphans the
// uploaded sectors and forgets discovered lost sectors.
const (
	resultReportAttempts      = 3
	resultReportRetryInterval = 10 * time.Second

	// resultFlushTimeout bounds the best-effort report of completed results
	// during shutdown.
	resultFlushTimeout = 30 * time.Second
)

// runRemoteCmd runs a remote worker that helps the primary indexd migrate
// unhealthy slabs. It holds no database connection: it fetches prepared
// migration jobs from the primary node's admin API, downloads and re-uploads the
// affected shards itself, and reports the results back for the primary to
// persist. Service accounts are funded on hosts by the primary node; the remote
// derives the same account keys from the shared recovery phrase and spends from
// those host-side accounts.
func runRemoteCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, log *zap.Logger) error {
	primary := adminapi.NewClient(cfg.Remote.Address, cfg.Remote.Password)

	// note: the recovery phrase must match the primary node's so the derived
	// migration account key lines up with the host-side accounts the primary
	// funds. A mismatch is not fatal: the host accounts simply go unfunded and
	// surface as insufficient-balance errors in the logs.
	store := newCachedHostStore()
	hostClient := client.New(client.NewProvider(store), log.Named("client"))
	defer hostClient.Close()

	migrationKey, _ := slabs.DeriveAccountKeys(walletKey)
	migrator := slabs.NewMigrator(hostClient, migrationKey, log.Named("migrations"))

	workers := cfg.Slabs.MigrationWorkers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	log.Info("remote node started", zap.String("primary", cfg.Remote.Address), zap.Int("workers", workers))
	runRemoteMigrationLoop(ctx, primary, store, migrator, workers, log.Named("migrations"))
	log.Info("shutting down")
	return nil
}

// runRemoteMigrationLoop repeatedly works through all currently-unhealthy slabs,
// pausing between passes.
func runRemoteMigrationLoop(ctx context.Context, primary *adminapi.Client, store *cachedHostStore, migrator *slabs.Migrator, workers int, log *zap.Logger) {
	for {
		if err := runRemoteMigrationPass(ctx, primary, store, migrator, workers, log); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("migration pass failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(remoteMigrationInterval):
		}
	}
}

// runRemoteMigrationPass pages through all unhealthy slabs the primary node has,
// fetching prepared jobs, executing them and reporting the results back.
func runRemoteMigrationPass(ctx context.Context, primary *adminapi.Client, store *cachedHostStore, migrator *slabs.Migrator, workers int, log *zap.Logger) error {
	// the host store only needs the hosts referenced by the current pass;
	// reset it so entries for long-pruned hosts don't accumulate forever.
	store.reset()

	var cursor int64
	for {
		resp, err := primary.MigrationJobs(ctx, cursor, remoteJobBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch migration jobs: %w", err)
		}
		log.Debug("fetched migration jobs", zap.Int("jobs", len(resp.Jobs)), zap.Int64("cursor", cursor), zap.Int64("nextCursor", resp.NextCursor))
		store.learn(resp.Hosts)

		var mu sync.Mutex
		results := make([]slabs.MigrationResult, 0, len(resp.Jobs))
		var wg sync.WaitGroup
		sema := make(chan struct{}, workers)
	dispatch:
		for _, job := range resp.Jobs {
			select {
			case <-ctx.Done():
				break dispatch
			case sema <- struct{}{}:
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sema }()
				res := migrator.MigrateJob(ctx, job)
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			}()
		}
		wg.Wait()

		if ctx.Err() != nil {
			// best-effort flush of completed work before shutting down so
			// uploaded sectors are recorded rather than orphaned.
			flushCtx, cancel := context.WithTimeout(context.Background(), resultFlushTimeout)
			defer cancel()
			if err := reportMigrationResults(flushCtx, primary, results, log); err != nil {
				log.Warn("failed to flush migration results during shutdown", zap.Error(err))
			}
			return ctx.Err()
		}

		if err := reportMigrationResults(ctx, primary, results, log); err != nil {
			return fmt.Errorf("failed to report migration results: %w", err)
		}

		// a next cursor of 0 means there are no more unhealthy slabs
		if resp.NextCursor == 0 {
			return nil
		}
		cursor = resp.NextCursor
	}
}

// reportMigrationResults reports a batch of migration results to the primary
// node, retrying transient failures. The results represent completed downloads
// and uploads: dropping them would orphan the uploaded sectors on their new
// hosts and lose any lost-sector discoveries, so a report is only abandoned
// after repeated failures.
func reportMigrationResults(ctx context.Context, primary *adminapi.Client, results []slabs.MigrationResult, log *zap.Logger) error {
	if len(results) == 0 {
		return nil
	}
	var err error
	for attempt := 1; ; attempt++ {
		if err = primary.ApplyMigrationResults(ctx, results); err == nil {
			return nil
		} else if attempt == resultReportAttempts {
			return err
		}
		log.Warn("failed to report migration results, retrying", zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(resultReportRetryInterval):
		}
	}
}
