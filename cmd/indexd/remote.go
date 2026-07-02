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
// client with. It gets refreshed on ever batch fetched for migration.
type cachedHostStore struct {
	mu    sync.RWMutex
	hosts map[types.PublicKey]hosts.Host
}

func newCachedHostStore() *cachedHostStore {
	return &cachedHostStore{hosts: make(map[types.PublicKey]hosts.Host)}
}

// update records the connection info carried by a batch of migration jobs.
func (s *cachedHostStore) update(usableHosts []hosts.Host) {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.hosts)
	for _, host := range usableHosts {
		s.hosts[host.PublicKey] = host
	}
}

// Addresses implements client.Store.
func (s *cachedHostStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var addrs []chain.NetAddress
	for _, addr := range s.hosts[hostKey].Addresses {
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// Usable implements client.Store.
func (s *cachedHostStore) Usable(hostKey types.PublicKey) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.hosts[hostKey]
	return ok, nil
}

// UsableHosts implements client.Store. It is never consulted on the migration
// path (which works from the explicit candidate list in each job) and exists
// only to satisfy the interface.
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
			GoodForUpload: h.IsGood() && h.StuckSince.IsZero() && h.Settings.RemainingStorage > 0, // match primary node's definition
		})
	}
	return infos, nil
}

// remoteJobBatchSize is the number of migration jobs a remote node fetches from
// the primary node per request.
const remoteJobBatchSize = 100

// remoteMigrationInterval is how long a remote node waits between passes once it
// has worked through all currently-unhealthy slabs. maxRemoteBackoff caps the
// exponential backoff applied when consecutive passes fail every recovery,
// which points at a systemic problem such as a recovery phrase that doesn't
// match the primary's.
const (
	remoteMigrationInterval = time.Minute
	maxRemoteBackoff        = 30 * time.Minute
)

const (
	// resultReportTimeout bounds the single report of a batch's results. The
	// report runs on its own context so a shutdown mid-report doesn't abort
	// it: the results represent completed downloads and uploads, and dropping
	// them orphans the uploaded sectors and forgets discovered lost sectors.
	resultReportTimeout = time.Minute

	// remoteRequestTimeout bounds a single request to the primary so a
	// half-open connection cannot stall the worker indefinitely.
	remoteRequestTimeout = 5 * time.Minute
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
// pausing between passes. When every job of a pass fails recovery the pause
// grows exponentially: a worker that can never succeed (e.g. its recovery
// phrase doesn't match the primary's, so its account is unfunded) would
// otherwise claim and burn a fresh batch of slabs every interval, parking them
// for the claim duration with nothing to show.
func runRemoteMigrationLoop(ctx context.Context, primary *adminapi.Client, store *cachedHostStore, migrator *slabs.Migrator, workers int, log *zap.Logger) {
	barrenPasses := 0
	for {
		executed, recovered, err := runRemoteMigrationPass(ctx, primary, store, migrator, workers, log)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("migration pass failed", zap.Error(err))
		}
		if executed > 0 && recovered == 0 {
			barrenPasses++
		} else if executed > 0 {
			barrenPasses = 0
		}

		interval := remoteMigrationInterval
		if barrenPasses > 0 {
			interval = min(remoteMigrationInterval<<barrenPasses, maxRemoteBackoff)
			log.Warn("every migration of the pass failed recovery, backing off",
				zap.Int("consecutivePasses", barrenPasses), zap.Duration("interval", interval))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// runRemoteMigrationPass pages through all unhealthy slabs the primary node
// has, determining which of their sectors to migrate from the state shipped
// with each batch, executing the migrations and reporting the results back. It
// returns how many migrations were attempted and how many of them recovered
// their slab's shards.
func runRemoteMigrationPass(ctx context.Context, primary *adminapi.Client, store *cachedHostStore, migrator *slabs.Migrator, workers int, log *zap.Logger) (executed, recovered int, _ error) {
	var cursor int64
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, remoteRequestTimeout)
		batch, err := primary.MigrationBatch(fetchCtx, cursor, remoteJobBatchSize)
		cancel()
		if err != nil {
			return executed, recovered, fmt.Errorf("failed to fetch migration batch: %w", err)
		}
		log.Debug("fetched migration batch", zap.Int("slabs", len(batch.Slabs)), zap.Int64("cursor", cursor), zap.Int64("nextCursor", batch.NextCursor))
		store.update(batch.State.Hosts)

		var mu sync.Mutex
		results := make([]slabs.MigrationResult, 0, len(batch.Slabs))
		var wg sync.WaitGroup
		sema := make(chan struct{}, workers)
	dispatch:
		for _, slab := range batch.Slabs {
			select {
			case <-ctx.Done():
				break dispatch
			case sema <- struct{}{}:
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sema }()
				if res, attempted := migrator.MigrateSlab(ctx, slab, batch.State); attempted {
					mu.Lock()
					results = append(results, res)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		for _, res := range results {
			executed++
			if res.Recovered {
				recovered++
			}
		}

		if err := reportMigrationResults(primary, results); err != nil {
			return executed, recovered, fmt.Errorf("failed to report migration results: %w", err)
		}
		if ctx.Err() != nil {
			return executed, recovered, ctx.Err()
		}

		// a next cursor of 0 means there are no more unhealthy slabs
		if batch.NextCursor == 0 {
			return executed, recovered, nil
		}
		cursor = batch.NextCursor
	}
}

// reportMigrationResults reports a batch of migration results to the primary
// node. It deliberately runs on its own context rather than the pass context:
// the results represent completed downloads and uploads, so a shutdown that
// interrupts the report would orphan the uploaded sectors on their new hosts
// and lose any lost-sector discoveries. The batch is not retried on failure;
// the affected slabs are simply repaired again once their claim expires.
func reportMigrationResults(primary *adminapi.Client, results []slabs.MigrationResult) error {
	if len(results) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), resultReportTimeout)
	defer cancel()
	return primary.ApplyMigrationResults(ctx, results)
}
