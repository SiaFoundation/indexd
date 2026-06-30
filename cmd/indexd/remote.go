package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	adminapi "go.sia.tech/indexd/api/admin"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

// remoteJobBatchSize is the number of migration jobs a remote node fetches from
// the primary node per request.
const remoteJobBatchSize = 100

// remoteMigrationInterval is how long a remote node waits between passes once it
// has worked through all currently-unhealthy slabs.
const remoteMigrationInterval = time.Minute

// runRemoteCmd runs a remote worker that helps the primary indexd migrate
// unhealthy slabs. It holds no database connection: it fetches prepared
// migration jobs from the primary node's admin API, downloads and re-uploads the
// affected shards itself, and reports the results back for the primary to
// persist. Service accounts are funded on hosts by the primary node; the remote
// derives the same account keys from the shared recovery phrase and spends from
// those host-side accounts.
func runRemoteCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, log *zap.Logger) error {
	if cfg.Remote.Address == "" {
		return errors.New("remote.address must be set to the primary node's admin API URL")
	}

	primary := adminapi.NewClient(cfg.Remote.Address, cfg.Remote.Password)

	// note: the recovery phrase must match the primary node's so the derived
	// migration account key lines up with the host-side accounts the primary
	// funds. A mismatch is not fatal: the host accounts simply go unfunded and
	// surface as insufficient-balance errors in the logs.
	directory := newRemoteHostDirectory()
	hostClient := client.New(client.NewProvider(directory), log.Named("client"))
	defer hostClient.Close()

	am := newInMemoryServiceAccounts()
	migrationKey, integrityKey := slabs.DeriveAccountKeys(walletKey)

	slabOpts := []slabs.Option{
		slabs.WithLogger(log.Named("slabs")),
		slabs.WithIntegrityChecks(false),
		slabs.WithMigrations(true),
	}
	if cfg.Slabs.MigrationWorkers > 0 {
		slabOpts = append(slabOpts, slabs.WithNumMigrationGoroutines(cfg.Slabs.MigrationWorkers))
	}

	alerter := alerts.NewManager()
	sm := slabs.NewRemoteManager(am, alwaysUsableHostManager{}, hostClient, alerter, migrationKey, integrityKey, slabOpts...)
	defer sm.Close()

	workers := cfg.Slabs.MigrationWorkers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	log.Info("remote node started", zap.String("primary", cfg.Remote.Address), zap.Int("workers", workers))
	runRemoteMigrationLoop(ctx, primary, directory, sm, workers, log.Named("migrations"))
	log.Info("shutting down")
	return nil
}

// runRemoteMigrationLoop repeatedly works through all currently-unhealthy slabs,
// pausing between passes.
func runRemoteMigrationLoop(ctx context.Context, primary *adminapi.Client, directory *remoteHostDirectory, sm *slabs.SlabManager, workers int, log *zap.Logger) {
	ticker := time.NewTicker(remoteMigrationInterval)
	defer ticker.Stop()
	for {
		if err := runRemoteMigrationPass(ctx, primary, directory, sm, workers, log); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("migration pass failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runRemoteMigrationPass pages through all unhealthy slabs the primary node has,
// fetching prepared jobs, executing them and reporting the results back.
func runRemoteMigrationPass(ctx context.Context, primary *adminapi.Client, directory *remoteHostDirectory, sm *slabs.SlabManager, workers int, log *zap.Logger) error {
	var cursor int64
	for {
		resp, err := primary.MigrationJobs(ctx, cursor, remoteJobBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch migration jobs: %w", err)
		}
		log.Debug("fetched migration jobs", zap.Int("jobs", len(resp.Jobs)), zap.Int64("cursor", cursor), zap.Int64("nextCursor", resp.NextCursor))

		results := make([]slabs.MigrationResult, len(resp.Jobs))
		var wg sync.WaitGroup
		sema := make(chan struct{}, workers)
		for i, job := range resp.Jobs {
			directory.learn(job.Hosts)
			select {
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			case sema <- struct{}{}:
			}
			wg.Add(1)
			go func(i int, job slabs.MigrationJob) {
				defer wg.Done()
				defer func() { <-sema }()
				results[i] = sm.MigrateJob(ctx, job)
			}(i, job)
		}
		wg.Wait()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if len(results) > 0 {
			if err := primary.ApplyMigrationResults(ctx, results); err != nil {
				return fmt.Errorf("failed to report migration results: %w", err)
			}
		}

		// a next cursor of 0 means there are no more unhealthy slabs
		if resp.NextCursor == 0 {
			return nil
		}
		cursor = resp.NextCursor
	}
}
