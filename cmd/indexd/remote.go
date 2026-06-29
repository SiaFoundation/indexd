package main

import (
	"context"
	"errors"
	"fmt"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	client "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

// runRemoteCmd runs a remote worker that connects to an existing indexd's
// database and only runs slab migrations. It serves no API and runs no syncer,
// consensus, subscriber, wallet or contract maintenance. Service accounts are
// kept funded on hosts by the primary node; the remote derives the same account
// keys from the shared recovery phrase and spends from those host-side accounts.
func runRemoteCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, log *zap.Logger) error {
	store, err := postgres.NewStore(ctx, cfg.Database, contracts.DefaultMaintenanceSettings, hosts.DefaultUsabilitySettings, log.Named("postgres"))
	if err != nil {
		return fmt.Errorf("failed to create postgres store: %w", err)
	}
	defer store.Close()

	// ensure the configured recovery phrase matches the primary node's wallet
	// so the derived migration account key lines up with the host-side accounts
	// the primary funds.
	walletHash := types.HashBytes(walletKey[:])
	if err := store.VerifyWalletKey(walletHash); errors.Is(err, wallet.ErrDifferentSeed) {
		return errors.New("wallet seed does not match the primary node's database")
	} else if err != nil {
		return fmt.Errorf("failed to verify wallet key: %w", err)
	}

	alerter := alerts.NewManager()

	am, err := accounts.NewManager(store, accounts.WithLogger(log.Named("accounts")))
	if err != nil {
		return fmt.Errorf("failed to create accounts manager: %w", err)
	}
	defer am.Close()

	hostClient := client.New(client.NewProvider(hosts.NewHostStore(store)), log.Named("client"))
	defer hostClient.Close()

	cm := &remoteContractManager{store: store}
	hm := &remoteHostManager{store: store, client: hostClient}

	migrationKey, integrityKey := slabs.DeriveAccountKeys(walletKey)

	slabOpts := []slabs.Option{
		slabs.WithLogger(log.Named("slabs")),
		slabs.WithIntegrityChecks(false),
		slabs.WithMigrations(true),
	}
	if cfg.Slabs.MigrationWorkers > 0 {
		slabOpts = append(slabOpts, slabs.WithNumMigrationGoroutines(cfg.Slabs.MigrationWorkers))
	}

	sm, err := slabs.NewManager(am, cm, hm, store, hostClient, alerter, migrationKey, integrityKey, slabOpts...)
	if err != nil {
		return fmt.Errorf("failed to create slabs manager: %w", err)
	}
	defer sm.Close()

	log.Info("remote node started")
	<-ctx.Done()
	log.Info("shutting down")
	return nil
}

// remoteContractManager satisfies slabs.ContractManager using only the shared
// database. It does not run any contract maintenance; account funding is
// performed proactively by the primary node.
type remoteContractManager struct {
	store *postgres.Store
}

// HealthyContracts returns all revisable, good contracts from the store.
func (m *remoteContractManager) HealthyContracts() ([]contracts.Contract, error) {
	return contracts.HealthyContracts(m.store)
}

// TriggerAccountRefill marks the account for funding in the shared database.
// The primary node's funding loop picks it up; the remote does no funding
// itself.
func (m *remoteContractManager) TriggerAccountRefill(ctx context.Context, hostKey types.PublicKey, account proto.Account) error {
	if err := m.store.ScheduleAccountForFunding(hostKey, account); err != nil {
		return fmt.Errorf("failed to schedule account for funding: %w", err)
	}
	return nil
}

// remoteHostManager satisfies slabs.HostManager by delegating to hosts.Usable,
// so the remote shares the exact usability logic of a full node.
type remoteHostManager struct {
	store  *postgres.Store
	client *client.Client
}

// Usable reports whether the host is usable for slab operations.
func (m *remoteHostManager) Usable(ctx context.Context, hostKey types.PublicKey) (bool, error) {
	return hosts.Usable(ctx, m.store, m.client, hostKey)
}
