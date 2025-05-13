package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/explorer"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

func runRootCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, network *consensus.Network, genesis types.Block, log *zap.Logger) error {
	store, err := postgres.Connect(ctx, cfg.Database, log.Named("postgres"))
	if err != nil {
		return fmt.Errorf("failed to connect to postgres database: %w", err)
	}
	defer store.Close()

	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(cfg.Directory, "consensus.db"))
	if err != nil {
		return fmt.Errorf("failed to open consensus database: %w", err)
	}
	defer bdb.Close()

	dbstore, tipState, err := chain.NewDBStore(bdb, network, genesis, chain.NewZapMigrationLogger(log.Named("chaindb")))
	if err != nil {
		return fmt.Errorf("failed to create chain store: %w", err)
	}
	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))

	wm, err := wallet.NewSingleAddressWallet(walletKey, cm, store, wallet.WithLogger(log.Named("wallet")), wallet.WithReservationDuration(3*time.Hour))
	if err != nil {
		return fmt.Errorf("failed to create wallet: %w", err)
	}
	defer wm.Close()

	syncerListener, err := net.Listen("tcp", cfg.Syncer.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on syncer address: %w", err)
	}
	defer syncerListener.Close()

	syncerAddr := syncerListener.Addr().String()
	if cfg.Syncer.EnableUPnP {
		_, portStr, _ := net.SplitHostPort(cfg.Syncer.Address)
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return fmt.Errorf("failed to parse syncer port: %w", err)
		}

		ip, err := setupUPNP(context.Background(), uint16(port), log)
		if err != nil {
			log.Warn("failed to set up UPnP", zap.Error(err))
		} else {
			syncerAddr = net.JoinHostPort(ip, portStr)
		}
	}
	// peers will reject us if our hostname is empty or unspecified, so use loopback
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}

	for _, peer := range cfg.Syncer.Peers {
		if err := store.AddPeer(peer); err != nil {
			log.Warn("failed to add peer", zap.String("address", peer), zap.Error(err))
		}
	}

	log.Debug("starting syncer", zap.String("syncer address", syncerAddr))
	s := syncer.New(syncerListener, cm, store, gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}, syncer.WithLogger(log.Named("syncer")))
	go s.Run()
	defer s.Close()

	hm, err := hosts.NewManager(s, store, hosts.WithLogger(log.Named("hosts")))
	if err != nil {
		return fmt.Errorf("failed to create host manager: %w", err)
	}
	defer hm.Close()

	funder := accounts.NewFunder(cm, wm)
	am := accounts.NewManager(store, funder, accounts.WithLogger(log.Named("accounts")))
	defer am.Close()

	cf := contracts.NewSiamuxDialer(cm, wm, walletKey)
	contracts, err := contracts.NewManager(walletKey.PublicKey(), am, cm, cf, hm, store, s, wm, contracts.WithLogger(log.Named("contracts")))
	if err != nil {
		return fmt.Errorf("failed to create contracts manager: %w", err)
	}
	defer contracts.Close()

	subscriber, err := subscriber.New(cm, hm, contracts, wm, store, subscriber.WithLogger(log.Named("subscriber")))
	if err != nil {
		return fmt.Errorf("failed to create subscriber: %w", err)
	}
	defer subscriber.Close()

	httpListener, err := startLocalhostListener(cfg.AdminAPI.Address, log.Named("listener"))
	if err != nil {
		return fmt.Errorf("failed to listen on http address: %w", err)
	}
	defer httpListener.Close()

	apiOpts := []api.ServerOption{
		api.WithLogger(log.Named("api")),
	}

	var e *explorer.Explorer
	if cfg.Explorer.Enabled {
		e = explorer.New(cfg.Explorer.URL)
		apiOpts = append(apiOpts, api.WithExplorer(e))
	}

	pm, err := pins.NewManager(e, hm, store, pins.WithLogger(log.Named("pins")))
	if err != nil {
		return fmt.Errorf("failed to create pins manager: %w", err)
	}
	defer pm.Close()

	web := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.AdminAPI.Password)(api.NewServer(cm, contracts, s, wm, store, apiOpts...)),
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	defer web.Close()

	go func() {
		log.Debug("starting http server", zap.String("adminAddress", cfg.AdminAPI.Address), zap.String("applicationAddress", cfg.ApplicationAPI.Address))
		if err := web.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(err))
		}
	}()

	log.Info("node started", zap.String("http", httpListener.Addr().String()), zap.String("p2p", string(s.Addr())))
	<-ctx.Done()
	log.Info("shutdown signal received...attempting graceful shutdown...")

	// attempt to gracefully shut down the http server but allow another signal
	// to interrupt shutdown. That way, indexd behaves in a more Linux-like way
	// as expected by tools like Docker and Kubernetes which first try to
	// trigger a graceful shutdown with SIGTERM followed by a user-configurable
	// timeout after which they send a SIGKILL which causes the OS to kill the
	// process without the process being able to catch the signal itself. For
	// convenience, we allow the user to send a second SIGTERM to force similar
	// behavior as if SIGKILL was sent.
	shutdownCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := web.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("graceful shutdown failed", zap.Error(err))
	}
	select {
	case <-shutdownCtx.Done():
		log.Info("graceful shutdown was interrupted")
	default:
	}

	log.Info("...shutdown complete")
	return nil
}

func startLocalhostListener(listenAddr string, log *zap.Logger) (l net.Listener, err error) {
	addr, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse API address: %w", err)
	}

	// if the address is not localhost, listen on the address as-is
	if addr != "localhost" {
		return net.Listen("tcp", listenAddr)
	}

	// localhost fails on some new installs of Windows 11, so try a few
	// different addresses
	tryAddresses := []string{
		net.JoinHostPort("localhost", port), // original address
		net.JoinHostPort("127.0.0.1", port), // IPv4 loopback
		net.JoinHostPort("::1", port),       // IPv6 loopback
	}

	for _, addr := range tryAddresses {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			return
		}
		log.Debug("failed to listen on fallback address", zap.String("address", addr), zap.Error(err))
	}
	return
}
