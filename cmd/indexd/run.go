package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
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
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/explorer"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/jape"
	"go.sia.tech/web/indexd"
	"go.uber.org/zap"
)

func runRootCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, network *consensus.Network, genesis types.Block, log *zap.Logger) error {
	store, err := postgres.NewStore(ctx, cfg.Database, contracts.DefaultMaintenanceSettings, hosts.DefaultUsabilitySettings, log.Named("postgres"))
	if err != nil {
		return fmt.Errorf("failed to create postgres store: %w", err)
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

	s := syncer.New(syncerListener, cm, store, gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}, syncer.WithLogger(log.Named("syncer").WithOptions(zap.IncreaseLevel(zap.InfoLevel)))) // increase to info to reduce log spam
	go s.Run()
	defer s.Close()

	wm, err := wallet.NewSingleAddressWallet(walletKey, cm, store, s, wallet.WithLogger(log.Named("wallet")), wallet.WithReservationDuration(3*time.Hour))
	if err != nil {
		return fmt.Errorf("failed to create wallet: %w", err)
	}
	defer wm.Close()

	locator, err := geoip.NewMaxMindLocator("")
	if err != nil {
		return fmt.Errorf("failed to create MaxMind locator: %w", err)
	}

	hm, err := hosts.NewManager(s, locator, store, hosts.WithLogger(log.Named("hosts")))
	if err != nil {
		return fmt.Errorf("failed to create host manager: %w", err)
	}
	defer hm.Close()

	signer := contracts.NewFormContractSigner(wm, walletKey)
	dialer := client.NewDialer(cm, signer, store, log)
	am := accounts.NewManager(store, accounts.NewFunder(dialer), accounts.WithLogger(log.Named("accounts")))
	defer am.Close()

	contracts, err := contracts.NewManager(walletKey, am, cm, store, dialer, hm, s, wm, contracts.WithLogger(log.Named("contracts")))
	if err != nil {
		return fmt.Errorf("failed to create contracts manager: %w", err)
	}
	defer contracts.Close()

	alerter := alerts.NewManager()
	slabs, err := slabs.NewManager(am, contracts, hm, store, dialer, alerter, keys.DerivePrivateKey(walletKey, "migration"), keys.DerivePrivateKey(walletKey, "integrity"), slabs.WithLogger(log.Named("slabs")))
	if err != nil {
		return fmt.Errorf("failed to create slabs manager: %w", err)
	}
	defer slabs.Close()

	subscriber, err := subscriber.New(cm, hm, contracts, wm, store, subscriber.WithLogger(log.Named("subscriber")))
	if err != nil {
		return fmt.Errorf("failed to create subscriber: %w", err)
	}
	defer subscriber.Close()

	adminAPIListener, err := startLocalhostListener(cfg.AdminAPI.Address, log.Named("api.admin.listener"))
	if err != nil {
		return fmt.Errorf("failed to start admin API listener: %w", err)
	}
	defer adminAPIListener.Close()

	adminAPIOpts := []admin.Option{
		admin.WithLogger(log.Named("api.admin")),
	}

	if cfg.Debug {
		adminAPIOpts = append(adminAPIOpts, admin.WithDebug())
	}

	var e *explorer.Explorer
	if cfg.Explorer.Enabled {
		e = explorer.New(cfg.Explorer.URL)
		adminAPIOpts = append(adminAPIOpts, admin.WithExplorer(e))
	}

	pm, err := pins.NewManager(e, hm, store, pins.WithLogger(log.Named("pins")))
	if err != nil {
		return fmt.Errorf("failed to create pins manager: %w", err)
	}
	defer pm.Close()

	adminAPI := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.AdminAPI.Password)(admin.NewAPI(cm, am, contracts, hm, pm, s, wm, store, alerter, adminAPIOpts...)),
			ui:  indexd.Handler(),
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	defer adminAPI.Close()

	go func() {
		log.Debug("starting admin API", zap.String("adminAddress", cfg.AdminAPI.Address))
		if err := adminAPI.Serve(adminAPIListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve admin API", zap.Error(err))
		}
	}()

	appAPIListener, err := startLocalhostListener(cfg.ApplicationAPI.Address, log.Named("api.application.listener"))
	if err != nil {
		return fmt.Errorf("failed to start application API listener: %w", err)
	}
	defer appAPIListener.Close()

	appAPIOpts := []app.Option{
		app.WithLogger(log.Named("api.application")),
	}

	advertiseURL := cfg.ApplicationAPI.AdvertiseURL
	if advertiseURL == "" {
		host, port, _ := net.SplitHostPort(appAPIListener.Addr().String())
		if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
			advertiseURL = "http://" + net.JoinHostPort("127.0.0.1", port)
		} else {
			advertiseURL = "http://" + net.JoinHostPort(host, port)
		}
	}

	appHandler, err := app.NewAPI(advertiseURL, store, am, contracts, slabs, appAPIOpts...)
	if err != nil {
		return fmt.Errorf("failed to create application API: %w", err)
	}

	appAPI := http.Server{
		Handler:      appHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	defer appAPI.Close()

	go func() {
		log.Debug("starting application API", zap.String("appAddress", advertiseURL), zap.String("applicationAddress", cfg.ApplicationAPI.Address))
		if err := appAPI.Serve(appAPIListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve application API", zap.Error(err))
		}
	}()

	// open the web UI if enabled
	if cfg.AutoOpenWebUI {
		time.Sleep(time.Millisecond) // give the web server a chance to start
		_, port, err := net.SplitHostPort(adminAPIListener.Addr().String())
		if err != nil {
			log.Debug("failed to parse API address", zap.Error(err))
		} else if err := openBrowser(fmt.Sprintf("http://127.0.0.1:%s", port)); err != nil {
			log.Debug("failed to open browser", zap.Error(err))
		}
	}

	log.Info("node started", zap.Stringer("admin", adminAPIListener.Addr()), zap.Stringer("application", appAPIListener.Addr()), zap.String("p2p", string(s.Addr())))
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
	if err := appAPI.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("graceful shutdown failed", zap.Error(err))
	}
	if err := adminAPI.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return fmt.Errorf("unsupported platform %q", runtime.GOOS)
	}
}
