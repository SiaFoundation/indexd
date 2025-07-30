package testutils

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/slabs"

	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/explorer"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/jape"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// DefaultHostname is the default hostname used for the application API.
	DefaultHostname = "indexer.sia.tech"
)

var (
	// MaintenanceSettings is the default maintenance settings used for testing.
	MaintenanceSettings = contracts.MaintenanceSettings{
		Enabled:         true,
		Period:          144,
		RenewWindow:     72,
		WantedContracts: 6,
	}
)

type (

	// Indexer is a test utility combining an indexer, an http client for the
	// indexer and useful helpers for testing.
	Indexer struct {
		*admin.Client
		App func(types.PrivateKey) *app.Client

		cm     *chain.Manager
		dialer *client.SiamuxDialer
		store  *postgres.Store
		syncer *Syncer
		wallet *wallet.SingleAddressWallet
	}

	// IndexerOpt is a functional option for configuring an indexer for testing
	IndexerOpt func(*indexerCfg)

	indexerCfg struct {
		maintenanceSettings contracts.MaintenanceSettings
		slabOpts            []slabs.Option
	}
)

func defaultIndexerCfg() *indexerCfg {
	return &indexerCfg{
		maintenanceSettings: MaintenanceSettings,
	}
}

// WithMaintenanceSettings allows for passing maintenance settings to the indexer
func WithMaintenanceSettings(ms contracts.MaintenanceSettings) IndexerOpt {
	return func(cfg *indexerCfg) {
		cfg.maintenanceSettings = ms
	}
}

// WithSlabOptions allows for passing slab options to the indexer
func WithSlabOptions(opts ...slabs.Option) IndexerOpt {
	return func(cfg *indexerCfg) {
		cfg.slabOpts = opts
	}
}

// NewIndexer creates a new indexer for testing that is automatically closed up
// after the test is finished.
func NewIndexer(t testing.TB, c *ConsensusNode, log *zap.Logger, opts ...IndexerOpt) *Indexer {
	cfg := defaultIndexerCfg()
	for _, opt := range opts {
		opt(cfg)
	}

	// prepare store
	store := NewDB(t, cfg.maintenanceSettings, log)

	s := NewSyncer(t, c.genesis.ID(), c.cm)

	walletKey := types.GeneratePrivateKey()
	wm := NewWallet(t, c, walletKey)

	syncer := NewSyncer(t, c.genesis.ID(), c.cm)
	hm, err := hosts.NewManager(syncer, store, hosts.WithLogger(log.Named("hosts")), hosts.WithScanFrequency(200*time.Millisecond), hosts.WithScanInterval(time.Second))
	if err != nil {
		t.Fatalf("failed to create host manager: %v", err)
	}

	signer := contracts.NewFormContractSigner(wm, walletKey)
	dialer := client.NewSiamuxDialer(c.cm, signer, store, log)
	am := accounts.NewManager(store, accounts.NewFunder(dialer), accounts.WithLogger(log.Named("accounts")))

	contracts, err := contracts.NewManager(walletKey, am, c.cm, store, dialer, hm, s, wm, contracts.WithLogger(log.Named("contracts")), contracts.WithMaintenanceFrequency(200*time.Millisecond), contracts.WithDisabledCIDRChecks())
	if err != nil {
		t.Fatalf("failed to create contract manager: %v", err)
	}

	slabOpts := []slabs.Option{slabs.WithLogger(log.Named("slabs"))}
	slabOpts = append(slabOpts, cfg.slabOpts...)

	alerter := alerts.NewManager()
	slabs, err := slabs.NewManager(am, hm, store, dialer, alerter, keys.DeriveKey(walletKey, "migration"), keys.DeriveKey(walletKey, "integrity"), slabOpts...)
	if err != nil {
		t.Fatalf("failed to create slab manager: %v", err)
	}

	subscriber, err := subscriber.New(c.cm, hm, contracts, wm, store, subscriber.WithLogger(log.Named("subscriber")))
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	// sync subscriber
	syncFn := func() {
		t.Helper()
		if err := subscriber.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	c.addSyncFn(syncFn)

	adminAPIOpts := []admin.Option{
		admin.WithDebug(),
		admin.WithLogger(log.Named("api.admin")),
		admin.WithExplorer(explorer.New("https://api.siascan.com")),
	}

	password := hex.EncodeToString(frand.Bytes(16))
	adminAPI := http.Server{
		Handler: jape.BasicAuth(password)(admin.NewAPI(c.cm, contracts, hm, syncer, wm, store, adminAPIOpts...)),
	}

	adminListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start admin API listener: %v", err)
	}
	adminAPIAddr := fmt.Sprintf("http://%s", adminListener.Addr().String())

	go func() {
		if err := adminAPI.Serve(adminListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve admin API", zap.Error(err))
		}
	}()

	appAPIOpts := []app.Option{
		app.WithLogger(log.Named("api.application")),
	}

	appListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on application address: %v", err)
	}
	appAPIAddr := fmt.Sprintf("http://%s", appListener.Addr().String())

	parsedURL, err := url.Parse(appAPIAddr)
	if err != nil {
		t.Fatalf("failed to parse address %q: %v", appAPIAddr, err)
	}

	appAPI := http.Server{
		Handler: app.NewAPI(parsedURL.Hostname(), store, store, appAPIOpts...),
	}

	go func() {
		if err := appAPI.Serve(appListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve application API", zap.Error(err))
		}
	}()

	t.Cleanup(func() {
		if err := shutdownWithTimeout(appAPI.Shutdown); err != nil {
			t.Errorf("failed to shutdown application API: %v", err)
		}
		if err := shutdownWithTimeout(adminAPI.Shutdown); err != nil {
			t.Errorf("failed to shutdown admin API: %v", err)
		}
		if err := closeWithTimeout(syncer.Close); err != nil {
			t.Errorf("failed to close syncer: %v", err)
		}
		if err := closeWithTimeout(wm.Close); err != nil {
			t.Errorf("failed to close wallet: %v", err)
		}
		if err := closeWithTimeout(hm.Close); err != nil {
			t.Errorf("failed to close host manager: %v", err)
		}
		if err := closeWithTimeout(contracts.Close); err != nil {
			t.Errorf("failed to close contract manager: %v", err)
		}
		if err := closeWithTimeout(am.Close); err != nil {
			t.Errorf("failed to close account manager: %v", err)
		}
		if err := closeWithTimeout(slabs.Close); err != nil {
			t.Errorf("failed to close slabs manager: %v", err)
		}
		if err := closeWithTimeout(subscriber.Close); err != nil {
			t.Errorf("failed to close subscriber: %v", err)
		}
		if err := closeWithTimeout(store.Close); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	})

	return &Indexer{
		Client: admin.NewClient(adminAPIAddr, password),
		App: func(appKey types.PrivateKey) *app.Client {
			client, _ := app.NewClient(appAPIAddr, appKey)
			return client
		},

		cm:     c.cm,
		dialer: dialer,
		store:  store,
		syncer: syncer,
		wallet: wm,
	}
}

// HostClient returns a host client for the given host public key.
func (idx *Indexer) HostClient(t *testing.T, hk types.PublicKey) *client.HostClient {
	h, err := idx.store.Host(context.Background(), hk)
	if err != nil {
		t.Fatalf("failed to get host %s: %v", hk, err) // developer error
	}
	hc, err := idx.dialer.DialHost(context.Background(), hk, h.SiamuxAddr())
	if err != nil {
		t.Fatalf("failed to dial host %s: %v", hk, err) // developer error
	}
	return hc
}

// Store returns the underlying store.
func (idx *Indexer) Store() *postgres.Store {
	return idx.store
}

// Tip returns the current tip of the chain.
func (idx *Indexer) Tip() (types.ChainIndex, error) {
	return idx.cm.Tip(), nil
}

// WalletAddr returns the address of the wallet.
func (idx *Indexer) WalletAddr() types.Address {
	return idx.wallet.Address()
}

// closeWithTimeout is a helper which closes a resource and panics if it takes
// longer than 30 seconds.
func closeWithTimeout(closeFn func() error) error {
	closed := make(chan struct{})
	defer close(closed)

	time.AfterFunc(30*time.Second, func() {
		select {
		case <-closed:
		default:
			panic("timeout")
		}
	})

	return closeFn()
}

// shutdownWithTimeout is a wrapper around closeWithTimeout to handle shutdown
// functions.
// NOTE: We pass a background context here since we want to be notified if the
// graceful shutdown times out during testing rather than forcing a shutdown by
// closing the ctx.
func shutdownWithTimeout(shutdownFn func(context.Context) error) error {
	return closeWithTimeout(func() error {
		return shutdownFn(context.Background())
	})
}
