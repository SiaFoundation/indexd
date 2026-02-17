package testutils

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/indexd/slabs"

	"go.sia.tech/indexd/client"
	client2 "go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
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
	// MaintenanceSettings are the default maintenance settings used in testing.
	MaintenanceSettings = contracts.MaintenanceSettings{
		Enabled:         true,
		Period:          144,
		RenewWindow:     72,
		WantedContracts: 12,
	}
)

type (

	// Indexer is a test utility combining an indexer, an http client for the
	// indexer and useful helpers for testing.
	Indexer struct {
		appApiAddr string

		Admin *admin.Client
		App   *app.Client

		cm        *chain.Manager
		dialer    *client.Dialer
		accounts  *accounts.AccountManager
		contracts *contracts.ContractManager
		hosts     *hosts.HostManager
		alerter   *alerts.Manager
		store     TestStore
		syncer    *Syncer
		wallet    *wallet.SingleAddressWallet
	}

	// IndexerOpt is a functional option for configuring an indexer for testing
	IndexerOpt func(*indexerCfg)

	indexerCfg struct {
		maintenanceSettings contracts.MaintenanceSettings
		slabOpts            []slabs.Option
		contractOpts        []contracts.ContractManagerOpt
	}
)

func defaultIndexerCfg(log *zap.Logger) *indexerCfg {
	return &indexerCfg{
		maintenanceSettings: MaintenanceSettings,
		contractOpts: []contracts.ContractManagerOpt{
			contracts.WithLogger(log.Named("contracts")),
			contracts.WithMaintenanceFrequency(500 * time.Millisecond),
			contracts.WithMinHostDistance(0), // disable location checks in tests
			contracts.WithSyncPollInterval(500 * time.Millisecond),
			contracts.WithSectorRootsBatchSize(5), // small batch size for testing
		},
		slabOpts: []slabs.Option{
			slabs.WithLogger(log.Named("slabs")),
			slabs.WithHealthCheckInterval(500 * time.Millisecond),
			slabs.WithMinHostDistance(0), // disable location checks in tests
		},
	}
}

// WithMaintenanceSettings allows for passing maintenance settings to the indexer
func WithMaintenanceSettings(ms contracts.MaintenanceSettings) IndexerOpt {
	return func(cfg *indexerCfg) {
		cfg.maintenanceSettings = ms
	}
}

// WithContractOptions allows for passing contract options to the indexer
func WithContractOptions(opts ...contracts.ContractManagerOpt) IndexerOpt {
	return func(cfg *indexerCfg) {
		cfg.contractOpts = append(cfg.contractOpts, opts...)
	}
}

// WithSlabOptions allows for passing slab options to the indexer
func WithSlabOptions(opts ...slabs.Option) IndexerOpt {
	return func(cfg *indexerCfg) {
		cfg.slabOpts = append(cfg.slabOpts, opts...)
	}
}

// AppAPIAddr returns the application API address of the indexer.
func (i *Indexer) AppAPIAddr() string {
	return i.appApiAddr
}

// Accounts returns the account manager for the indexer.
func (i *Indexer) Accounts() *accounts.AccountManager {
	return i.accounts
}

// Contracts returns the contract manager for the indexer.
func (i *Indexer) Contracts() *contracts.ContractManager {
	return i.contracts
}

// Hosts returns the host manager for the indexer.
func (i *Indexer) Hosts() *hosts.HostManager {
	return i.hosts
}

// NewIndexer creates a new indexer for testing that is automatically closed up
// after the test is finished.
func NewIndexer(t testing.TB, c *ConsensusNode, log *zap.Logger, opts ...IndexerOpt) *Indexer {
	cfg := defaultIndexerCfg(log)
	for _, opt := range opts {
		opt(cfg)
	}

	// prepare store
	store := NewDB(t, cfg.maintenanceSettings, log)

	s := NewSyncer(t, c.genesis.ID(), c.cm)

	walletKey := types.GeneratePrivateKey()
	wm := NewWallet(t, c, walletKey)

	locator, err := geoip.NewMaxMindLocator("")
	if err != nil {
		t.Fatal(err)
	}
	syncer := NewSyncer(t, c.genesis.ID(), c.cm)

	client2 := client2.New(client2.NewProvider(hosts.NewHostStore(store)))

	alerter := alerts.NewManager()

	hm, err := hosts.NewManager(syncer, locator, client2, store, alerter, hosts.WithLogger(log.Named("hosts")), hosts.WithScanFrequency(200*time.Millisecond), hosts.WithScanInterval(time.Second))
	if err != nil {
		t.Fatalf("failed to create host manager: %v", err)
	}

	signer := contracts.NewFormContractSigner(wm, walletKey)
	dialer := client.NewDialer(c.cm, signer, store, log, client.WithRevisionSubmissionBuffer(1))
	am, err := accounts.NewManager(store, accounts.WithPruneAccountsInterval(100*time.Millisecond), accounts.WithLogger(log.Named("accounts")))
	if err != nil {
		t.Fatalf("failed to create accounts manager: %v", err)
	}

	f := contracts.NewFunder(client2, signer, c.cm, store, log, contracts.WithRevisionSubmissionBuffer(1))
	contracts, err := contracts.NewManager(walletKey, am, f, c.cm, store, dialer, hm, s, wm, cfg.contractOpts...)
	if err != nil {
		t.Fatalf("failed to create contract manager: %v", err)
	}

	slabs, err := slabs.NewManager(c.cm, am, contracts, hm, store, client2, alerter, keys.DerivePrivateKey(walletKey, "migration"), keys.DerivePrivateKey(walletKey, "integrity"), cfg.slabOpts...)
	if err != nil {
		t.Fatalf("failed to create slab manager: %v", err)
	}

	subscriber, err := subscriber.New(c.cm, hm, contracts, wm, store, subscriber.WithLogger(log.Named("subscriber")))
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	explorer := NewExplorer()
	pm, err := pins.NewManager(explorer, hm, store)
	if err != nil {
		t.Fatalf("failed to create pin manager: %v", err)
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
		admin.WithExplorer(explorer),
	}

	password := hex.EncodeToString(frand.Bytes(16))
	adminAPI := http.Server{
		Handler: jape.BasicAuth(password)(admin.NewAPI(c.cm, am, contracts, hm, pm, syncer, wm, store, alerter, adminAPIOpts...)),
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
	appHandler, err := app.NewAPI(appAPIAddr, store, am, contracts, slabs, appAPIOpts...)
	if err != nil {
		t.Fatalf("failed to create application API: %v", err)
	}

	appAPI := http.Server{
		Handler: appHandler,
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
			t.Errorf("failed to close slab manager: %v", err)
		}
		if err := closeWithTimeout(subscriber.Close); err != nil {
			t.Errorf("failed to close subscriber: %v", err)
		}
		if err := pm.Close(); err != nil {
			t.Errorf("failed to close pin manager: %v", err)
		}
		if err := closeWithTimeout(store.Close); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
		time.Sleep(time.Second) // pgx does not properly clean up its background goroutines triggering goleak
	})

	return &Indexer{
		appApiAddr: appAPIAddr,

		Admin: admin.NewClient(adminAPIAddr, password),
		App:   app.NewClient(appAPIAddr),

		cm:        c.cm,
		accounts:  am,
		hosts:     hm,
		contracts: contracts,
		dialer:    dialer,
		alerter:   alerter,
		store:     store,
		syncer:    syncer,
		wallet:    wm,
	}
}

// AddTestAccount adds a test account to the indexer and funds it.
func (idx *Indexer) AddTestAccount(t *testing.T, pk types.PublicKey) {
	t.Helper()
	idx.store.AddTestAccount(t, pk)
	if err := idx.contracts.TriggerAccountFunding(true); err != nil {
		t.Fatalf("failed to trigger account funding: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // wait for funding to complete
}

// HostClient returns a host client for the given host public key.
func (idx *Indexer) HostClient(t *testing.T, hk types.PublicKey) *client.HostClient {
	h, err := idx.store.Host(hk)
	if err != nil {
		t.Fatalf("failed to get host %s: %v", hk, err) // developer error
	}
	hc, err := idx.dialer.DialHost(context.Background(), hk, h.RHP4Addrs())
	if err != nil {
		t.Fatalf("failed to dial host %s: %v", hk, err) // developer error
	}
	t.Cleanup(func() { hc.Close() })
	return hc
}

// Alerter returns the underlying alert manager.
func (idx *Indexer) Alerter() *alerts.Manager {
	return idx.alerter
}

// Store returns the underlying store.
func (idx *Indexer) Store() TestStore {
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
