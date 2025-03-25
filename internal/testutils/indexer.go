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

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/explorer"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/jape"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// Indexer is a test utility combining an indexer, an http client for the
// indexer and useful helpers for testing.
type Indexer struct {
	*api.Client

	db     *postgres.Store
	cm     *chain.Manager
	syncer *syncer.Syncer
	wallet *wallet.SingleAddressWallet
}

// NewIndexer creates a new indexer for testing that is automatically closed up
// after the test is finished.
func NewIndexer(t testing.TB, c *ConsensusNode, log *zap.Logger) *Indexer {
	// prepare store
	store := NewDB(t, log)

	syncerListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// peers will reject us if our hostname is empty or unspecified, so use loopback
	s := syncer.New(syncerListener, c.cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  c.genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerListener.Addr().String(),
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)
	go s.Run()

	walletKey := types.GeneratePrivateKey()
	wm, err := wallet.NewSingleAddressWallet(walletKey, c.cm, store, wallet.WithLogger(log.Named("wallet")), wallet.WithReservationDuration(3*time.Hour))
	if err != nil {
		t.Fatalf("failed to create wallet: %v", err)
	}

	hm, err := hosts.NewManager(store, hosts.WithLogger(log.Named("hosts")), hosts.WithScanFrequency(500*time.Millisecond), hosts.WithScanInterval(time.Second))
	if err != nil {
		t.Fatalf("failed to create host manager: %v", err)
	}

	cf := contracts.NewContractor(c.cm, wm, walletKey)
	contracts, err := contracts.NewManager(walletKey.PublicKey(), c.cm, cf, nil, store, s, wm, contracts.WithLogger(log.Named("contracts")))
	if err != nil {
		t.Fatalf("failed to create contract manager: %v", err)
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

	apiOpts := []api.ServerOption{
		api.WithLogger(log.Named("api")),
		api.WithExplorer(explorer.New("https://api.siascan.com")),
	}

	password := hex.EncodeToString(frand.Bytes(16))
	web := http.Server{
		Handler: jape.BasicAuth(password)(api.NewServer(c.cm, s, wm, store, apiOpts...)),
	}

	httpListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on http address: %v", err)
	}

	go func() {
		if err := web.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(err))
		}
	}()

	t.Cleanup(func() {
		if err := shutdownWithTimeout(web.Shutdown); err != nil {
			t.Errorf("failed to shutdown webserver: %v", err)
		}
		if err := closeWithTimeout(s.Close); err != nil {
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
		if err := closeWithTimeout(subscriber.Close); err != nil {
			t.Errorf("failed to close subscriber: %v", err)
		}
		if err := closeWithTimeout(store.Close); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	})
	return &Indexer{
		Client: api.NewClient(fmt.Sprintf("http://%s", httpListener.Addr().String()), password),

		db:     store,
		cm:     c.cm,
		syncer: s,
		wallet: wm,
	}
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
