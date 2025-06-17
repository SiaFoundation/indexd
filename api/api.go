package api

import (
	"context"
	"net/http"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	// A ChainManager retrieves the current blockchain state
	ChainManager interface {
		TipState() consensus.State
		RecommendedFee() types.Currency
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// ContractManager manages contracts, but is also responsible for funding
	// ephemeral accounts. If a new account is added we trigger account funding
	// to ensure the account is funded as soon as possible.
	ContractManager interface {
		TriggerAccountFunding()
		TriggerMaintenance()
	}

	// HostManager defines an interface that allows trigger a host scan.
	HostManager interface {
		TriggerHostScanning()
	}

	// Explorer retrieves data about the Sia network from an external source.
	Explorer interface {
		SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error)
	}

	// A Store is a persistent store for the indexer.
	Store interface {
		Accounts(ctx context.Context, offset, limit int) ([]types.PublicKey, error)
		AddAccount(ctx context.Context, ak types.PublicKey) error
		DeleteAccount(ctx context.Context, ak types.PublicKey) error
		UpdateAccount(ctx context.Context, oldAK, newAK types.PublicKey) error
		BlockHosts(ctx context.Context, hks []types.PublicKey, reason string) error
		BlockedHosts(ctx context.Context, offset, limit int) ([]types.PublicKey, error)
		Host(ctx context.Context, hk types.PublicKey) (hosts.Host, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		LastScannedIndex(context.Context) (types.ChainIndex, error)
		UnblockHost(ctx context.Context, hk types.PublicKey) error
		UsabilitySettings(ctx context.Context) (hosts.UsabilitySettings, error)
		UpdateUsabilitySettings(ctx context.Context, us hosts.UsabilitySettings) error
		MaintenanceSettings(ctx context.Context) (contracts.MaintenanceSettings, error)
		UpdateMaintenanceSettings(ctx context.Context, ms contracts.MaintenanceSettings) error
		PinnedSettings(ctx context.Context) (pins.PinnedSettings, error)
		UpdatePinnedSettings(ctx context.Context, ps pins.PinnedSettings) error
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
	}

	// A Wallet manages siacoins and siafunds.
	Wallet interface {
		Address() types.Address
		Balance() (balance wallet.Balance, err error)
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
		UnconfirmedEvents() ([]wallet.Event, error)
		Events(offset, limit int) ([]wallet.Event, error)

		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) error
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}
)

type (
	// An api provides an HTTP API for the indexer
	api struct {
		debug     bool
		chain     ChainManager
		contracts ContractManager
		hosts     HostManager
		explorer  Explorer
		store     Store
		syncer    Syncer
		wallet    Wallet
		log       *zap.Logger
	}
)

// NewServer initializes the API
func NewServer(chain ChainManager, contracts ContractManager, hosts HostManager, syncer Syncer, wallet Wallet, store Store, opts ...ServerOption) http.Handler {
	a := &api{
		chain:     chain,
		contracts: contracts,
		hosts:     hosts,
		store:     store,
		syncer:    syncer,
		wallet:    wallet,
		log:       zap.NewNop(),
	}
	for _, opt := range opts {
		opt(a)
	}

	routes := map[string]jape.Handler{
		"GET /state": a.handleGETState,

		// accounts endpoints
		"GET    /accounts":            a.handleGETAccounts,
		"POST   /account/:accountkey": a.handlePOSTAccount,
		"PUT    /account/:accountkey": a.handlePUTAccount,
		"DELETE /account/:accountkey": a.handleDELETEAccount,

		// explorer endpoints
		"GET /explorer/exchange-rate/siacoin/:currency": a.handleGETExplorerSiacoinExchangeRate,

		// host endpoints
		"GET    /host/:hostkey": a.handleGETHost,

		// hosts endpoints
		"GET    /hosts":                    a.handleGETHosts,
		"GET    /hosts/blocklist":          a.handleGETHostsBlocklist,
		"PUT    /hosts/blocklist":          a.handlePUTHostsBlocklist,
		"DELETE /hosts/blocklist/:hostkey": a.handleDELETEHostsBlocklist,

		// settings endpoints
		"GET /settings/contracts":    a.handleGETSettingsContracts,
		"PUT /settings/contracts":    a.handlePUTSettingsContracts,
		"GET /settings/hosts":        a.handleGETSettingsHosts,
		"PUT /settings/hosts":        a.handlePUTSettingsHosts,
		"GET /settings/pricepinning": a.handleGETSettingsPricePinning,
		"PUT /settings/pricepinning": a.handlePUTSettingsPricePinning,

		// wallet endpoints
		"GET /wallet":         a.handleGETWallet,
		"GET /wallet/events":  a.handleGETWalletEvents,
		"GET /wallet/pending": a.handleGETWalletPending,
		"POST /wallet/send":   a.handlePOSTWalletSend,
	}

	// debug endpoints
	if a.debug {
		routes["GET /debug/trigger/:action"] = a.handleGETTrigger
	}

	return jape.Mux(routes)
}
