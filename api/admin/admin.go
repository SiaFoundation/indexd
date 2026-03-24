package admin

import (
	"context"
	"net/http"
	"regexp"

	"errors"
	"fmt"
	"net/http/pprof"
	"runtime"
	"time"
	"unicode/utf8"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/build"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/explorer"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/prometheus"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

const (
	stdTxnSize = 1200 // bytes
)

var startTime = time.Now()

var (
	// ErrInternalError is returned when a signed URL can not be authenticated
	// because of an unexpected issue.
	ErrInternalError = errors.New("internal error")
)

type (
	// A ChainManager retrieves the current blockchain state
	ChainManager interface {
		TipState() consensus.State
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// ContractManager manages contracts, but is also responsible for funding
	// ephemeral accounts. If a new account is added we trigger account funding
	// to ensure the account is funded as soon as possible.
	ContractManager interface {
		TriggerAccountFunding(force bool) error

		MaintenanceSettings(ctx context.Context) (contracts.MaintenanceSettings, error)
		UpdateMaintenanceSettings(ctx context.Context, ms contracts.MaintenanceSettings) error

		Contract(ctx context.Context, contractID types.FileContractID) (contracts.Contract, error)
		Contracts(ctx context.Context, offset, limit int, queryOpts ...contracts.ContractQueryOpt) ([]contracts.Contract, error)
	}

	// HostManager defines an interface that allows triggering a host scan.
	HostManager interface {
		TriggerHostScanning()
		ScanHost(ctx context.Context, hk types.PublicKey) (hosts.Host, error)

		Host(ctx context.Context, hk types.PublicKey) (hosts.Host, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)

		BlockHosts(ctx context.Context, hks []types.PublicKey, reasons []string) error
		BlockedHosts(ctx context.Context, offset, limit int) ([]types.PublicKey, error)
		UnblockHost(ctx context.Context, hk types.PublicKey) error
		ResetLostSectors(ctx context.Context, hk types.PublicKey) error

		UsabilitySettings(ctx context.Context) (hosts.UsabilitySettings, error)
		UpdateUsabilitySettings(ctx context.Context, us hosts.UsabilitySettings) error

		Stats(ctx context.Context, offset, limit int) ([]hosts.HostStats, error)
	}

	// PinManager defines an interface for managing pinned settings.
	PinManager interface {
		PinnedSettings(ctx context.Context) (pins.PinnedSettings, error)
		UpdatePinnedSettings(ctx context.Context, ps pins.PinnedSettings) error
	}

	// Explorer retrieves data about the Sia network from an external source.
	Explorer interface {
		BaseURL() string
		SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error)
	}

	// Accounts defines the account management interface for the application API.
	Accounts interface {
		Account(ctx context.Context, ak types.PublicKey) (accounts.Account, error)
		Accounts(ctx context.Context, offset, limit int, opts ...accounts.QueryAccountsOpt) ([]accounts.Account, error)
		DeleteAccount(ctx context.Context, acc proto.Account) error
		UpdateAccount(ctx context.Context, ak types.PublicKey, updates accounts.UpdateAccountRequest) error

		AddAppConnectKey(context.Context, accounts.AppConnectKeyRequest) (accounts.ConnectKey, error)
		UpdateAppConnectKey(context.Context, accounts.AppConnectKeyRequest) (accounts.ConnectKey, error)
		DeleteAppConnectKey(context.Context, string) error
		AppConnectKey(ctx context.Context, key string) (accounts.ConnectKey, error)
		AppConnectKeys(ctx context.Context, offset, limit int) ([]accounts.ConnectKey, error)
		RegisterAppKey(string, types.PublicKey, accounts.AppMeta) error

		PutQuota(ctx context.Context, key string, req accounts.PutQuotaRequest) error
		DeleteQuota(ctx context.Context, key string) error
		Quota(ctx context.Context, key string) (accounts.Quota, error)
		Quotas(ctx context.Context, offset, limit int) ([]accounts.Quota, error)
	}

	// A Store is a persistent store for the indexer.
	Store interface {
		AccountStats() (AccountStatsResponse, error)
		AggregatedHostStats() (AggregatedHostStatsResponse, error)
		ConnectKeyStats() (ConnectKeyStatsResponse, error)
		AppStats(offset, limit int) ([]AppStats, error)
		ContractsStats() (ContractsStatsResponse, error)
		HostStats(offset, limit int) ([]hosts.HostStats, error)
		SectorStats() (SectorsStatsResponse, error)

		DeleteContract(contractID types.FileContractID) error
		DeleteObject(account proto.Account, objectKey types.Hash256) error
		LastScannedIndex() (types.ChainIndex, error)
		ObjectsForSlab(slabID slabs.SlabID) ([]slabs.SlabObject, error)
		PruneSlabs(account proto.Account) error
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		Connect(ctx context.Context, addr string) (*syncer.Peer, error)
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
	}

	// A Wallet manages siacoins and siafunds.
	Wallet interface {
		Address() types.Address
		Balance() (balance wallet.Balance, err error)

		UnconfirmedEvents() ([]wallet.Event, error)
		Events(offset, limit int) ([]wallet.Event, error)
		Event(id types.Hash256) (wallet.Event, error)

		RecommendedFee() types.Currency
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error

		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}

	// An Alerter manages alerts.
	Alerter interface {
		DismissAlerts(ids ...types.Hash256)
		Alert(id types.Hash256) (alerts.Alert, error)
		Alerts(offset, limit int, opts ...alerts.AlertOpt) ([]alerts.Alert, error)
	}
)

type (
	admin struct {
		debug bool

		accounts  Accounts
		alerter   Alerter
		chain     ChainManager
		contracts ContractManager
		explorer  Explorer
		hosts     HostManager
		pins      PinManager
		store     Store
		syncer    Syncer
		wallet    Wallet

		log *zap.Logger
	}
)

// NewAPI initializes the admin API, which is protected via http basic
// authentication and should never be exposed on the public internet. The admin
// API exposes endpoints to manage accounts, hosts, settings and the wallet.
// This is different from the application API, which users, or rather their
// applications, can use to pin slabs.
func NewAPI(chain ChainManager, accounts Accounts, contracts ContractManager, hosts HostManager, pm PinManager, syncer Syncer, wallet Wallet, store Store, alerter Alerter, opts ...Option) http.Handler {
	a := &admin{
		chain:     chain,
		accounts:  accounts,
		contracts: contracts,
		hosts:     hosts,
		pins:      pm,
		store:     store,
		syncer:    syncer,
		wallet:    wallet,
		alerter:   alerter,
		log:       zap.NewNop(),
	}
	for _, opt := range opts {
		opt(a)
	}

	routes := map[string]jape.Handler{
		"GET /state": a.handleGETState,

		"GET /consensus/state":   a.handleGETConsensusState,
		"GET /consensus/network": a.handleGETConsensusNetwork,

		// accounts endpoints
		"GET    /accounts":                        a.handleGETAccounts,
		"GET    /account/:accountkey":             a.handleGETAccount,
		"DELETE /account/:accountkey":             a.handleDELETEAccount,
		"POST   /account/:accountkey/slabs/prune": a.handlePOSTAccountPrune,
		"PATCH  /account/:accountkey":             a.handlePATCHAccount,

		// alerts endpoints
		"GET    /alerts":         a.handleGETAlerts,
		"GET    /alerts/:id":     a.handleGETAlertsID,
		"POST   /alerts/dismiss": a.handlePOSTAlertsDismiss,

		// contract endpoints
		"GET    /contract/:contractid": a.handleGETContract,
		"DELETE /contract/:contractid": a.handleDELETEContract,

		// contracts endpoints
		"GET /contracts": a.handleGETContracts,

		// explorer endpoints
		"GET /explorer/exchange-rate/siacoin/:currency": a.handleGETExplorerSiacoinExchangeRate,

		// host endpoints
		"GET    /host/:hostkey":                   a.handleGETHost,
		"POST   /host/:hostkey/scan":              a.handlePOSTHostScan,
		"POST   /host/:hostkey/lostsectors/reset": a.handlePOSTHostLostSectorsReset,

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

		// syncer endpoints
		"POST /syncer/connect": a.handlePOSTSyncerConnect,

		// txpool endpoints
		"GET /txpool/recommendedfee": a.handleGETTxpoolRecommendedFee,

		// connect endpoints
		"GET    /apps/connect/keys":      a.handleGETAppConnectKeys,
		"POST   /apps/connect/keys":      a.handlePOSTAppConnectKeys,
		"PUT    /apps/connect/keys":      a.handlePUTAppConnectKeys,
		"GET    /apps/connect/keys/:key": a.handleGETAppConnectKeysKey,
		"DELETE /apps/connect/keys/:key": a.handleDELETEAppConnectKeys,
		"POST   /apps/register":          a.handlePOSTAppsRegister,

		// quota endpoints
		"GET    /quotas":      a.handleGETQuotas,
		"PUT    /quotas/:key": a.handlePUTQuota,
		"GET    /quotas/:key": a.handleGETQuota,
		"DELETE /quotas/:key": a.handleDELETEQuota,

		// wallet endpoints
		"GET /wallet":            a.handleGETWallet,
		"GET /wallet/events":     a.handleGETWalletEvents,
		"GET /wallet/events/:id": a.handleGETWalletEventsID,
		"GET /wallet/pending":    a.handleGETWalletPending,
		"POST /wallet/send":      a.handlePOSTWalletSend,

		// stats endpoints
		"GET /stats/accounts":       a.handleGETStatsAccounts,
		"GET /stats/apps":           a.handleGETStatsApps,
		"GET /stats/connectkeys":    a.handleGETStatsConnectKeys,
		"GET /stats/contracts":      a.handleGETStatsContracts,
		"GET /stats/hosts":          a.handleGETStatsHostsAggregated,
		"GET /stats/hosts/detailed": a.handleGETStatsHostsDetailed,
		"GET /stats/sectors":        a.handleGETStatsSectors,
	}

	// debug endpoints
	if a.debug {
		routes["GET /debug/pprof/:handler"] = a.handleGETPProf
		routes["DELETE /debug/slab/:slabid"] = a.handleDELETESlab
		routes["POST /debug/slabs/prune"] = a.handlePOSTPruneAccounts
	}

	return jape.Mux(routes)
}

func (a *admin) checkServerError(jc jape.Context, context string, err error) bool {
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *admin) handleGETConsensusState(jc jape.Context) {
	jc.Encode(a.chain.TipState())
}

func (a *admin) handleGETConsensusNetwork(jc jape.Context) {
	jc.Encode(a.chain.TipState().Network)
}

func (a *admin) handleGETPProf(jc jape.Context) {
	var handler string
	if err := jc.DecodeParam("handler", &handler); err != nil {
		return
	}

	switch handler {
	case "cmdline":
		pprof.Cmdline(jc.ResponseWriter, jc.Request)
	case "profile":
		pprof.Profile(jc.ResponseWriter, jc.Request)
	case "symbol":
		pprof.Symbol(jc.ResponseWriter, jc.Request)
	case "trace":
		pprof.Trace(jc.ResponseWriter, jc.Request)
	default:
		pprof.Index(jc.ResponseWriter, jc.Request)
	}
}

func (a *admin) handleDELETESlab(jc jape.Context) {
	var slabID slabs.SlabID
	if jc.DecodeParam("slabid", &slabID) != nil {
		return
	}

	// fetch all objects referencing the slab
	objects, err := a.store.ObjectsForSlab(slabID)
	if jc.Check("failed to get objects for slab", err) != nil {
		return
	}

	// delete each object
	for _, obj := range objects {
		if err := a.store.DeleteObject(obj.Account, obj.ObjectID); err != nil {
			jc.Check("failed to delete object", err)
			return
		}
	}

	jc.Encode(nil)
}

func (a *admin) handlePOSTPruneAccounts(jc jape.Context) {
	const batchSize = 100
	for offset := 0; ; offset += batchSize {
		accs, err := a.accounts.Accounts(jc.Request.Context(), offset, batchSize)
		if jc.Check("failed to get accounts", err) != nil {
			return
		}

		for _, acc := range accs {
			if err := a.store.PruneSlabs(acc.AccountKey); err != nil {
				jc.Check("failed to prune slabs", err)
				return
			}
		}

		if len(accs) < batchSize {
			break
		}
	}
	jc.Encode(nil)
}

func (a *admin) handleGETAppConnectKeys(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	keys, err := a.accounts.AppConnectKeys(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get app connect keys", err) != nil {
		return
	}
	jc.Encode(keys)
}

func (a *admin) handlePOSTAppConnectKeys(jc jape.Context) {
	var req accounts.AppConnectKeyRequest
	if jc.Decode(&req) != nil {
		return
	}

	created, err := a.accounts.AddAppConnectKey(jc.Request.Context(), req)
	if errors.Is(err, accounts.ErrKeyAlreadyExists) {
		jc.Error(err, http.StatusConflict)
		return
	} else if errors.Is(err, accounts.ErrQuotaNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, accounts.ErrNoQuota) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("failed to add app connect key", err) != nil {
		return
	}
	jc.Encode(created)
}

func (a *admin) handlePUTAppConnectKeys(jc jape.Context) {
	var key accounts.AppConnectKeyRequest
	if jc.Decode(&key) != nil {
		return
	}

	_, err := a.accounts.UpdateAppConnectKey(jc.Request.Context(), key)
	if errors.Is(err, accounts.ErrKeyNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, accounts.ErrQuotaNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, accounts.ErrNoQuota) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("failed to update app connect key", err) != nil {
		return
	}
	// PUT doesn't expect a body
	jc.Encode(nil)
}

func (a *admin) handleGETAppConnectKeysKey(jc jape.Context) {
	var key string
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	connectKey, err := a.accounts.AppConnectKey(jc.Request.Context(), key)
	if errors.Is(err, accounts.ErrKeyNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get app connect key", err) != nil {
		return
	}
	jc.Encode(connectKey)
}

func (a *admin) handleDELETEAppConnectKeys(jc jape.Context) {
	var key string
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	err := a.accounts.DeleteAppConnectKey(jc.Request.Context(), key)
	if errors.Is(err, accounts.ErrKeyNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to delete app connect key", err) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handlePOSTAppsRegister(jc jape.Context) {
	var req RegisterAppKeyRequest
	if jc.Decode(&req) != nil {
		return
	}

	// basic validation of required fields
	if req.ConnectKey == "" {
		jc.Error(errors.New("connect key is required"), http.StatusBadRequest)
		return
	} else if req.AppKey == (types.PublicKey{}) {
		jc.Error(errors.New("app key is required"), http.StatusBadRequest)
		return
	}

	err := a.accounts.RegisterAppKey(req.ConnectKey, req.AppKey, req.Meta)
	switch {
	case errors.Is(err, accounts.ErrExists):
		jc.Encode(nil)
	case errors.Is(err, accounts.ErrKeyExhausted):
		jc.Error(accounts.ErrKeyExhausted, http.StatusForbidden)
	case errors.Is(err, accounts.ErrKeyNotFound):
		jc.Error(accounts.ErrKeyNotFound, http.StatusUnauthorized)
	case err != nil:
		a.log.Debug("failed to use app connect key", zap.Error(err))
		jc.Error(ErrInternalError, http.StatusInternalServerError)
	default:
		if err := a.contracts.TriggerAccountFunding(false); err != nil {
			// error is ignored since the account is already connected
			a.log.Debug("failed to trigger account funding", zap.Error(err))
		}
		jc.Encode(nil)
	}
}

func (a *admin) handleGETQuotas(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	quotas, err := a.accounts.Quotas(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get quotas", err) != nil {
		return
	}
	jc.Encode(quotas)
}

func (a *admin) handleGETQuota(jc jape.Context) {
	var key string
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	quota, err := a.accounts.Quota(jc.Request.Context(), key)
	if errors.Is(err, accounts.ErrQuotaNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get quota", err) != nil {
		return
	}
	jc.Encode(quota)
}

func (a *admin) handlePUTQuota(jc jape.Context) {
	var key string
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	// validate key
	if key == "" {
		jc.Error(errors.New("key cannot be empty"), http.StatusBadRequest)
		return
	} else if !isValidQuotaKey(key) {
		jc.Error(errors.New("key must only contain letters, digits, hyphens, underscores and be between 1 and 32 characters long"), http.StatusBadRequest)
		return
	}

	var req accounts.PutQuotaRequest
	if jc.Decode(&req) != nil {
		return
	} else if req.TotalUses < 0 {
		jc.Error(errors.New("totalUses must be non-negative"), http.StatusBadRequest)
		return
	} else if req.FundTargetBytes == nil {
		jc.Error(errors.New("fundTargetBytes is required"), http.StatusBadRequest)
		return
	}

	if jc.Check("failed to put quota", a.accounts.PutQuota(jc.Request.Context(), key, req)) != nil {
		return
	}
}

func (a *admin) handleDELETEQuota(jc jape.Context) {
	var key string
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	err := a.accounts.DeleteQuota(jc.Request.Context(), key)
	if errors.Is(err, accounts.ErrQuotaNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, accounts.ErrQuotaInUse) {
		jc.Error(err, http.StatusConflict)
		return
	} else if jc.Check("failed to delete quota", err) != nil {
		return
	}
}

func (a *admin) handleGETAccount(jc jape.Context) {
	var ak types.PublicKey
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}
	acc, err := a.accounts.Account(jc.Request.Context(), ak)
	if errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get account", err) != nil {
		return
	}
	jc.Encode(acc)
}

func (a *admin) handleGETAccounts(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	var opts []accounts.QueryAccountsOpt
	if connectKey := jc.Request.FormValue("connectkey"); connectKey != "" {
		opts = append(opts, accounts.WithConnectKey(connectKey))
	}

	accs, err := a.accounts.Accounts(jc.Request.Context(), offset, limit, opts...)
	if errors.Is(err, accounts.ErrKeyNotFound) {
		// cast needed or empty response will be written
		jc.Encode([]accounts.Account(nil))
		return
	} else if jc.Check("failed to get accounts", err) != nil {
		return
	}
	jc.Encode(accs)
}

func (a *admin) handleDELETEAccount(jc jape.Context) {
	var ak proto.Account
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}

	err := a.accounts.DeleteAccount(jc.Request.Context(), ak)
	if errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to delete account", err) != nil {
		return
	}
}

func (a *admin) handlePOSTAccountPrune(jc jape.Context) {
	var ak proto.Account
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}
	if err := a.store.PruneSlabs(ak); errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(nil)
}

func (a *admin) handlePATCHAccount(jc jape.Context) {
	var ak types.PublicKey
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}
	var req accounts.UpdateAccountRequest
	if jc.Decode(&req) != nil {
		return
	}
	err := a.accounts.UpdateAccount(jc.Request.Context(), ak, req)
	if errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to update max pinned data", err) != nil {
		return
	}
}

func (a *admin) handleGETAlerts(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	var opts []alerts.AlertOpt
	if jc.Request.FormValue("severity") != "" {
		var severity alerts.Severity
		if jc.DecodeForm("severity", &severity) != nil {
			return
		}
		opts = append(opts, alerts.WithSeverity(severity))
	}

	alerts, err := a.alerter.Alerts(offset, limit, opts...)
	if jc.Check("failed to get alerts", err) != nil {
		return
	}
	jc.Encode(alerts)
}

func (a *admin) handleGETAlertsID(jc jape.Context) {
	var id types.Hash256
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	alert, err := a.alerter.Alert(id)
	if errors.Is(err, alerts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get alert", err) != nil {
		return
	}
	jc.Encode(alert)
}

func (a *admin) handlePOSTAlertsDismiss(jc jape.Context) {
	var ids []types.Hash256
	if jc.Decode(&ids) != nil {
		return
	}

	a.alerter.DismissAlerts(ids...)
	jc.Encode(nil)
}

func (a *admin) handleGETState(jc jape.Context) {
	ci, err := a.store.LastScannedIndex()
	if jc.Check("failed to get last scanned index", err) != nil {
		return
	}

	var explorer ExplorerState
	if a.explorer != nil {
		explorer.Enabled = true
		explorer.URL = a.explorer.BaseURL()
	}

	ts := a.chain.TipState()
	writeResponse(jc, State{
		Network:   ts.Network.Name,
		StartTime: startTime,
		Explorer:  explorer,
		BuildState: BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: build.Time(),
		},
		ScanHeight: ci.Height,
		SyncHeight: ts.Index.Height,
		Synced:     time.Since(ts.PrevTimestamps[0]) <= 3*time.Hour,
	})
}

func (a *admin) handlePOSTSyncerConnect(jc jape.Context) {
	var addr string
	if jc.Decode(&addr) != nil {
		return
	}
	_, err := a.syncer.Connect(jc.Request.Context(), addr)
	if jc.Check("couldn't connect to peer", err) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleGETTxpoolRecommendedFee(jc jape.Context) {
	jc.Encode(a.wallet.RecommendedFee())
}

func (a *admin) handleGETExplorerSiacoinExchangeRate(jc jape.Context) {
	if a.explorer == nil {
		jc.Error(explorer.ErrDisabled, http.StatusServiceUnavailable)
		return
	}

	var currency string
	if jc.DecodeParam("currency", &currency) != nil {
		return
	}

	rate, err := a.explorer.SiacoinExchangeRate(jc.Request.Context(), currency)
	if jc.Check("failed to get siacoin exchange rate", err) != nil {
		return
	}

	jc.Encode(rate)
}

func (a *admin) handleGETContract(jc jape.Context) {
	var contractID types.FileContractID
	if jc.DecodeParam("contractid", &contractID) != nil {
		return
	}

	contract, err := a.contracts.Contract(jc.Request.Context(), contractID)
	if errors.Is(err, contracts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get contract", err) != nil {
		return
	}

	jc.Encode(contract)
}

func (a *admin) handleDELETEContract(jc jape.Context) {
	var contractID types.FileContractID
	if jc.DecodeParam("contractid", &contractID) != nil {
		return
	}

	err := a.store.DeleteContract(contractID)
	if errors.Is(err, contracts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to delete contract", err) != nil {
		return
	}

	jc.Encode(nil)
}

func (a *admin) handleGETContracts(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	sorts, ok := api.ParseSortOptions(jc)
	if !ok {
		return
	}

	var opts []contracts.ContractQueryOpt
	if jc.Request.FormValue("revisable") != "" {
		var revisable bool
		if jc.DecodeForm("revisable", &revisable) != nil {
			return
		}
		opts = append(opts, contracts.WithRevisable(revisable))
	} else {
		opts = append(opts, contracts.WithRevisable(true)) // default to revisable contracts
	}

	if jc.Request.FormValue("good") != "" {
		var good bool
		if jc.DecodeForm("good", &good) != nil {
			return
		}
		opts = append(opts, contracts.WithGood(good))
	}

	// filter by ID
	if strs := jc.Request.Form["id"]; len(strs) > 0 {
		ids := make([]types.FileContractID, len(strs))
		for i := range strs {
			var id types.FileContractID
			if err := id.UnmarshalText([]byte(strs[i])); err != nil {
				jc.Error(fmt.Errorf("failed to parse contract ID %s: %w", strs[i], err), http.StatusBadRequest)
				return
			}
			ids[i] = id
		}
		opts = append(opts, contracts.WithIDs(ids))
	}

	// filter by host public key
	if strs := jc.Request.Form["hostkey"]; len(strs) > 0 {
		hks := make([]types.PublicKey, len(strs))
		for i := range strs {
			var hk types.PublicKey
			if err := hk.UnmarshalText([]byte(strs[i])); err != nil {
				jc.Error(fmt.Errorf("failed to parse public key %s: %w", strs[i], err), http.StatusBadRequest)
				return
			}
			hks[i] = hk
		}
		opts = append(opts, contracts.WithHostKeys(hks))
	}

	for _, sort := range sorts {
		opts = append(opts, contracts.WithSorting(sort.Field, sort.Descending))
	}

	res, err := a.contracts.Contracts(jc.Request.Context(), offset, limit, opts...)
	if errors.Is(err, contracts.ErrInvalidSortField) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("failed to get contracts", err) != nil {
		return
	}
	jc.Encode(res)
}

func (a *admin) handleGETHost(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	host, err := a.hosts.Host(jc.Request.Context(), hk)
	if errors.Is(err, hosts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get host", err) != nil {
		return
	}
	jc.Encode(host)
}

func (a *admin) handlePOSTHostScan(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	host, err := a.hosts.ScanHost(jc.Request.Context(), hk)
	if errors.Is(err, hosts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to scan host", err) != nil {
		return
	}
	jc.Encode(host)
}

func (a *admin) handlePOSTHostLostSectorsReset(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	err := a.hosts.ResetLostSectors(jc.Request.Context(), hk)
	if errors.Is(err, hosts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to reset lost sectors", err) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleGETHosts(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	sorts, ok := api.ParseSortOptions(jc)
	if !ok {
		return
	}

	var opts []hosts.HostQueryOpt
	if jc.Request.FormValue("usable") != "" {
		var usable bool
		if jc.DecodeForm("usable", &usable) != nil {
			return
		}
		opts = append(opts, hosts.WithUsable(usable))
	}
	if jc.Request.FormValue("blocked") != "" {
		var blocked bool
		if jc.DecodeForm("blocked", &blocked) != nil {
			return
		}
		opts = append(opts, hosts.WithBlocked(blocked))
	}
	if jc.Request.FormValue("activecontracts") != "" {
		var activeContracts bool
		if jc.DecodeForm("activecontracts", &activeContracts) != nil {
			return
		}
		opts = append(opts, hosts.WithActiveContracts(activeContracts))
	}
	if strs := jc.Request.Form["hostkey"]; len(strs) > 0 {
		hks := make([]types.PublicKey, len(strs))
		for i := range strs {
			var hk types.PublicKey
			if err := hk.UnmarshalText([]byte(strs[i])); err != nil {
				jc.Error(fmt.Errorf("failed to parse host key %s: %w", strs[i], err), http.StatusBadRequest)
				return
			}
			hks[i] = hk
		}
		opts = append(opts, hosts.WithPublicKeys(hks))
	}
	for _, sort := range sorts {
		opts = append(opts, hosts.WithSorting(sort.Field, sort.Descending))
	}

	res, err := a.hosts.Hosts(jc.Request.Context(), offset, limit, opts...)
	if errors.Is(err, hosts.ErrInvalidSortField) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("failed to get hosts", err) != nil {
		return
	}
	jc.Encode(res)
}

func (a *admin) handleGETHostsBlocklist(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	hosts, err := a.hosts.BlockedHosts(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get blocklist", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (a *admin) handlePUTHostsBlocklist(jc jape.Context) {
	var hosts HostsBlocklistRequest
	if jc.Decode(&hosts) != nil {
		return
	}
	if jc.Check("failed to add host keys to blocklist", a.hosts.BlockHosts(jc.Request.Context(), hosts.HostKeys, hosts.Reasons)) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleDELETEHostsBlocklist(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	if jc.Check("failed to unblock host", a.hosts.UnblockHost(jc.Request.Context(), hk)) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleGETSettingsContracts(jc jape.Context) {
	ms, err := a.contracts.MaintenanceSettings(jc.Request.Context())
	if jc.Check("failed to get contract settings", err) != nil {
		return
	}
	jc.Encode(ms)
}

func (a *admin) handlePUTSettingsContracts(jc jape.Context) {
	var ms contracts.MaintenanceSettings
	if jc.Decode(&ms) != nil {
		return
	}
	if jc.Check("failed to update contract settings", a.contracts.UpdateMaintenanceSettings(jc.Request.Context(), ms)) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleGETSettingsHosts(jc jape.Context) {
	s, err := a.hosts.UsabilitySettings(jc.Request.Context())
	if jc.Check("failed to get host settings", err) != nil {
		return
	}
	jc.Encode(s)
}

func (a *admin) handlePUTSettingsHosts(jc jape.Context) {
	var s hosts.UsabilitySettings
	if jc.Decode(&s) != nil {
		return
	}
	if jc.Check("failed to update host settings", a.hosts.UpdateUsabilitySettings(jc.Request.Context(), s)) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleGETSettingsPricePinning(jc jape.Context) {
	s, err := a.pins.PinnedSettings(jc.Request.Context())
	if jc.Check("failed to get price pinning settings", err) != nil {
		return
	}
	jc.Encode(s)
}

func (a *admin) handlePUTSettingsPricePinning(jc jape.Context) {
	var s pins.PinnedSettings
	if jc.Decode(&s) != nil {
		return
	} else if !(s.Currency == "" || utf8.RuneCountInString(s.Currency) == 3) {
		jc.Error(errors.New("'currency' must exactly 3 characters, or left empty to disable price pinning"), http.StatusBadRequest)
		return
	}

	if jc.Check("failed to update price pinning settings", a.pins.UpdatePinnedSettings(jc.Request.Context(), s)) != nil {
		return
	}
	jc.Encode(nil)
}

func (a *admin) handleGETWallet(jc jape.Context) {
	balance, err := a.wallet.Balance()
	if !a.checkServerError(jc, "failed to get wallet", err) {
		return
	}

	writeResponse(jc, WalletResponse{
		Balance: balance,
		Address: a.wallet.Address(),
	})
}

func (a *admin) handleGETWalletEvents(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	events, err := a.wallet.Events(offset, limit)
	if !a.checkServerError(jc, "failed to get events", err) {
		return
	}

	jc.Encode(events)
}

func (a *admin) handleGETWalletEventsID(jc jape.Context) {
	var id types.Hash256
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	event, err := a.wallet.Event(id)
	if !a.checkServerError(jc, "failed to get events", err) {
		return
	}

	jc.Encode(event)
}

func (a *admin) handleGETWalletPending(jc jape.Context) {
	pending, err := a.wallet.UnconfirmedEvents()
	if !a.checkServerError(jc, "failed to get wallet pending", err) {
		return
	}
	jc.Encode(pending)
}

func (a *admin) handlePOSTWalletSend(jc jape.Context) {
	var req WalletSendSiacoinsRequest
	if jc.Decode(&req) != nil {
		return
	}
	if req.Address == types.VoidAddress {
		jc.Error(errors.New("cannot send to void address"), http.StatusBadRequest)
		return
	}

	// subtract miner fee if necessary
	minerFee := a.wallet.RecommendedFee().Mul64(stdTxnSize)
	if req.SubtractMinerFee {
		var underflow bool
		req.Amount, underflow = req.Amount.SubWithUnderflow(minerFee)
		if underflow {
			jc.Error(fmt.Errorf("amount must be greater than miner fee: %s", minerFee), http.StatusBadRequest)
			return
		}
	}

	// prepare the transaction
	txn := types.V2Transaction{
		MinerFee: minerFee,
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: req.Address, Value: req.Amount},
		},
	}

	// fund and sign the transaction
	basis, toSign, err := a.wallet.FundV2Transaction(&txn, req.Amount.Add(minerFee), req.UseUnconfirmed)
	if !a.checkServerError(jc, "failed to fund transaction", err) {
		return
	}
	a.wallet.SignV2Inputs(&txn, toSign)
	basis, txnset, err := a.chain.V2TransactionSet(basis, txn)
	if !a.checkServerError(jc, "failed to create transaction set", err) {
		a.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
		return
	}
	// broadcast the transaction
	if jc.Check("failed to broadcast transaction", a.wallet.BroadcastV2TransactionSet(basis, txnset)) != nil {
		a.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
		return
	}

	jc.Encode(txn.ID())
}

func (a *admin) handleGETStatsAccounts(jc jape.Context) {
	stats, err := a.store.AccountStats()
	if jc.Check("failed to retrieve account stats", err) != nil {
		return
	}
	writeResponse(jc, stats)
}

func (a *admin) handleGETStatsConnectKeys(jc jape.Context) {
	stats, err := a.store.ConnectKeyStats()
	if jc.Check("failed to retrieve connect key stats", err) != nil {
		return
	}
	writeResponse(jc, stats)
}

func (a *admin) handleGETStatsApps(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}
	stats, err := a.store.AppStats(offset, limit)
	if jc.Check("failed to retrieve app stats", err) != nil {
		return
	}
	writeResponse(jc, AppStatsResponse(stats))
}

func (a *admin) handleGETStatsContracts(jc jape.Context) {
	stats, err := a.store.ContractsStats()
	if jc.Check("failed to retrieve contracts stats", err) != nil {
		return
	}
	writeResponse(jc, stats)
}

func (a *admin) handleGETStatsHostsDetailed(jc jape.Context) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	stats, err := a.hosts.Stats(jc.Request.Context(), offset, limit)
	if jc.Check("failed to retrieve host stats", err) != nil {
		return
	}

	var res []HostStats
	for _, s := range stats {
		res = append(res, HostStats(s))
	}
	writeResponse(jc, HostStatsResponse(res))
}

func (a *admin) handleGETStatsHostsAggregated(jc jape.Context) {
	stats, err := a.store.AggregatedHostStats()
	if jc.Check("failed to retrieve aggregated hosts stats", err) != nil {
		return
	}
	writeResponse(jc, stats)
}

func (a *admin) handleGETStatsSectors(jc jape.Context) {
	stats, err := a.store.SectorStats()
	if jc.Check("failed to retrieve sector stats", err) != nil {
		return
	}
	writeResponse(jc, stats)
}

func writeResponse(jc jape.Context, resp prometheus.Marshaller) {
	if resp == nil {
		return
	}

	var responseFormat string
	if jc.Check("failed to decode form", jc.DecodeForm("response", &responseFormat)) != nil {
		return
	}
	switch responseFormat {
	case "prometheus":
		jc.Request.Header.Set("Content-Type", "text/plain; version=0.0.4")
		enc := prometheus.NewEncoder(jc.ResponseWriter)
		if jc.Check("failed to marshal prometheus response", enc.Append(resp)) != nil {
			return
		}
	default:
		jc.Encode(resp)
	}
}

var validQuotaKey = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

func isValidQuotaKey(key string) bool {
	return validQuotaKey.MatchString(key)
}
