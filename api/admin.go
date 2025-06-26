package api

import (
	"context"
	"net/http"

	"errors"
	"fmt"
	"runtime"
	"time"
	"unicode/utf8"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/build"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/explorer"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/pins"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

const (
	defaultLimit = 100
	maxLimit     = 500
	stdTxnSize   = 1200 // bytes
)

var (
	// ErrInvalidOffset is returned when the requested offset is invalid.
	ErrInvalidOffset = errors.New("offset must be non-negative")

	// ErrInvalidLimit is returned when the requested limit is invalid.
	ErrInvalidLimit = fmt.Errorf("limit must between 1 and %d", maxLimit)
)

var startTime = time.Now()

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
	adminAPI struct {
		chain     ChainManager
		contracts ContractManager
		explorer  Explorer
		store     Store
		syncer    Syncer
		wallet    Wallet
		log       *zap.Logger
	}
)

func (a *adminAPI) applyOption(opt Option) { opt.applyToAdmin(a) }

// NewAdminAPI initializes the admin API, which is protected via http basic
// authentication and should never be exposed on the public internet. The admin
// API exposes endpoints to manage accounts, hosts, settings and the wallet.
// This is different from the application API, which users, or rather their
// applications, can use to pin slabs.
func NewAdminAPI(chain ChainManager, contracts ContractManager, syncer Syncer, wallet Wallet, store Store, opts ...Option) http.Handler {
	a := &adminAPI{
		chain:     chain,
		contracts: contracts,
		store:     store,
		syncer:    syncer,
		wallet:    wallet,
		log:       zap.NewNop(),
	}
	for _, opt := range opts {
		a.applyOption(opt)
	}

	return jape.Mux(map[string]jape.Handler{
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
	})
}

func (a *adminAPI) checkServerError(jc jape.Context, context string, err error) bool {
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *adminAPI) handleGETAccounts(jc jape.Context) {
	offset, limit, ok := parseOffsetLimit(jc)
	if !ok {
		return
	}

	accounts, err := a.store.Accounts(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get accounts", err) != nil {
		return
	}
	jc.Encode(accounts)
}

func (a *adminAPI) handlePOSTAccount(jc jape.Context) {
	var ak types.PublicKey
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}

	err := a.store.AddAccount(jc.Request.Context(), ak)
	if errors.Is(err, accounts.ErrExists) {
		jc.Error(err, http.StatusConflict)
		return
	} else if jc.Check("failed to add account", err) != nil {
		return
	}

	a.contracts.TriggerAccountFunding()

	jc.Encode(nil)
}

func (a *adminAPI) handlePUTAccount(jc jape.Context) {
	var ak types.PublicKey
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}

	var req AccountRotateKeyRequest
	if jc.Decode(&req) != nil {
		return
	}

	switch req.NewAccountKey {
	case types.PublicKey{}:
		jc.Error(errors.New("new account key cannot be empty"), http.StatusBadRequest)
		return
	case ak:
		jc.Error(errors.New("new account key cannot be the same as the old one"), http.StatusBadRequest)
		return
	default:
	}

	err := a.store.UpdateAccount(jc.Request.Context(), ak, req.NewAccountKey)
	if errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, accounts.ErrExists) {
		jc.Error(err, http.StatusConflict)
		return
	} else if jc.Check("failed to rotate account key", err) != nil {
		return
	}

	a.contracts.TriggerAccountFunding()

	jc.Encode(nil)
}

func (a *adminAPI) handleDELETEAccount(jc jape.Context) {
	var ak types.PublicKey
	if jc.DecodeParam("accountkey", &ak) != nil {
		return
	}

	err := a.store.DeleteAccount(jc.Request.Context(), ak)
	if errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to delete account", err) != nil {
		return
	}
}

func (a *adminAPI) handleGETState(jc jape.Context) {
	ci, err := a.store.LastScannedIndex(jc.Request.Context())
	if jc.Check("failed to get last scanned index", err) != nil {
		return
	}

	jc.Encode(State{
		StartTime: startTime,
		BuildState: BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: build.Time(),
		},
		ScanHeight: ci.Height,
	})
}

func (a *adminAPI) handleGETExplorerSiacoinExchangeRate(jc jape.Context) {
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

func (a *adminAPI) handleGETHost(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	host, err := a.store.Host(jc.Request.Context(), hk)
	if errors.Is(err, hosts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to get host", err) != nil {
		return
	}
	jc.Encode(host)
}

func (a *adminAPI) handleGETHosts(jc jape.Context) {
	offset, limit, ok := parseOffsetLimit(jc)
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

	hosts, err := a.store.Hosts(jc.Request.Context(), offset, limit, opts...)
	if jc.Check("failed to get hosts", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (a *adminAPI) handleGETHostsBlocklist(jc jape.Context) {
	offset, limit, ok := parseOffsetLimit(jc)
	if !ok {
		return
	}
	hosts, err := a.store.BlockedHosts(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get blocklist", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (a *adminAPI) handlePUTHostsBlocklist(jc jape.Context) {
	var hosts HostsBlocklistRequest
	if jc.Decode(&hosts) != nil {
		return
	}
	jc.Check("failed to add host keys to blocklist", a.store.BlockHosts(jc.Request.Context(), hosts.HostKeys, hosts.Reason))
}

func (a *adminAPI) handleDELETEHostsBlocklist(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	jc.Check("failed to unblock host", a.store.UnblockHost(jc.Request.Context(), hk))
}

func (a *adminAPI) handleGETSettingsContracts(jc jape.Context) {
	ms, err := a.store.MaintenanceSettings(jc.Request.Context())
	if jc.Check("failed to get contract settings", err) != nil {
		return
	}
	jc.Encode(ms)
}

func (a *adminAPI) handlePUTSettingsContracts(jc jape.Context) {
	var ms contracts.MaintenanceSettings
	if jc.Decode(&ms) != nil {
		return
	}
	jc.Check("failed to update contract settings", a.store.UpdateMaintenanceSettings(jc.Request.Context(), ms))
}

func (a *adminAPI) handleGETSettingsHosts(jc jape.Context) {
	s, err := a.store.UsabilitySettings(jc.Request.Context())
	if jc.Check("failed to get host settings", err) != nil {
		return
	}
	jc.Encode(s)
}

func (a *adminAPI) handlePUTSettingsHosts(jc jape.Context) {
	var s hosts.UsabilitySettings
	if jc.Decode(&s) != nil {
		return
	}
	jc.Check("failed to update host settings", a.store.UpdateUsabilitySettings(jc.Request.Context(), s))
}

func (a *adminAPI) handleGETSettingsPricePinning(jc jape.Context) {
	s, err := a.store.PinnedSettings(jc.Request.Context())
	if jc.Check("failed to get price pinning settings", err) != nil {
		return
	}
	jc.Encode(s)
}

func (a *adminAPI) handlePUTSettingsPricePinning(jc jape.Context) {
	var s pins.PinnedSettings
	if jc.Decode(&s) != nil {
		return
	} else if !(s.Currency == "" || utf8.RuneCountInString(s.Currency) == 3) {
		jc.Error(errors.New("'currency' must exactly 3 characters, or left empty to disable price pinning"), http.StatusBadRequest)
		return
	}

	jc.Check("failed to update price pinning settings", a.store.UpdatePinnedSettings(jc.Request.Context(), s))
}

func (a *adminAPI) handleGETWallet(jc jape.Context) {
	balance, err := a.wallet.Balance()
	if !a.checkServerError(jc, "failed to get wallet", err) {
		return
	}

	jc.Encode(WalletResponse{
		Balance: balance,
		Address: a.wallet.Address(),
	})
}

func (a *adminAPI) handleGETWalletEvents(jc jape.Context) {
	offset, limit, ok := parseOffsetLimit(jc)
	if !ok {
		return
	}

	events, err := a.wallet.Events(offset, limit)
	if !a.checkServerError(jc, "failed to get events", err) {
		return
	}

	jc.Encode(events)
}

func (a *adminAPI) handleGETWalletPending(jc jape.Context) {
	pending, err := a.wallet.UnconfirmedEvents()
	if !a.checkServerError(jc, "failed to get wallet pending", err) {
		return
	}
	jc.Encode(pending)
}

func (a *adminAPI) handlePOSTWalletSend(jc jape.Context) {
	var req WalletSendSiacoinsRequest
	if jc.Decode(&req) != nil {
		return
	}
	if req.Address == types.VoidAddress {
		jc.Error(errors.New("cannot send to void address"), http.StatusBadRequest)
		return
	}

	// subtract miner fee if necessary
	minerFee := a.chain.RecommendedFee().Mul64(stdTxnSize)
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

func parseOffsetLimit(jc jape.Context) (offset int, limit int, ok bool) {
	if jc.DecodeForm("offset", &offset) != nil {
		return 0, 0, false
	} else if offset < 0 {
		jc.Error(ErrInvalidOffset, http.StatusBadRequest)
		return 0, 0, false
	}

	limit = defaultLimit
	if jc.DecodeForm("limit", &limit) != nil {
		return 0, 0, false
	} else if limit < 1 || limit > maxLimit {
		jc.Error(ErrInvalidLimit, http.StatusBadRequest)
		return 0, 0, false
	}

	return offset, limit, true
}
