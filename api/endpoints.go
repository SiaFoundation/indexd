package api

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"
	"unicode/utf8"

	"go.sia.tech/core/types"
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

func (a *api) checkServerError(jc jape.Context, context string, err error) bool {
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *api) handleGETState(jc jape.Context) {
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

func (a *api) handleGETExplorerSiacoinExchangeRate(jc jape.Context) {
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

func (a *api) handleGETHost(jc jape.Context) {
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

func (a *api) handleGETHosts(jc jape.Context) {
	offset, limit, ok := parseOffsetLimit(jc)
	if !ok {
		return
	}
	hosts, err := a.store.Hosts(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get hosts", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (a *api) handleGETHostsBlocklist(jc jape.Context) {
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

func (a *api) handlePUTHostsBlocklist(jc jape.Context) {
	var hks []types.PublicKey
	if jc.Decode(&hks) != nil {
		return
	}
	jc.Check("failed to add host keys to blocklist", a.store.BlockHosts(jc.Request.Context(), hks))
}

func (a *api) handleDELETEHostsBlocklist(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	jc.Check("failed to unblock host", a.store.UnblockHost(jc.Request.Context(), hk))
}

func (a *api) handleGETSettingsContracts(jc jape.Context) {
	ms, err := a.store.MaintenanceSettings(jc.Request.Context())
	if jc.Check("failed to get contract settings", err) != nil {
		return
	}
	jc.Encode(ms)
}

func (a *api) handlePUTSettingsContracts(jc jape.Context) {
	var ms contracts.MaintenanceSettings
	if jc.Decode(&ms) != nil {
		return
	}
	jc.Check("failed to update contract settings", a.store.UpdateMaintenanceSettings(jc.Request.Context(), ms))
}

func (a *api) handleGETSettingsHosts(jc jape.Context) {
	s, err := a.store.UsabilitySettings(jc.Request.Context())
	if jc.Check("failed to get host settings", err) != nil {
		return
	}
	jc.Encode(s)
}

func (a *api) handlePUTSettingsHosts(jc jape.Context) {
	var s hosts.UsabilitySettings
	if jc.Decode(&s) != nil {
		return
	}
	jc.Check("failed to update host settings", a.store.UpdateUsabilitySettings(jc.Request.Context(), s))
}

func (a *api) handleGETSettingsPricePinning(jc jape.Context) {
	s, err := a.store.PinnedSettings(jc.Request.Context())
	if jc.Check("failed to get price pinning settings", err) != nil {
		return
	}
	jc.Encode(s)
}

func (a *api) handlePUTSettingsPricePinning(jc jape.Context) {
	var s pins.PinnedSettings
	if jc.Decode(&s) != nil {
		return
	} else if !(s.Currency == "" || utf8.RuneCountInString(s.Currency) == 3) {
		jc.Error(errors.New("'currency' must exactly 3 characters, or left empty to disable price pinning"), http.StatusBadRequest)
		return
	}

	jc.Check("failed to update price pinning settings", a.store.UpdatePinnedSettings(jc.Request.Context(), s))
}

func (a *api) handleGETWallet(jc jape.Context) {
	balance, err := a.wallet.Balance()
	if !a.checkServerError(jc, "failed to get wallet", err) {
		return
	}

	jc.Encode(WalletResponse{
		Balance: balance,
		Address: a.wallet.Address(),
	})
}

func (a *api) handleGETWalletEvents(jc jape.Context) {
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

func (a *api) handleGETWalletPending(jc jape.Context) {
	pending, err := a.wallet.UnconfirmedEvents()
	if !a.checkServerError(jc, "failed to get wallet pending", err) {
		return
	}
	jc.Encode(pending)
}

func (a *api) handlePOSTWalletSend(jc jape.Context) {
	var req WalletSendSiacoinsRequest
	if err := jc.Decode(&req); err != nil {
		return
	} else if req.Address == types.VoidAddress {
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
	if _, err := a.chain.AddV2PoolTransactions(basis, txnset); !a.checkServerError(jc, "failed to add v2 transaction set", err) {
		a.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
		return
	}

	// broadcast the transaction
	a.syncer.BroadcastV2TransactionSet(basis, txnset)

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
