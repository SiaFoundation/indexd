package api

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/build"
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
