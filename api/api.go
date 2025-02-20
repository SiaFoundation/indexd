package api

import (
	"context"
	"net/http"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
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

	// A Store is a persistent store for the indexer.
	Store interface {
		LastScannedIndex(context.Context) (types.ChainIndex, error)
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction)
	}

	// A Wallet manages siacoins and siafunds.
	Wallet interface {
		Address() types.Address
		Balance() (balance wallet.Balance, err error)
		UnconfirmedEvents() ([]wallet.Event, error)
		Events(offset, limit int) ([]wallet.Event, error)

		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}
)

type (
	// An api provides an HTTP API for the indexer
	api struct {
		chain  ChainManager
		store  Store
		syncer Syncer
		wallet Wallet
		log    *zap.Logger
	}
)

// NewServer initializes the API
func NewServer(chain ChainManager, syncer Syncer, wallet Wallet, store Store, opts ...ServerOption) http.Handler {
	a := &api{
		chain:  chain,
		store:  store,
		syncer: syncer,
		wallet: wallet,
		log:    zap.NewNop(),
	}
	for _, opt := range opts {
		opt(a)
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /state": a.handleGETState,

		// wallet endpoints
		"GET /wallet":         a.handleGETWallet,
		"GET /wallet/events":  a.handleGETWalletEvents,
		"GET /wallet/pending": a.handleGETWalletPending,
		"POST /wallet/send":   a.handlePOSTWalletSend,
	})
}
