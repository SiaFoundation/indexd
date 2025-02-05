package api

import (
	"net/http"

	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	// A ChainManager retrieves the current blockchain state
	ChainManager interface{}

	// A Store is a persistent store for the indexer.
	Store interface{}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface{}

	// An api provides an HTTP API for the indexer
	api struct {
		chain  ChainManager
		store  Store
		syncer Syncer
		log    *zap.Logger
	}
)

// NewServer initializes the API
func NewServer(chain ChainManager, syncer Syncer, store Store, opts ...ServerOption) http.Handler {
	a := &api{
		chain:  chain,
		store:  store,
		syncer: syncer,
		log:    zap.NewNop(),
	}
	for _, opt := range opts {
		opt(a)
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /state": a.handleGETState,
	})
}
