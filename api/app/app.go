package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	// Option is a function that applies an option to the application API.
	Option func(*app)

	// Store defines the store interface for the application API.
	Store interface {
		PinSlab(context.Context, proto.Account, time.Time, slabs.SlabPinParams) (slabs.SlabID, error)
		Slab(context.Context, slabs.SlabID) (slabs.PinnedSlab, error)
		UnpinSlab(context.Context, proto.Account, slabs.SlabID) error
		UsableHosts(ctx context.Context, offset, limit int) ([]hosts.HostInfo, error)
	}

	app struct {
		store Store
		log   *zap.Logger
	}
)

// WithLogger sets the logger for application API.
func WithLogger(log *zap.Logger) Option {
	return func(api *app) {
		api.log = log
	}
}

func (a *app) handleGETHosts(jc jape.Context, _ types.PublicKey) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	hosts, err := a.store.UsableHosts(jc.Request.Context(), offset, limit)
	if jc.Check("failed to get hosts", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (a *app) handlePOSTSlabs(jc jape.Context, pk types.PublicKey) {
	var params slabs.SlabPinParams
	if err := jc.Decode(&params); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err := params.Validate(); err != nil {
		jc.Error(fmt.Errorf("invalid slab pin params: %w", err), http.StatusBadRequest)
		return
	}

	slabID, err := a.store.PinSlab(jc.Request.Context(), proto.Account(pk), time.Now(), params)
	if jc.Check("failed to pin slab", err) != nil {
		return
	}

	jc.Encode(slabID)
}

func (a *app) handleGETSlab(jc jape.Context, pk types.PublicKey) {
	var slabID slabs.SlabID
	if err := jc.DecodeParam("slabid", &slabID); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	slab, err := a.store.Slab(jc.Request.Context(), slabID)
	if errors.Is(err, slabs.ErrSlabNotFound) {
		jc.Error(slabs.ErrSlabNotFound, http.StatusNotFound)
		return
	} else if jc.Check("failed to get slab", err) != nil {
		return
	}

	jc.Encode(slab)
}

func (a *app) handleDELETESlab(jc jape.Context, pk types.PublicKey) {
	var slabID slabs.SlabID
	if err := jc.DecodeParam("slabid", &slabID); err != nil {
		return
	}

	err := a.store.UnpinSlab(jc.Request.Context(), proto.Account(pk), slabID)
	if jc.Check("failed to unpin slab", err) != nil {
		return
	}

	jc.Encode(nil)
}

// NewAPI creates a new instance of the application API. This API is used by
// users, or rather their applications, to pin slabs to the indexer.
// Authentication happens through presigned URLs that are signed with a private
// key that corresponds to a previously registered public key.
func NewAPI(hostname string, store Store, as AccountStore, opts ...Option) http.Handler {
	a := &app{
		store: store,
		log:   zap.NewNop(),
	}
	for _, opt := range opts {
		opt(a)
	}

	routes := map[string]authedHandler{
		"GET /hosts": a.handleGETHosts,

		"POST /slabs":           a.handlePOSTSlabs,
		"GET /slabs/:slabid":    a.handleGETSlab,
		"DELETE /slabs/:slabid": a.handleDELETESlab,
	}

	signed := make(map[string]jape.Handler)
	for path, handler := range routes {
		signed[path] = func(jc jape.Context) {
			pk, ok := checkSignedURLAuth(jc, hostname, as)
			if !ok {
				return
			}
			handler(jc, pk)
		}
	}
	return jape.Mux(signed)
}
