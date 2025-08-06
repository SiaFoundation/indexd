package app

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
	"lukechampine.com/frand"

	_ "embed"
)

type (
	// Option is a function that applies an option to the application API.
	Option func(*app)

	// Store defines the store interface for the application API.
	Store interface {
		PinSlab(context.Context, proto.Account, time.Time, slabs.SlabPinParams) (slabs.SlabID, error)
		PinnedSlab(context.Context, slabs.SlabID) (slabs.PinnedSlab, error)
		SlabIDs(ctx context.Context, accountID proto.Account, offset, limit int) ([]slabs.SlabID, error)
		UnpinSlab(context.Context, proto.Account, slabs.SlabID) error
		UsableHosts(ctx context.Context, offset, limit int) ([]hosts.HostInfo, error)

		HasAccount(ctx context.Context, ak types.PublicKey) (bool, error)
		AddAccount(ctx context.Context, pk types.PublicKey) error
	}

	authReq struct {
		Request     RegisterAppRequest
		ResponseURL string
		AppKey      types.PublicKey
		Expiration  time.Time
	}

	// A RegisterAppRequest is the request body for registering a new application.
	RegisterAppRequest struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		LogoURL     string `json:"logoURL"`
		ServiceURL  string `json:"serviceURL"`
		CallbackURL string `json:"callbackURL"`
	}

	// RegisterAppResponse is the response body for registering a new application.
	// It contains the URL to redirect the user to for authentication.
	// The user must approve the request before the expiration time.
	RegisterAppResponse struct {
		ResponseURL string    `json:"responseURL"`
		Expiration  time.Time `json:"expiration"`
	}

	// ApproveAppRequest is the request body for approving or rejecting an application connection request.
	ApproveAppRequest struct {
		Approve bool `json:"approve"`
	}

	app struct {
		store Store
		log   *zap.Logger

		hostname        string
		authRequiresTLS bool

		mu           sync.Mutex
		authRequests map[string]authReq // maps request ID to auth request
	}
)

var (
	//go:embed auth.html
	authHTML string

	authTemplate = template.Must(template.New("auth").Parse(authHTML))

	// ErrAlreadyConnected is returned when an application that
	// is already connected tries to connect again.
	ErrAlreadyConnected = errors.New("account already connected")
)

// WithLogger sets the logger for application API.
func WithLogger(log *zap.Logger) Option {
	return func(api *app) {
		api.log = log
	}
}

// WithAuthRequiresTLS sets whether the application API requires TLS when
// connecting with new apps.
func WithAuthRequiresTLS(requiresTLS bool) Option {
	return func(api *app) {
		api.authRequiresTLS = requiresTLS
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

	slab, err := a.store.PinnedSlab(jc.Request.Context(), slabID)
	if errors.Is(err, slabs.ErrSlabNotFound) {
		jc.Error(slabs.ErrSlabNotFound, http.StatusNotFound)
		return
	} else if jc.Check("failed to get slab", err) != nil {
		return
	}

	jc.Encode(slab)
}

func (a *app) handleGETSlabs(jc jape.Context, pk types.PublicKey) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	slabIDs, err := a.store.SlabIDs(jc.Request.Context(), proto.Account(pk), offset, limit)
	if jc.Check("failed to fetch slab digests", err) != nil {
		return
	}

	jc.Encode(slabIDs)
}

func (a *app) handleDELETESlab(jc jape.Context, pk types.PublicKey) {
	var slabID slabs.SlabID
	if err := jc.DecodeParam("slabid", &slabID); err != nil {
		return
	}

	err := a.store.UnpinSlab(jc.Request.Context(), proto.Account(pk), slabID)
	if errors.Is(err, slabs.ErrSlabNotFound) {
		jc.Error(fmt.Errorf("slab %s not found", slabID), http.StatusNotFound)
		return
	} else if jc.Check("failed to unpin slab", err) != nil {
		return
	}

	jc.Encode(nil)
}

func (a *app) handleAuthRegister(jc jape.Context) {
	// check whether the request is properly signed
	pk, ok := getSignedURLAuth(jc, a.hostname)
	if !ok {
		return
	}

	// check if the account is already connected
	if known, err := a.store.HasAccount(jc.Request.Context(), pk); err != nil {
		jc.Error(ErrInternalError, http.StatusInternalServerError)
		return
	} else if known {
		jc.Error(ErrAlreadyConnected, http.StatusConflict)
		return
	}

	var req RegisterAppRequest
	if err := jc.Decode(&req); err != nil {
		return
	}

	switch {
	case req.Name == "":
		jc.Error(errors.New("name is required"), http.StatusBadRequest)
		return
	case req.Description == "":
		jc.Error(errors.New("description is required"), http.StatusBadRequest)
		return
	case req.ServiceURL == "":
		jc.Error(errors.New("service URL is required"), http.StatusBadRequest)
		return
	}

	requestID := hex.EncodeToString(frand.Bytes(16))
	expiration := time.Now().Add(10 * time.Minute)

	a.mu.Lock()
	a.authRequests[requestID] = authReq{
		Request:    req,
		AppKey:     pk,
		Expiration: expiration,
	}
	a.mu.Unlock()
	time.AfterFunc(time.Until(expiration), func() {
		a.mu.Lock()
		delete(a.authRequests, requestID)
		a.mu.Unlock()
	})
	proto := "https"
	if !a.authRequiresTLS {
		proto = "http"
	}
	jc.Encode(RegisterAppResponse{
		ResponseURL: fmt.Sprintf("%s://%s/auth/connect/%s", proto, a.hostname, requestID),
		Expiration:  expiration,
	})
}

func (a *app) handleGETAuthCheck(jc jape.Context, _ types.PublicKey) {
	jc.Encode(nil) // if we reached this point, account is already authenticated
}

func (a *app) handleGETAuthConnectUI(jc jape.Context) {
	var requestID string
	jc.DecodeParam("requestID", &requestID)
	jc.ResponseWriter.Header().Set("Content-Type", "text/html")

	a.mu.Lock()
	authReq, ok := a.authRequests[requestID]
	a.mu.Unlock()
	if !ok {
		jc.Error(fmt.Errorf("unknown request ID %q", requestID), http.StatusNotFound)
		return
	}

	authTemplate.Execute(jc.ResponseWriter, authReq) // skip error check since it could be an io error
}

func (a *app) handlePOSTAuthConnect(jc jape.Context) {
	ctx := jc.Request.Context()

	if jc.Request.Host != a.hostname {
		jc.Error(fmt.Errorf("invalid hostname %q", jc.Request.Host), http.StatusBadRequest)
		return
	} else if jc.Request.TLS == nil && a.authRequiresTLS {
		jc.Error(errors.New("application API requires TLS"), http.StatusBadRequest)
		return
	}

	var requestID string
	if err := jc.DecodeParam("requestID", &requestID); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	a.mu.Lock()
	req, ok := a.authRequests[requestID]
	a.mu.Unlock()
	if !ok {
		jc.Error(fmt.Errorf("unknown request ID %q", requestID), http.StatusNotFound)
		return
	}

	var approveReq ApproveAppRequest
	if err := jc.Decode(&approveReq); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	a.mu.Lock()
	delete(a.authRequests, requestID)
	a.mu.Unlock()

	if !approveReq.Approve {
		// request is rejected, nothing to do
		jc.Encode(nil)
		return
	}

	if err := a.store.AddAccount(ctx, req.AppKey); err != nil {
		jc.Error(ErrInternalError, http.StatusInternalServerError)
		return
	}
	jc.Encode(nil)
}

// NewAPI creates a new instance of the application API. This API is used by
// users, or rather their applications, to pin slabs to the indexer.
// Authentication happens through presigned URLs that are signed with a private
// key that corresponds to a previously registered public key.
func NewAPI(hostname string, store Store, registerAppPassword string, opts ...Option) http.Handler {
	a := &app{
		store: store,
		log:   zap.NewNop(),

		hostname:        hostname,
		authRequests:    make(map[string]authReq),
		authRequiresTLS: true,
	}
	for _, opt := range opts {
		opt(a)
	}

	wrapSignedAuth := func(h authedHandler) jape.Handler {
		return func(jc jape.Context) {
			pk, ok := checkSignedURLAuth(jc, a.hostname, store)
			if !ok {
				return
			}
			h(jc, pk)
		}
	}

	wrapBasicAuth := func(h jape.Handler) jape.Handler {
		return func(jc jape.Context) {
			_, password, ok := jc.Request.BasicAuth()
			if !ok || password != registerAppPassword {
				jc.Error(errors.New("unauthorized"), http.StatusUnauthorized)
				return
			}
			h(jc)
		}
	}

	wrapCORS := func(h jape.Handler) jape.Handler {
		return func(jc jape.Context) {
			jc.ResponseWriter.Header().Set("Access-Control-Allow-Origin", "*")
			jc.ResponseWriter.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE")
			jc.ResponseWriter.Header().Set("Access-Control-Allow-Headers", "*")
			if jc.Request.Method == http.MethodOptions {
				jc.ResponseWriter.WriteHeader(http.StatusNoContent)
				return
			}
			h(jc)
		}
	}

	return jape.Mux(map[string]jape.Handler{
		"POST /auth/connect":            a.handleAuthRegister, // register request
		"GET /auth/connect/:requestID":  a.handleGETAuthConnectUI,
		"POST /auth/connect/:requestID": wrapBasicAuth(a.handlePOSTAuthConnect),         // accept/reject
		"GET /auth/check":               wrapCORS(wrapSignedAuth(a.handleGETAuthCheck)), // check auth status

		"GET /hosts":            wrapCORS(wrapSignedAuth(a.handleGETHosts)),
		"GET /slabs":            wrapCORS(wrapSignedAuth(a.handleGETSlabs)),
		"POST /slabs":           wrapCORS(wrapSignedAuth(a.handlePOSTSlabs)),
		"GET /slabs/:slabid":    wrapCORS(wrapSignedAuth(a.handleGETSlab)),
		"DELETE /slabs/:slabid": wrapCORS(wrapSignedAuth(a.handleDELETESlab)),
	})
}
