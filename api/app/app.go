package app

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
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

		ValidAppConnectKey(context.Context, string) (bool, error)
		UseAppConnectKey(context.Context, string, types.PublicKey) error

		HasAccount(context.Context, types.PublicKey) (bool, error)
	}

	// Accounts defines the account management interface for the application API.
	Accounts interface {
		TriggerAccountFunding(force bool) error
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

	// AuthConnectStatusResponse is the response body for checking the status of an
	// application connection request.
	AuthConnectStatusResponse struct {
		Approved bool `json:"approved"`
	}

	// RegisterAppResponse is the response body for registering a new application.
	// It contains the URL to redirect the user to for authentication.
	// The user must approve the request before the expiration time.
	RegisterAppResponse struct {
		ResponseURL string    `json:"responseURL"`
		StatusURL   string    `json:"statusURL"`
		Expiration  time.Time `json:"expiration"`
	}

	// ApproveAppRequest is the request body for approving or rejecting an application connection request.
	ApproveAppRequest struct {
		Approve bool `json:"approve"`
	}

	app struct {
		store    Store
		accounts Accounts
		log      *zap.Logger

		hostname     string
		advertiseURL string

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

	// ErrUserRejected is returned when a user rejects an application
	// connection request.
	ErrUserRejected = errors.New("user rejected connection request")
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
	pk, ok := validateURLSignature(jc, a.hostname)
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
	jc.Encode(RegisterAppResponse{
		ResponseURL: fmt.Sprintf("%s/auth/connect/%s", a.advertiseURL, requestID),
		StatusURL:   fmt.Sprintf("%s/auth/connect/%s/status", a.advertiseURL, requestID),
		Expiration:  expiration,
	})
}

func (a *app) handleGETAuthCheck(jc jape.Context, _ types.PublicKey) {
	jc.Encode(nil) // if we reached this point, account is already authenticated
}

func (a *app) handleGETAuthConnectStatus(jc jape.Context) {
	pk, ok := validateURLSignature(jc, a.hostname)
	if !ok {
		return
	}

	if ok, err := a.store.HasAccount(jc.Request.Context(), pk); err != nil {
		jc.Error(ErrInternalError, http.StatusInternalServerError)
		return
	} else if ok {
		jc.Encode(AuthConnectStatusResponse{
			Approved: true,
		})
		return
	}

	var requestID string
	jc.DecodeParam("requestID", &requestID)

	a.mu.Lock()
	authReq, ok := a.authRequests[requestID]
	a.mu.Unlock()
	switch {
	case !ok:
		jc.Error(fmt.Errorf("unknown request ID %q", requestID), http.StatusNotFound)
	case authReq.AppKey != pk:
		jc.Error(fmt.Errorf("invalid app key"), http.StatusBadRequest)
	case time.Now().After(authReq.Expiration):
		jc.Error(fmt.Errorf("request expired"), http.StatusGone)
	default:
		jc.Encode(AuthConnectStatusResponse{
			Approved: false,
		})
	}
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

	if err := authTemplate.Execute(jc.ResponseWriter, authReq); err != nil {
		// cannot return an error at this point, just log it
		a.log.Debug("failed to execute auth template", zap.Error(err))
	}
}

func (a *app) handlePOSTAuthConnect(jc jape.Context) {
	_, connectKey, _ := jc.Request.BasicAuth()
	ctx := jc.Request.Context()

	if jc.Request.Host != a.hostname {
		jc.Error(fmt.Errorf("invalid hostname %q", jc.Request.Host), http.StatusBadRequest)
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

	err := a.store.UseAppConnectKey(ctx, connectKey, req.AppKey)
	switch {
	case errors.Is(err, accounts.ErrExists):
		jc.Encode(nil)
	case errors.Is(err, ErrKeyExhausted):
		jc.Error(ErrKeyExhausted, http.StatusForbidden)
	case errors.Is(err, ErrKeyNotFound):
		jc.Error(ErrKeyNotFound, http.StatusUnauthorized)
	case err != nil:
		a.log.Debug("failed to use app connect key", zap.Error(err))
		jc.Error(ErrInternalError, http.StatusInternalServerError)
	default:
		if err := a.accounts.TriggerAccountFunding(false); err != nil {
			a.log.Warn("failed to trigger account funding after adding account", zap.Error(err))
			jc.Error(ErrInternalError, http.StatusInternalServerError)
			return
		}
		jc.Encode(nil)
	}
}

func (a *app) handleGETAccount(jc jape.Context, pk types.PublicKey) {
	jc.Encode(struct{}{}) // TODO: include account details, like storage usage
}

// NewAPI creates a new instance of the application API. This API is used by
// users, or rather their applications, to pin slabs to the indexer.
// Authentication happens through presigned URLs that are signed with a private
// key that corresponds to a previously registered public key.
func NewAPI(advertiseURL string, store Store, accounts Accounts, opts ...Option) (http.Handler, error) {
	u, err := url.Parse(advertiseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advertise URL %q: %w", advertiseURL, err)
	}
	a := &app{
		store:    store,
		accounts: accounts,
		log:      zap.NewNop(),

		hostname:     u.Host,
		advertiseURL: advertiseURL,
		authRequests: make(map[string]authReq),
	}
	for _, opt := range opts {
		opt(a)
	}

	wrapSignedAuth := func(h authedHandler) jape.Handler {
		return func(jc jape.Context) {
			pk, ok := validateSignedURLAuth(jc, a.hostname, store)
			if !ok {
				return
			}
			h(jc, pk)
		}
	}

	wrapBasicAuth := func(h jape.Handler) jape.Handler {
		return func(jc jape.Context) {
			_, password, ok := jc.Request.BasicAuth()
			if !ok {
				jc.Error(errors.New("missing basic auth credentials"), http.StatusUnauthorized)
				return
			}

			ok, err := store.ValidAppConnectKey(jc.Request.Context(), password)
			if errors.Is(err, ErrKeyNotFound) {
				jc.Error(errors.New("invalid app connect key"), http.StatusUnauthorized)
				return
			} else if err != nil {
				jc.Error(ErrInternalError, http.StatusInternalServerError)
				return
			} else if !ok {
				jc.Error(errors.New("no more uses"), http.StatusForbidden)
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
		"GET /account": wrapCORS(wrapSignedAuth(a.handleGETAccount)),

		"POST /auth/connect":                  a.handleAuthRegister, // register request
		"GET /auth/connect/:requestID":        a.handleGETAuthConnectUI,
		"POST /auth/connect/:requestID":       wrapBasicAuth(a.handlePOSTAuthConnect), // accept/reject
		"GET /auth/connect/:requestID/status": wrapCORS(a.handleGETAuthConnectStatus),
		"GET /auth/check":                     wrapCORS(wrapSignedAuth(a.handleGETAuthCheck)),

		"GET /hosts":            wrapCORS(wrapSignedAuth(a.handleGETHosts)),
		"GET /slabs":            wrapCORS(wrapSignedAuth(a.handleGETSlabs)),
		"POST /slabs":           wrapCORS(wrapSignedAuth(a.handlePOSTSlabs)),
		"GET /slabs/:slabid":    wrapCORS(wrapSignedAuth(a.handleGETSlab)),
		"DELETE /slabs/:slabid": wrapCORS(wrapSignedAuth(a.handleDELETESlab)),
	}), nil
}
