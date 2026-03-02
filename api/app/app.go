package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
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

	// Slabs defines the slab interface for the application API.
	Slabs interface {
		PinSlabs(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) ([]slabs.SlabID, error)
		PruneSlabs(ctx context.Context, account proto.Account) error
		PinnedSlab(ctx context.Context, account proto.Account, slabID slabs.SlabID) (slabs.PinnedSlab, error)
		SlabIDs(ctx context.Context, account proto.Account, offset, limit int) ([]slabs.SlabID, error)
		UnpinSlab(ctx context.Context, account proto.Account, slabID slabs.SlabID) error

		Object(ctx context.Context, account proto.Account, key types.Hash256) (slabs.SealedObject, error)
		DeleteObject(ctx context.Context, account proto.Account, objectKey types.Hash256) error
		SaveObject(ctx context.Context, account proto.Account, obj slabs.SealedObject) error
		ListObjects(ctx context.Context, account proto.Account, cursor slabs.Cursor, limit int) ([]slabs.ObjectEvent, error)
		SharedObject(ctx context.Context, key types.Hash256) (slabs.SharedObject, error)
	}

	// Store defines the store interface for the application API.
	Store interface {
		UsableHosts(offset, limit int, opts ...hosts.UsableHostQueryOpt) ([]hosts.HostInfo, error)
	}

	// Accounts defines the account management interface for the application API.
	Accounts interface {
		ValidAppConnectKey(context.Context, string) error
		RegisterAppKey(string, types.PublicKey, accounts.AppMeta) error
		AppSecret(connectKey string, appID types.Hash256) (types.Hash256, error)

		HasAccount(context.Context, types.PublicKey) (bool, error)
		Account(context.Context, types.PublicKey) (accounts.Account, error)
	}

	// Contracts defines the contract management interface for the application API.
	Contracts interface {
		TriggerAccountFunding(force bool) error
	}

	authReq struct {
		Request    RegisterAppRequest
		Expiration time.Time

		// set when the user approves the request
		Approved   bool
		UserSecret types.Hash256
		ConnectKey string // ties the request to the app connect key used
	}

	// A RegisterAppRequest is the request body for registering a new application.
	RegisterAppRequest struct {
		AppID       types.Hash256 `json:"appID"`
		Name        string        `json:"name"`
		Description string        `json:"description"`
		LogoURL     string        `json:"logoURL"`
		ServiceURL  string        `json:"serviceURL"`
		CallbackURL string        `json:"callbackURL"`
	}

	// AuthConnectStatusResponse is the response body for checking the status of an
	// application connection request.
	AuthConnectStatusResponse struct {
		Approved   bool          `json:"approved"`
		UserSecret types.Hash256 `json:"userSecret,omitempty"`
	}

	// RegisterAppResponse is the response body for registering a new application.
	// It contains the URL to redirect the user to for authentication.
	// The user must approve the request before the expiration time.
	RegisterAppResponse struct {
		ResponseURL string    `json:"responseURL"`
		StatusURL   string    `json:"statusURL"`
		RegisterURL string    `json:"registerURL"`
		Expiration  time.Time `json:"expiration"`
	}

	// ApproveAppRequest is the request body for approving or rejecting an application connection request.
	ApproveAppRequest struct {
		Approve bool `json:"approve"`
	}

	app struct {
		store     Store
		accounts  Accounts
		contracts Contracts
		slabs     Slabs
		log       *zap.Logger
		rl        api.RateLimiter

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

const (
	acceptHeader = "Accept"

	applicationJSON        = "application/json"
	applicationOctetStream = "application/octet-stream"
)

// WithLogger sets the logger for application API.
func WithLogger(log *zap.Logger) Option {
	return func(api *app) {
		api.log = log
	}
}

// WithRateLimiter sets the rate limiter for the /auth/connect endpoint.
func WithRateLimiter(rl api.RateLimiter) Option {
	return func(a *app) {
		a.rl = rl
	}
}

func (a *app) handleGETHosts(jc jape.Context, _ types.PublicKey) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	var opts []hosts.UsableHostQueryOpt

	var protocol string
	if jc.DecodeForm("protocol", &protocol) != nil {
		return
	} else if protocol != "" && protocol != string(siamux.Protocol) && protocol != string(quic.Protocol) {
		jc.Error(fmt.Errorf("invalid protocol %q", protocol), http.StatusBadRequest)
		return
	} else if protocol != "" {
		opts = append(opts, hosts.WithProtocol(chain.Protocol(protocol)))
	}

	var countryCode string
	if jc.DecodeForm("country", &countryCode) != nil {
		return
	} else if countryCode != "" {
		opts = append(opts, hosts.WithCountry(countryCode))
	}

	var locationStr string
	if jc.DecodeForm("location", &locationStr) != nil {
		return
	} else if locationStr != "" {
		var lat, lng float64
		if _, err := fmt.Sscanf(locationStr, "(%f,%f)", &lat, &lng); err != nil {
			jc.Error(fmt.Errorf("invalid location %q, must be of the form (lat,lng)", locationStr), http.StatusBadRequest)
			return
		}
		opts = append(opts, hosts.SortByDistance(lat, lng))
	}

	hosts, err := a.store.UsableHosts(offset, limit, opts...)
	if jc.Check("failed to get hosts", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (a *app) handleGETObject(jc jape.Context, pk types.PublicKey) {
	var key types.Hash256
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	obj, err := a.slabs.Object(jc.Request.Context(), proto.Account(pk), key)
	if errors.Is(err, slabs.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(obj)
}

func (a *app) handleGETObjectShared(jc jape.Context, _ types.PublicKey) {
	var key types.Hash256
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	obj, err := a.slabs.SharedObject(jc.Request.Context(), key)
	if errors.Is(err, slabs.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(obj)
}

func (a *app) handleGETObjects(jc jape.Context, pk types.PublicKey) {
	_, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	var key types.Hash256
	if jc.DecodeForm("key", &key) != nil {
		return
	}

	var after time.Time
	if jc.DecodeForm("after", &after) != nil {
		return
	}

	objs, err := a.slabs.ListObjects(jc.Request.Context(), proto.Account(pk), slabs.Cursor{
		After: after,
		Key:   key,
	}, limit)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(objs)
}

func (a *app) handlePOSTObjects(jc jape.Context, pk types.PublicKey) {
	var obj slabs.SealedObject
	if jc.Decode(&obj) != nil {
		return
	}

	err := a.slabs.SaveObject(jc.Request.Context(), proto.Account(pk), obj)
	if errors.Is(err, slabs.ErrObjectMetadataLimitExceeded) || errors.Is(err, slabs.ErrObjectMinimumSlabs) || errors.Is(err, slabs.ErrObjectUnpinnedSlab) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(nil)
}

func (a *app) handleDELETEObjects(jc jape.Context, pk types.PublicKey) {
	var key types.Hash256
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	err := a.slabs.DeleteObject(jc.Request.Context(), proto.Account(pk), key)
	if errors.Is(err, slabs.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(nil)
}

func (a *app) handlePOSTSlabs(jc jape.Context, pk types.PublicKey) {
	var params []slabs.SlabPinParams
	if jc.Decode(&params) != nil {
		return
	}
	for _, param := range params {
		if err := param.Validate(); err != nil {
			jc.Error(fmt.Errorf("invalid slab pin params: %w", err), http.StatusBadRequest)
			return
		}
	}

	slabIDs, err := a.slabs.PinSlabs(jc.Request.Context(), proto.Account(pk), time.Now().Add(6*time.Hour), params...)
	if errors.Is(err, slabs.ErrBadHosts) || errors.Is(err, slabs.ErrMinShards) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("failed to pin slab", err) != nil {
		return
	} else if len(slabIDs) == 0 {
		jc.Error(fmt.Errorf("PinSlabs did not return any slab IDs"), http.StatusInternalServerError)
		return
	}

	jc.Encode(slabIDs)
}

func (a *app) handlePOSTSlabsPrune(jc jape.Context, pk types.PublicKey) {
	err := a.slabs.PruneSlabs(jc.Request.Context(), proto.Account(pk))
	if jc.Check("failed to prune slabs", err) != nil {
		return
	}

	jc.Encode(nil)
}

func encodeBinary(jc jape.Context, resp types.EncoderTo) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	resp.EncodeTo(e)
	e.Flush()

	jc.ResponseWriter.Header().Set("Content-Type", applicationOctetStream)
	jc.ResponseWriter.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	buf.WriteTo(jc.ResponseWriter)
}

func (a *app) handleGETSlab(jc jape.Context, pk types.PublicKey) {
	var slabID slabs.SlabID
	if jc.DecodeParam("slabid", &slabID) != nil {
		return
	}

	slab, err := a.slabs.PinnedSlab(jc.Request.Context(), proto.Account(pk), slabID)
	if errors.Is(err, slabs.ErrSlabNotFound) {
		jc.Error(slabs.ErrSlabNotFound, http.StatusNotFound)
		return
	} else if jc.Check("failed to get slab", err) != nil {
		return
	}

	if accept := jc.Request.Header.Get(acceptHeader); accept == applicationOctetStream {
		encodeBinary(jc, slab)
		return
	}
	jc.Encode(slab)
}

func (a *app) handleGETSlabs(jc jape.Context, pk types.PublicKey) {
	offset, limit, ok := api.ParseOffsetLimit(jc)
	if !ok {
		return
	}

	slabIDs, err := a.slabs.SlabIDs(jc.Request.Context(), proto.Account(pk), offset, limit)
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

	err := a.slabs.UnpinSlab(jc.Request.Context(), proto.Account(pk), slabID)
	if errors.Is(err, slabs.ErrSlabNotFound) {
		jc.Error(fmt.Errorf("slab %s not found", slabID), http.StatusNotFound)
		return
	} else if jc.Check("failed to unpin slab", err) != nil {
		return
	}

	jc.Encode(nil)
}

// 1. app requests connection by POSTing to /auth/connect
func (a *app) handleAuthRequest(jc jape.Context) {
	var req RegisterAppRequest
	if err := jc.Decode(&req); err != nil {
		return
	}

	switch {
	case req.AppID == types.Hash256{}:
		jc.Error(errors.New("app ID is required"), http.StatusBadRequest)
		return
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
		RegisterURL: fmt.Sprintf("%s/auth/connect/%s/register", a.advertiseURL, requestID),
		Expiration:  expiration,
	})
}

func (a *app) handleGETAuthCheck(jc jape.Context, _ types.PublicKey) {
	jc.Encode(nil) // if we reached this point, account is already authenticated
}

// 2. user is redirected to /auth/connect/:requestID to approve or reject the connection.
// Serves a UI for the user to approve or reject the connection.
// Approval requires the user to enter an app connect key.
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

// 3. web UI sends approval/rejection
// If approved, a shared secret is generated.
func (a *app) handlePOSTAuthConnect(jc jape.Context) {
	if jc.Request.Host != a.hostname {
		jc.Error(fmt.Errorf("invalid hostname %q", jc.Request.Host), http.StatusBadRequest)
		return
	}

	var requestID string
	if jc.DecodeParam("requestID", &requestID) != nil {
		return
	}

	var approveReq ApproveAppRequest
	if jc.Decode(&approveReq) != nil {
		return
	}

	_, connectKey, ok := jc.Request.BasicAuth()
	if !ok || connectKey == "" {
		jc.Error(fmt.Errorf("missing basic auth password"), http.StatusBadRequest)
		return
	}

	if !approveReq.Approve {
		// user rejected the request
		a.mu.Lock()
		delete(a.authRequests, requestID)
		a.mu.Unlock()
		jc.Encode(nil)
		return
	}

	// get the auth request
	a.mu.Lock()
	defer a.mu.Unlock()
	authReq, ok := a.authRequests[requestID]
	if !ok || time.Now().After(authReq.Expiration) {
		jc.Error(fmt.Errorf("request invalid or expired"), http.StatusNotFound)
		return
	} else if authReq.Approved {
		return
	}

	// derive shared secret
	sharedSecret, err := a.accounts.AppSecret(connectKey, authReq.Request.AppID)
	if err != nil {
		jc.Error(fmt.Errorf("failed to derive app secret: %w", err), http.StatusInternalServerError)
		return
	}

	// update the auth request with the shared secret and approval status
	authReq.UserSecret = sharedSecret
	authReq.Approved = approveReq.Approve
	authReq.ConnectKey = connectKey
	a.authRequests[requestID] = authReq
	jc.Encode(nil)
}

// 4. app polls /auth/connect/:requestID/status to check if the user approved or rejected the connection.
// If the user approves, the app receives a shared secret.
func (a *app) handleGETAuthConnectStatus(jc jape.Context) {
	var requestID string
	jc.DecodeParam("requestID", &requestID)

	a.mu.Lock()
	defer a.mu.Unlock()
	authReq, ok := a.authRequests[requestID]
	if !ok || time.Now().After(authReq.Expiration) {
		jc.Error(fmt.Errorf("request invalid or expired"), http.StatusNotFound)
		return
	}
	jc.Encode(AuthConnectStatusResponse{
		Approved:   authReq.Approved,
		UserSecret: authReq.UserSecret,
	})
}

// 5. app registers the public key with indexd using a request signed
// with the derived private key
func (a *app) handleAuthRegister(jc jape.Context) {
	var requestID string
	if jc.DecodeParam("requestID", &requestID) != nil {
		return
	}

	a.mu.Lock()
	authReq, ok := a.authRequests[requestID]
	a.mu.Unlock()
	if !ok {
		jc.Error(fmt.Errorf("unknown request ID %q", requestID), http.StatusNotFound)
		return
	} else if time.Now().After(authReq.Expiration) {
		jc.Error(fmt.Errorf("request expired"), http.StatusGone)
		return
	} else if !authReq.Approved {
		jc.Error(ErrUserRejected, http.StatusForbidden)
		return
	} else if authReq.UserSecret == (types.Hash256{}) {
		panic("user secret is empty for approved request") // should never happen
	}

	// check whether the request is properly signed
	appKey, ok := validateURLSignature(jc, a.hostname, jc.Request.URL.Path)
	if !ok {
		return
	}

	err := a.accounts.RegisterAppKey(authReq.ConnectKey, appKey, accounts.AppMeta{
		ID:          authReq.Request.AppID,
		Description: authReq.Request.Description,
		LogoURL:     authReq.Request.LogoURL,
		ServiceURL:  authReq.Request.ServiceURL,
	})
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

func (a *app) handleGETAccount(jc jape.Context, pk types.PublicKey) {
	account, err := a.accounts.Account(jc.Request.Context(), pk)
	if errors.Is(err, accounts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(account)
}

// NewAPI creates a new instance of the application API. This API is used by
// users, or rather their applications, to pin slabs to the indexer.
// Authentication happens through presigned URLs that are signed with a private
// key that corresponds to a previously registered public key.
func NewAPI(advertiseURL string, store Store, am Accounts, contracts Contracts, slabs Slabs, opts ...Option) (http.Handler, error) {
	u, err := url.Parse(advertiseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advertise URL %q: %w", advertiseURL, err)
	}
	a := &app{
		store:     store,
		accounts:  am,
		contracts: contracts,
		slabs:     slabs,
		log:       zap.NewNop(),

		hostname:     u.Host,
		advertiseURL: advertiseURL,
		authRequests: make(map[string]authReq),
	}
	for _, opt := range opts {
		opt(a)
	}

	wrapSignedAuth := func(h authedHandler) jape.Handler {
		return func(jc jape.Context) {
			pk, ok := validateSignedURLAuth(jc, a.hostname, jc.Request.URL.Path, am)
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

			if err := am.ValidAppConnectKey(jc.Request.Context(), password); errors.Is(err, accounts.ErrKeyNotFound) {
				jc.Error(errors.New("invalid app connect key"), http.StatusUnauthorized)
				return
			} else if err != nil {
				jc.Error(ErrInternalError, http.StatusInternalServerError)
				return
			}

			h(jc)
		}
	}

	return corsMux(map[string]jape.Handler{
		"GET /account": wrapSignedAuth(a.handleGETAccount),

		// auth is a multi-step process designed to protect privacy while allowing arbitrary apps to connect:
		// 1. app requests connection by POSTing to /auth/connect
		// 2. user is redirected to /auth/connect/:requestID to approve or reject the connection.
		// Approval requires the user to enter their indexd credentials.
		// 3. web UI sends approval/rejection by POSTing to /auth/connect/:requestID
		// 4. app polls /auth/connect/:requestID/status to check if the user approved or rejected the connection.
		// If the user approves, the app receives a shared secret.
		// 5. once approved, the app derives an ed25519 keypair using `HKDF(user's mnemonic, its app ID, user secret)`
		// app registers the public key with indexd using a request signed with the derived private key
		"POST /auth/connect":                     api.WrapRateLimit(a.rl, a.handleAuthRequest),
		"GET /auth/connect/:requestID/status":    a.handleGETAuthConnectStatus,
		"POST /auth/connect/:requestID/register": a.handleAuthRegister,
		"GET /auth/check":                        wrapSignedAuth(a.handleGETAuthCheck),

		"GET /hosts": wrapSignedAuth(a.handleGETHosts),

		"GET /objects":             wrapSignedAuth(a.handleGETObjects),
		"GET /objects/:key":        wrapSignedAuth(a.handleGETObject),
		"GET /objects/:key/shared": wrapSignedAuth(a.handleGETObjectShared),
		"POST /objects":            wrapSignedAuth(a.handlePOSTObjects),
		"DELETE /objects/:key":     wrapSignedAuth(a.handleDELETEObjects),

		"GET /slabs":            wrapSignedAuth(a.handleGETSlabs),
		"POST /slabs":           wrapSignedAuth(a.handlePOSTSlabs),
		"POST /slabs/prune":     wrapSignedAuth(a.handlePOSTSlabsPrune),
		"GET /slabs/:slabid":    wrapSignedAuth(a.handleGETSlab),
		"DELETE /slabs/:slabid": wrapSignedAuth(a.handleDELETESlab),
	}, map[string]jape.Handler{
		// CORS is disabled on these routes because we don't want to encourage programmatic access. It can't be
		// blocked entirely, but it's less convenient without CORS support.
		"GET /auth/connect/:requestID":  a.handleGETAuthConnectUI,               // UI for accept/reject connection requests
		"POST /auth/connect/:requestID": wrapBasicAuth(a.handlePOSTAuthConnect), // API for accept/reject connection requests
	}), nil
}
