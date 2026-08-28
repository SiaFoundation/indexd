package app

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/x402"

	xtypes "github.com/x402-foundation/x402/go/v2/types"
)

const (
	defaultValidity = 10 * time.Minute
)

// Client is an HTTP client for the application API of the indexer.
type Client struct {
	baseURL string

	validity time.Duration
}

type requestAppConnectionOptions struct {
	preAuthorizedKey types.PrivateKey
}

// RequestAppConnectionOption configures an application connection request.
type RequestAppConnectionOption func(*requestAppConnectionOptions)

// WithPreAuthorizedKey authorizes a connection request without interactive
// approval. The key is used only to sign the request and is not transmitted.
func WithPreAuthorizedKey(key types.PrivateKey) RequestAppConnectionOption {
	return func(opts *requestAppConnectionOptions) {
		opts.preAuthorizedKey = key
	}
}

// HTTPError is returned by the client when the server responds with a non-2xx
// status code. Callers can use errors.As to inspect StatusCode and decide
// whether to retry.
type HTTPError struct {
	StatusCode int
	Body       string
}

// Error implements the error interface.
func (e *HTTPError) Error() string {
	msg := e.Body
	if msg == "" {
		msg = http.StatusText(e.StatusCode)
	}
	if msg == "" {
		return fmt.Sprintf("HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, msg)
}

// sign signs the request with the appropriate headers and returns the signed URL
// and request body.
func sign(appKey types.PrivateKey, validUntil time.Time, method, endpointURL string, requestBuf []byte) (*url.URL, io.Reader, error) {
	u, err := url.Parse(endpointURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// prepare request hash
	sigHash := requestHash(method, u.Host, u.Path, validUntil, requestBuf)

	// prepare query parameters
	val := url.Values{}
	val.Set(queryParamValidUntil, fmt.Sprint(validUntil.Unix()))

	pk := appKey.PublicKey()
	val.Set(queryParamCredential, base64.URLEncoding.EncodeToString(pk[:]))

	sig := appKey.SignHash(sigHash)
	val.Set(queryParamSignature, base64.URLEncoding.EncodeToString(sig[:]))

	// merge query params
	q := u.Query()
	for k, v := range val {
		for _, s := range v {
			q.Add(k, s)
		}
	}
	u.RawQuery = q.Encode()
	var body io.Reader = http.NoBody
	if requestBuf != nil {
		body = bytes.NewReader(requestBuf)
	}
	return u, body, nil
}

func doRequest(ctx context.Context, method string, u *url.URL, body io.Reader, accept string) (io.ReadCloser, error) {
	return doRequestWithHeaders(ctx, method, u, body, accept, nil)
}

func doRequestWithHeaders(ctx context.Context, method string, u *url.URL, body io.Reader, accept string, headers map[string]string) (io.ReadCloser, error) {
	if u == nil {
		return nil, errors.New("nil URL")
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", accept)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if !(200 <= r.StatusCode && r.StatusCode < 300) {
		defer r.Body.Close()
		defer io.Copy(io.Discard, r.Body)
		b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
		// a 402 carries a quote; surface it rather than a bare status
		if r.StatusCode == http.StatusPaymentRequired {
			if quote, err := decodeQuote(r.Header, b); err == nil {
				return nil, &PaymentRequiredError{Quote: quote}
			}
		}
		return nil, &HTTPError{StatusCode: r.StatusCode, Body: strings.TrimSpace(string(b))}
	} else if contentType := r.Header.Get("Content-Type"); r.StatusCode != http.StatusNoContent && accept != contentType {
		defer r.Body.Close()
		defer io.Copy(io.Discard, r.Body)
		return nil, fmt.Errorf("expected content type %s, got %s", accept, contentType)
	}

	return r.Body, nil
}

func (c *Client) signedRequestCustom(ctx context.Context, appKey types.PrivateKey, accept, method, route string, request any, headers ...map[string]string) (io.ReadCloser, error) {
	var requestBuf []byte
	if request != nil {
		var err error
		requestBuf, err = json.Marshal(request)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request data: %w", err)
		}
	}
	u, body, err := sign(appKey, time.Now().Add(c.validity), method, fmt.Sprintf("%s%s", c.baseURL, route), requestBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}
	var extra map[string]string
	if len(headers) > 0 {
		extra = headers[0]
	}
	return doRequestWithHeaders(ctx, method, u, body, accept, extra)
}

func (c *Client) signedRequestJSON(ctx context.Context, appKey types.PrivateKey, method, route string, data, resp any, headers ...map[string]string) error {
	body, err := c.signedRequestCustom(ctx, appKey, applicationJSON, method, route, data, headers...)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, body)
	defer body.Close()

	if resp == nil {
		return nil
	}
	return json.NewDecoder(body).Decode(resp)
}

func (c *Client) signedRequestBinary(ctx context.Context, appKey types.PrivateKey, method, route string, data any, resp types.DecoderFrom) error {
	body, err := c.signedRequestCustom(ctx, appKey, applicationOctetStream, method, route, data)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, body)
	defer body.Close()

	d := types.NewDecoder(io.LimitedReader{R: body, N: math.MaxInt64})
	resp.DecodeFrom(d)
	return d.Err()
}

// Hosts returns all usable hosts.
func (c *Client) Hosts(ctx context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) (hosts []hosts.HostInfo, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, "/hosts?"+values.Encode(), nil, &hosts)
	return
}

// PinSlabs pins slabs to the indexer.
func (c *Client) PinSlabs(ctx context.Context, appKey types.PrivateKey, params ...slabs.SlabPinParams) (slabIDs []slabs.SlabID, err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodPost, "/slabs", params, &slabIDs)
	return
}

// UnpinSlab unpins a slab from the indexer. A slab that is still referenced by
// one of the account's objects can not be unpinned.
func (c *Client) UnpinSlab(ctx context.Context, appKey types.PrivateKey, slabID slabs.SlabID) error {
	return c.signedRequestJSON(ctx, appKey, http.MethodDelete, fmt.Sprintf("/slabs/%s", slabID), nil, nil)
}

// Slab retrieves a slab from the indexer by its ID.
func (c *Client) Slab(ctx context.Context, appKey types.PrivateKey, slabID slabs.SlabID) (s slabs.PinnedSlab, err error) {
	err = c.signedRequestBinary(ctx, appKey, http.MethodGet, fmt.Sprintf("/slabs/%s", slabID), nil, &s)
	return
}

// PruneSlabs prunes all pinned slabs of a user not currently connected to an
// object. Use api.WithBefore to override the default cutoff (72 hours ago).
func (c *Client) PruneSlabs(ctx context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) error {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	path := "/slabs/prune"
	if q := values.Encode(); q != "" {
		path += "?" + q
	}
	return c.signedRequestJSON(ctx, appKey, http.MethodPost, path, nil, nil)
}

// SlabIDs fetches the digests of slabs associated with the account. It supports
// pagination through the provided options.
func (c *Client) SlabIDs(ctx context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) (resp []slabs.SlabID, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, "/slabs?"+values.Encode(), nil, &resp)
	return
}

// Object retrieves the object with the given key for the given account.
func (c *Client) Object(ctx context.Context, appKey types.PrivateKey, objectID types.Hash256) (resp slabs.SealedObject, err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, fmt.Sprintf("/objects/%s", objectID), nil, &resp)
	return
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (c *Client) ListObjects(ctx context.Context, appKey types.PrivateKey, cursor slabs.Cursor, limit int) (resp []slabs.ObjectEvent, err error) {
	values := url.Values{}
	values.Set("limit", fmt.Sprintf("%d", limit))
	values.Set("after", cursor.After.Format(time.RFC3339Nano))
	values.Set("key", cursor.Key.String())

	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, "/objects?"+values.Encode(), nil, &resp)
	return
}

// PinObject pins the object to the given account. If an object with
// the given key exists for an account, it is overwritten.
func (c *Client) PinObject(ctx context.Context, appKey types.PrivateKey, obj slabs.SealedObject) (err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodPost, "/objects", obj.PinRequest(), nil)
	return
}

// DeleteObject deletes the object with the given key for the given account.
// Slabs that were referenced by the object and are no longer referenced by any
// of the account's objects are unpinned and queued for deletion.
func (c *Client) DeleteObject(ctx context.Context, appKey types.PrivateKey, key types.Hash256) (err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodDelete, fmt.Sprintf("/objects/%s", key), nil, nil)
	return
}

// Account retrieves the account of the current user.
func (c *Client) Account(ctx context.Context, appKey types.PrivateKey) (resp AccountResponse, err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, "/account", nil, &resp)
	return
}

// AddSharingKey creates a sharing key for the account. The request must be
// signed by the sharing key.
func (c *Client) AddSharingKey(ctx context.Context, appKey types.PrivateKey, req sharing.KeyRequest) (key sharing.Key, err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodPost, "/sharing", req, &key)
	return
}

// SharingKey retrieves one of the account's sharing keys by its public key.
func (c *Client) SharingKey(ctx context.Context, appKey types.PrivateKey, publicKey types.PublicKey) (key sharing.Key, err error) {
	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, fmt.Sprintf("/sharing/%s", publicKey), nil, &key)
	return
}

// SharingKeys lists the account's sharing keys. It supports pagination through
// the provided options.
func (c *Client) SharingKeys(ctx context.Context, appKey types.PrivateKey, opts ...api.URLQueryParameterOption) (keys []sharing.Key, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, "/sharing?"+values.Encode(), nil, &keys)
	return
}

// DeleteSharingKey deletes one of the account's sharing keys.
func (c *Client) DeleteSharingKey(ctx context.Context, appKey types.PrivateKey, publicKey types.PublicKey) error {
	return c.signedRequestJSON(ctx, appKey, http.MethodDelete, fmt.Sprintf("/sharing/%s", publicKey), nil, nil)
}

// AddSharedObject attaches an object the account owns to one of its sharing
// keys.
func (c *Client) AddSharedObject(ctx context.Context, appKey types.PrivateKey, sharingKey types.PublicKey, req sharing.SharedObjectRequest) error {
	return c.signedRequestJSON(ctx, appKey, http.MethodPost, fmt.Sprintf("/sharing/%s/objects", sharingKey), req, nil)
}

// DeleteSharedObject detaches an object from one of the account's sharing keys.
func (c *Client) DeleteSharedObject(ctx context.Context, appKey types.PrivateKey, sharingKey types.PublicKey, objectKey types.Hash256) error {
	return c.signedRequestJSON(ctx, appKey, http.MethodDelete, fmt.Sprintf("/sharing/%s/objects/%s", sharingKey, objectKey), nil, nil)
}

// SharingKeyObjects lists the objects attached to one of the account's sharing
// keys. It supports pagination through the provided options.
func (c *Client) SharingKeyObjects(ctx context.Context, appKey types.PrivateKey, sharingKey types.PublicKey, opts ...api.URLQueryParameterOption) (objects []slabs.SealedObject, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.signedRequestJSON(ctx, appKey, http.MethodGet, fmt.Sprintf("/sharing/%s/objects?%s", sharingKey, values.Encode()), nil, &objects)
	return
}

// SharedStats returns the sharing key's aggregate totals. The request is signed
// with the sharing key's private key.
func (c *Client) SharedStats(ctx context.Context, sharingKey types.PrivateKey) (stats sharing.KeyStats, err error) {
	err = c.signedRequestJSON(ctx, sharingKey, http.MethodGet, "/shared", nil, &stats)
	return
}

// SharedObjects lists the objects the sharing key grants access to. The request
// is signed with the sharing key's private key.
func (c *Client) SharedObjects(ctx context.Context, sharingKey types.PrivateKey, opts ...SharedOption) (objects []slabs.SealedObject, err error) {
	err = c.sharedGetJSON(ctx, sharingKey, "/shared/objects", opts, &objects)
	return
}

// SharedObjectByID retrieves a single object the sharing key grants access to.
// The request is signed with the sharing key's private key.
func (c *Client) SharedObjectByID(ctx context.Context, sharingKey types.PrivateKey, objectKey types.Hash256, opts ...SharedOption) (obj slabs.SealedObject, err error) {
	err = c.sharedGetJSON(ctx, sharingKey, fmt.Sprintf("/shared/objects/%s", objectKey), opts, &obj)
	return
}

// SharedHosts lists usable hosts using the sharing key for authentication. Each
// host includes an account token the recipient can use to pay for downloads
// from that host.
func (c *Client) SharedHosts(ctx context.Context, sharingKey types.PrivateKey, opts ...SharedOption) (sharedHosts []SharedHost, err error) {
	err = c.sharedGetJSON(ctx, sharingKey, "/shared/hosts", opts, &sharedHosts)
	return
}

// CreateSharedObjectURL generates a signed URL for accessing the object with the given
// key. The URL is valid until the specified validUntil time.
func (c *Client) CreateSharedObjectURL(ctx context.Context, appKey types.PrivateKey, objectKey types.Hash256, masterKey []byte, validUntil time.Time) (string, error) {
	u, _, err := sign(appKey, validUntil, http.MethodGet, fmt.Sprintf("%s/objects/%s/shared", c.baseURL, objectKey), nil)
	if err != nil {
		return "", fmt.Errorf("failed to sign request: %w", err)
	}
	u.Fragment = fmt.Sprintf("encryption_key=%s", base64.URLEncoding.EncodeToString(masterKey))
	return u.String(), nil
}

// SharedObject retrieves an object using the pre-signed URL.
func (c *Client) SharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, []byte, error) {
	u, err := url.Parse(sharedURL)
	if err != nil {
		return slabs.SharedObject{}, nil, fmt.Errorf("failed to parse shared URL: %w", err)
	}
	if !(strings.HasPrefix(u.Path, "/objects/") && strings.HasSuffix(u.Path, "/shared")) {
		return slabs.SharedObject{}, nil, fmt.Errorf("path must start with '/objects/' and end with '/shared'")
	}
	values, err := url.ParseQuery(u.Fragment)
	if err != nil {
		return slabs.SharedObject{}, nil, fmt.Errorf("failed to parse URL fragment: %w", err)
	}

	keyStr := values.Get("encryption_key")
	encryptionKey := make([]byte, 32)
	n, err := base64.URLEncoding.Decode(encryptionKey, []byte(keyStr))
	if err != nil {
		return slabs.SharedObject{}, nil, fmt.Errorf("invalid base64 encoding for encryption key: %w", err)
	} else if n != 32 {
		return slabs.SharedObject{}, nil, fmt.Errorf("missing encryption key")
	}

	u.Fragment = ""
	var obj slabs.SharedObject
	resp, err := doRequest(ctx, http.MethodGet, u, nil, applicationJSON)
	if err != nil {
		return slabs.SharedObject{}, nil, fmt.Errorf("failed to fetch shared object: %w", err)
	}
	defer io.Copy(io.Discard, resp)
	defer resp.Close()

	dec := json.NewDecoder(resp)
	err = dec.Decode(&obj)
	return obj, encryptionKey, err
}

// RequestAppConnection requests an application connection to the indexer.
func (c *Client) RequestAppConnection(ctx context.Context, ephemeralKey types.PrivateKey, info Info, options ...RequestAppConnectionOption) (resp RegisterAppResponse, err error) {
	var opts requestAppConnectionOptions
	for _, option := range options {
		option(&opts)
	}
	if opts.preAuthorizedKey != nil && len(opts.preAuthorizedKey) != ed25519.PrivateKeySize {
		return RegisterAppResponse{}, errors.New("invalid pre-authorized private key")
	}

	request := RegisterAppRequest{Info: info}
	if opts.preAuthorizedKey != nil {
		request.PreAuthorizedKey = opts.preAuthorizedKey.PublicKey()
		proofHash := preAuthorizationHash(ephemeralKey.PublicKey(), request)
		request.PreAuthorizationSignature = opts.preAuthorizedKey.SignHash(proofHash)
	}

	requestBuf, err := json.Marshal(request)
	if err != nil {
		return RegisterAppResponse{}, fmt.Errorf("failed to marshal request data: %w", err)
	}

	u, reqBody, err := sign(ephemeralKey, time.Now().Add(c.validity), http.MethodPost, fmt.Sprintf("%s/auth/connect", c.baseURL), requestBuf)
	if err != nil {
		return RegisterAppResponse{}, fmt.Errorf("failed to sign request: %w", err)
	}

	respBody, err := doRequest(ctx, http.MethodPost, u, reqBody, applicationJSON)
	if err != nil {
		return RegisterAppResponse{}, err
	}
	defer io.Copy(io.Discard, respBody)
	defer respBody.Close()
	err = json.NewDecoder(respBody).Decode(&resp)
	return
}

// RequestStatus checks if an auth request has been approved.
// If the auth request is still pending, it returns false.
func (c *Client) RequestStatus(ctx context.Context, ephemeralKey types.PrivateKey, statusURL string) (status AuthConnectStatusResponse, err error) {
	u, _, err := sign(ephemeralKey, time.Now().Add(c.validity), http.MethodGet, statusURL, nil)
	if err != nil {
		return AuthConnectStatusResponse{}, fmt.Errorf("failed to sign request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return AuthConnectStatusResponse{}, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return AuthConnectStatusResponse{}, fmt.Errorf("failed to check app auth: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		return AuthConnectStatusResponse{}, ErrUserRejected
	case http.StatusOK:
		err = json.NewDecoder(resp.Body).Decode(&status)
		return
	default:
		buf, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			return AuthConnectStatusResponse{}, fmt.Errorf("failed to read response error: %w", err)
		}
		return AuthConnectStatusResponse{}, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, buf)
	}
}

// RegisterApp registers the application with the indexer using the provided
// app key and registration URL.
//
// The request must be signed with the ephemeral key
func (c *Client) RegisterApp(ctx context.Context, registerURL string, ephemeralKey, appKey types.PrivateKey) error {
	u, err := url.Parse(registerURL)
	if err != nil {
		return fmt.Errorf("failed to parse register URL: %w", err)
	}
	// extract request ID from path
	pathPieces := strings.Split(u.Path, "/")
	if len(pathPieces) != 5 || pathPieces[4] != "register" {
		return fmt.Errorf("invalid register URL path: %s", u.Path)
	}
	requestID := pathPieces[3]

	requestBuf, err := json.Marshal(RegisterAppKeyRequest{
		AppKey:    appKey.PublicKey(),
		Signature: appKey.SignHash(registerAppKeyHash(ephemeralKey.PublicKey(), requestID)),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request data: %w", err)
	}

	u, body, err := sign(ephemeralKey, time.Now().Add(c.validity), http.MethodPost, u.String(), requestBuf)
	if err != nil {
		return fmt.Errorf("failed to sign request: %w", err)
	}

	_, err = doRequest(ctx, http.MethodPost, u, body, applicationJSON)
	return err
}

// CheckAppAuth checks if the application is authenticated with the indexer.
// It returns true if authenticated, false if not, and an error if the request fails.
func (c *Client) CheckAppAuth(ctx context.Context, appKey types.PrivateKey) (bool, error) {
	u, body, err := sign(appKey, time.Now().Add(c.validity), http.MethodGet, fmt.Sprintf("%s/auth/check", c.baseURL), nil)
	if err != nil {
		return false, fmt.Errorf("failed to sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), body)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to check app auth: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return false, nil // not authenticated
	case http.StatusNoContent:
		return true, nil // authenticated
	default:
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			return false, fmt.Errorf("failed to read response body: %w", err)
		}
		return false, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, body)
	}
}

// ClientOption is a function that applies an option to the application API
// client.
type ClientOption func(client *Client)

// WithValidity sets the validity period for the URLs signed by the application
// API client.
func WithValidity(validity time.Duration) ClientOption {
	return func(client *Client) {
		client.validity = validity
	}
}

// NewClient creates a new AppClient that can be used to interact with the
// application API of the indexer. The address should be the full URL to the
// application API, including the scheme (e.g., "http://indexer.sia.tech").
func NewClient(address string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:  address,
		validity: defaultValidity,
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

// decodeQuote reads the quote from a 402. v2 carries it in the
// PAYMENT-REQUIRED header; the body is a fallback.
func decodeQuote(header http.Header, body []byte) (xtypes.PaymentRequired, error) {
	if h := header.Get(x402.HeaderPaymentRequired); h != "" {
		return x402.DecodePaymentRequired(h)
	}
	var quote xtypes.PaymentRequired
	if err := json.Unmarshal(body, &quote); err != nil {
		return xtypes.PaymentRequired{}, fmt.Errorf("no %s header and body is not a quote: %w", x402.HeaderPaymentRequired, err)
	}
	return quote, nil
}

// sharedGetJSON performs a signed GET, carrying any attached payment.
func (c *Client) sharedGetJSON(ctx context.Context, sharingKey types.PrivateKey, route string, opts []SharedOption, resp any) error {
	var r sharedRequest
	r.query = url.Values{}
	for _, opt := range opts {
		opt(&r)
	}
	if q := r.query.Encode(); q != "" {
		route += "?" + q
	}

	var headers map[string]string
	if r.payment != "" {
		headers = map[string]string{x402.HeaderPayment: r.payment}
	}
	return c.signedRequestJSON(ctx, sharingKey, http.MethodGet, route, nil, resp, headers)
}

// A SharedOption configures a request against the shared endpoints.
type SharedOption func(*sharedRequest)

type sharedRequest struct {
	query   url.Values
	payment string
}

// WithQuery applies query parameter options, such as pagination.
func WithQuery(opts ...api.URLQueryParameterOption) SharedOption {
	return func(r *sharedRequest) {
		for _, opt := range opts {
			opt(r.query)
		}
	}
}

// WithPayment attaches a signed x402 payment, paying for the key if it has not
// been paid for. The payload must be signed by the buyer's wallet against the
// quote from the 402; this client does not sign payments itself.
func WithPayment(payload xtypes.PaymentPayload) SharedOption {
	return func(r *sharedRequest) {
		if buf, err := json.Marshal(payload); err == nil {
			r.payment = base64.StdEncoding.EncodeToString(buf)
		}
	}
}

// A PaymentRequiredError is returned when the indexer answers with 402.
type PaymentRequiredError struct {
	Quote xtypes.PaymentRequired
}

// Error implements the error interface.
func (e *PaymentRequiredError) Error() string {
	if e.Quote.Error != "" {
		return fmt.Sprintf("payment required: %s", e.Quote.Error)
	}
	return "payment required"
}
