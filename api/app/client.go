package app

import (
	"bytes"
	"context"
	"encoding/hex"
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
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
)

const (
	defaultValidity = 10 * time.Minute
)

// Client is an HTTP client for the application API of the indexer.
type Client struct {
	baseURL string

	// the following fields are used to sign requests
	appkey   types.PrivateKey
	validity time.Duration
}

// sign signs the request with the appropriate headers and returns the signed URL
// and request body.
func sign(appKey types.PrivateKey, validUntil time.Time, method, endpointURL string, req any) (*url.URL, io.Reader, error) {
	u, err := url.Parse(endpointURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	var buf []byte
	if req != nil {
		var err error
		buf, err = json.Marshal(req)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal request: %w", err)
		}
	}
	// prepare request hash
	sigHash := requestHash(method, u.Host, u.Path, validUntil, buf)

	// prepare query parameters
	val := url.Values{}
	val.Set("SiaIdx-ValidUntil", fmt.Sprint(validUntil.Unix()))
	val.Set("SiaIdx-Credential", appKey.PublicKey().String())
	val.Set("SiaIdx-Signature", appKey.SignHash(sigHash).String())

	// merge query params
	q := u.Query()
	for k, v := range val {
		for _, s := range v {
			q.Add(k, s)
		}
	}
	u.RawQuery = q.Encode()
	var body io.Reader = http.NoBody
	if buf != nil {
		body = bytes.NewReader(buf)
	}
	return u, body, nil
}

func doRequest(ctx context.Context, method string, u *url.URL, body io.Reader, accept string) (io.ReadCloser, error) {
	if u == nil {
		return nil, errors.New("nil URL")
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", accept)

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if !(200 <= r.StatusCode && r.StatusCode < 300) {
		defer io.Copy(io.Discard, r.Body)
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return nil, errors.New(strings.TrimSpace(string(b)))
	} else if contentType := r.Header.Get("Content-Type"); r.StatusCode != http.StatusNoContent && accept != contentType {
		return nil, fmt.Errorf("expected content type %s, got %s", accept, contentType)
	}

	return r.Body, nil
}

func (c *Client) signedRequestCustom(ctx context.Context, accept, method, route string, data any) (io.ReadCloser, error) {
	u, body, err := sign(c.appkey, time.Now().Add(c.validity), method, fmt.Sprintf("%s%s", c.baseURL, route), data)
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}
	return doRequest(ctx, method, u, body, accept)
}

func (c *Client) signedRequestJSON(ctx context.Context, method, route string, data, resp any) error {
	body, err := c.signedRequestCustom(ctx, applicationJSON, method, route, data)
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

func (c *Client) signedRequestBinary(ctx context.Context, method, route string, data any, resp types.DecoderFrom) error {
	body, err := c.signedRequestCustom(ctx, applicationOctetStream, method, route, data)
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
func (c *Client) Hosts(ctx context.Context, opts ...api.URLQueryParameterOption) (hosts []hosts.HostInfo, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	err = c.signedRequestJSON(ctx, http.MethodGet, "/hosts?"+values.Encode(), nil, &hosts)
	return
}

// PinSlab pins a slab to the indexer.
func (c *Client) PinSlab(ctx context.Context, params slabs.SlabPinParams) (slabID slabs.SlabID, err error) {
	err = c.signedRequestJSON(ctx, http.MethodPost, "/slabs", params, &slabID)
	return
}

// UnpinSlab unpins a slab from the indexer.
func (c *Client) UnpinSlab(ctx context.Context, slabID slabs.SlabID) error {
	return c.signedRequestJSON(ctx, http.MethodDelete, fmt.Sprintf("/slabs/%s", slabID), nil, nil)
}

// Slab retrieves a slab from the indexer by its ID.
func (c *Client) Slab(ctx context.Context, slabID slabs.SlabID) (s slabs.PinnedSlab, err error) {
	err = c.signedRequestBinary(ctx, http.MethodGet, fmt.Sprintf("/slabs/%s", slabID), nil, &s)
	return
}

// SlabIDs fetches the digests of slabs associated with the account. It supports
// pagination through the provided options.
func (c *Client) SlabIDs(ctx context.Context, opts ...api.URLQueryParameterOption) (resp []slabs.SlabID, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	err = c.signedRequestJSON(ctx, http.MethodGet, "/slabs?"+values.Encode(), nil, &resp)
	return
}

// Object retrieves the object with the given key for the given account.
func (c *Client) Object(ctx context.Context, key types.Hash256) (resp slabs.Object, err error) {
	err = c.signedRequestJSON(ctx, http.MethodGet, fmt.Sprintf("/objects/%s", key), nil, &resp)
	return
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (c *Client) ListObjects(ctx context.Context, cursor slabs.Cursor, limit int) (resp []slabs.Object, err error) {
	values := url.Values{}
	values.Set("limit", fmt.Sprintf("%d", limit))
	values.Set("after", cursor.After.Format(time.RFC3339Nano))
	values.Set("key", cursor.Key.String())

	err = c.signedRequestJSON(ctx, http.MethodGet, "/objects?"+values.Encode(), nil, &resp)
	return
}

// SaveObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (c *Client) SaveObject(ctx context.Context, obj slabs.Object) (err error) {
	err = c.signedRequestJSON(ctx, http.MethodPost, "/objects", obj, nil)
	return
}

// DeleteObject deletes the object with the given key for the given account.
func (c *Client) DeleteObject(ctx context.Context, key types.Hash256) (err error) {
	err = c.signedRequestJSON(ctx, http.MethodDelete, fmt.Sprintf("/objects/%s", key), nil, nil)
	return
}

// Account retrieves the account of the current user.
func (c *Client) Account(ctx context.Context) (resp accounts.Account, err error) {
	err = c.signedRequestJSON(ctx, http.MethodGet, "/account", nil, &resp)
	return
}

// CreateSharedObjectURL generates a signed URL for accessing the object with the given
// key. The URL is valid until the specified validUntil time.
func (c *Client) CreateSharedObjectURL(ctx context.Context, objectKey types.Hash256, encryptionKey [32]byte, validUntil time.Time) (string, error) {
	u, _, err := sign(c.appkey, validUntil, http.MethodGet, fmt.Sprintf("%s/objects/%s/shared", c.baseURL, objectKey), nil)
	if err != nil {
		return "", fmt.Errorf("failed to sign request: %w", err)
	}
	u.Fragment = fmt.Sprintf("encryption_key=%x", encryptionKey)
	return u.String(), nil
}

// RetrieveSharedObject retrieves an object using the pre-signed URL.
func (c *Client) RetrieveSharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, error) {
	u, err := url.Parse(sharedURL)
	if err != nil {
		return slabs.SharedObject{}, fmt.Errorf("failed to parse shared URL: %w", err)
	}
	if !strings.HasPrefix(u.Path, "/objects/") && !strings.HasSuffix(u.Path, "/shared") {
		return slabs.SharedObject{}, fmt.Errorf("invalid shared URL: path must start with /objects/")
	}
	fields := strings.Split(u.Fragment, "=")
	if len(fields) != 2 || fields[0] != "encryption_key" {
		return slabs.SharedObject{}, fmt.Errorf("invalid shared URL: missing encryption key")
	}
	var encryptionKey [32]byte
	if n, err := hex.Decode(encryptionKey[:], []byte(fields[1])); err != nil {
		return slabs.SharedObject{}, fmt.Errorf("invalid shared URL: invalid encryption key: %w", err)
	} else if n != 32 {
		return slabs.SharedObject{}, fmt.Errorf("invalid shared URL: invalid encryption key length: expected 32 bytes, got %d", n)
	}

	var obj slabs.SharedObject
	resp, err := doRequest(ctx, http.MethodGet, u, nil, applicationJSON)
	if err != nil {
		return slabs.SharedObject{}, fmt.Errorf("failed to fetch shared object: %w", err)
	}
	defer io.Copy(io.Discard, resp)
	defer resp.Close()

	dec := json.NewDecoder(resp)
	err = dec.Decode(&obj)
	return obj, err
}

// RequestAppConnection requests an application connection to the indexer.
func (c *Client) RequestAppConnection(ctx context.Context, request RegisterAppRequest) (resp RegisterAppResponse, err error) {
	err = c.signedRequestJSON(ctx, http.MethodPost, "/auth/connect", request, &resp)
	return
}

// CheckRequestStatus checks if an auth request has been approved.
// If the auth request is still pending, it returns false.
func (c *Client) CheckRequestStatus(ctx context.Context, statusURL string) (bool, error) {
	requestURL, body, err := sign(c.appkey, time.Now().Add(c.validity), http.MethodGet, statusURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), body)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to check app auth: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		return false, ErrUserRejected
	case http.StatusOK:
		var connectResp AuthConnectStatusResponse
		err = json.NewDecoder(resp.Body).Decode(&connectResp)
		return connectResp.Approved, err
	default:
		buf, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			return false, fmt.Errorf("failed to read response error: %w", err)
		}
		return false, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, buf)
	}
}

// CheckAppAuth checks if the application is authenticated with the indexer.
// It returns true if authenticated, false if not, and an error if the request fails.
func (c *Client) CheckAppAuth(ctx context.Context) (bool, error) {
	u, body, err := sign(c.appkey, time.Now().Add(c.validity), http.MethodGet, fmt.Sprintf("%s/auth/check", c.baseURL), nil)
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
func NewClient(address string, appKey types.PrivateKey, opts ...ClientOption) *Client {
	c := &Client{
		baseURL: address,

		appkey:   appKey,
		validity: defaultValidity,
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}
