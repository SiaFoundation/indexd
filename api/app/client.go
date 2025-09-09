package app

import (
	"bytes"
	"context"
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
	"go.sia.tech/indexd/objects"
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

// Sign signs the request with the appropriate headers and returns the signed URL
// and request body.
func Sign(appKey types.PrivateKey, validity time.Duration, method, endpointURL string, req any) (string, io.Reader, error) {
	u, err := url.Parse(endpointURL)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	var buf []byte
	if req != nil {
		var err error
		buf, err = json.Marshal(req)
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal request: %w", err)
		}
	}
	// prepare request hash
	validUntil := time.Now().Add(validity)
	sigHash := requestHash(method, u.Host, validUntil, buf)

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
	return u.String(), body, nil
}

func (c *Client) signedRequestCustom(ctx context.Context, accept, method, route string, data any) (io.ReadCloser, error) {
	u, body, err := Sign(c.appkey, c.validity, method, fmt.Sprintf("%s%s", c.baseURL, route), data)
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
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
func (c *Client) Object(ctx context.Context, key types.Hash256) (resp objects.Object, err error) {
	err = c.signedRequestJSON(ctx, http.MethodGet, fmt.Sprintf("/objects/%s", key), nil, &resp)
	return
}

// ListObjects lists objects for the given account that were updated after the
// the given 'after' time.
func (c *Client) ListObjects(ctx context.Context, cursor objects.Cursor, limit int) (resp []objects.Object, err error) {
	values := url.Values{}
	values.Set("limit", fmt.Sprintf("%d", limit))
	values.Set("after", cursor.After.Format(time.RFC3339Nano))
	values.Set("key", cursor.Key.String())

	err = c.signedRequestJSON(ctx, http.MethodGet, "/objects?"+values.Encode(), nil, &resp)
	return
}

// SaveObject saves the given object for the given account. If an object with
// the given key exists for an account, it is overwritten.
func (c *Client) SaveObject(ctx context.Context, obj objects.Object) (err error) {
	err = c.signedRequestJSON(ctx, http.MethodPost, "/objects", obj, nil)
	return
}

// DeleteObject deletes the object with the given key for the given account.
func (c *Client) DeleteObject(ctx context.Context, key types.Hash256) (err error) {
	err = c.signedRequestJSON(ctx, http.MethodDelete, fmt.Sprintf("/objects/%s", key), nil, nil)
	return
}

// RequestAppConnection requests an application connection to the indexer.
func (c *Client) RequestAppConnection(ctx context.Context, request RegisterAppRequest) (resp RegisterAppResponse, err error) {
	err = c.signedRequestJSON(ctx, http.MethodPost, "/auth/connect", request, &resp)
	return
}

// CheckRequestStatus checks if an auth request has been approved.
// If the auth request is still pending, it returns false.
func (c *Client) CheckRequestStatus(ctx context.Context, statusURL string) (bool, error) {
	requestURL, body, err := Sign(c.appkey, c.validity, http.MethodGet, statusURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, body)
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
	u, body, err := Sign(c.appkey, c.validity, http.MethodGet, fmt.Sprintf("%s/auth/check", c.baseURL), nil)
	if err != nil {
		return false, fmt.Errorf("failed to sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, body)
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
