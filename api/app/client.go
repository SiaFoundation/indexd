package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
)

const (
	defaultValidity = time.Hour
)

// Client is an HTTP client for the application API of the indexer.
type Client struct {
	baseURL string

	// the following fields are used to sign requests
	appkey   types.PrivateKey
	hostname string
}

// sign signs the request with the appropriate headers and returns the signed URL
// and request body.
func sign(appKey types.PrivateKey, method, endpointURL string, req any) (string, io.Reader, error) {
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
	validUntil := time.Now().Add(defaultValidity)
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

func (c *Client) signedRequest(ctx context.Context, method, route string, data any, resp any) error {
	u, body, err := sign(c.appkey, method, fmt.Sprintf("%s%s", c.baseURL, route), data)
	if err != nil {
		return fmt.Errorf("failed to sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, r.Body)
	defer r.Body.Close()
	if !(200 <= r.StatusCode && r.StatusCode < 300) {
		err, _ := io.ReadAll(r.Body)
		return errors.New(strings.TrimSpace(string(err)))
	}
	if resp == nil {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(resp)
}

// Hosts returns all usable hosts.
func (c *Client) Hosts(ctx context.Context, opts ...api.URLQueryParameterOption) (hosts []hosts.HostInfo, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.signedRequest(ctx, http.MethodGet, "/hosts?"+values.Encode(), nil, &hosts)
	return
}

// PinSlab pins a slab to the indexer.
func (c *Client) PinSlab(ctx context.Context, params slabs.SlabPinParams) (slabID slabs.SlabID, err error) {
	err = c.signedRequest(ctx, http.MethodPost, "/slabs", params, &slabID)
	return
}

// UnpinSlab unpins a slab from the indexer.
func (c *Client) UnpinSlab(ctx context.Context, slabID slabs.SlabID) error {
	return c.signedRequest(ctx, http.MethodDelete, fmt.Sprintf("/slabs/%s", slabID), nil, nil)
}

// Slab retrieves a slab from the indexer by its ID.
func (c *Client) Slab(ctx context.Context, slabID slabs.SlabID) (s slabs.PinnedSlab, err error) {
	err = c.signedRequest(ctx, http.MethodGet, fmt.Sprintf("/slabs/%s", slabID), nil, &s)
	return
}

// SlabIDs fetches the digests of slabs associated with the account. It supports
// pagination through the provided options.
func (c *Client) SlabIDs(ctx context.Context, opts ...api.URLQueryParameterOption) (slabIDs []slabs.SlabID, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	err = c.signedRequest(ctx, http.MethodGet, "/slabs?"+values.Encode(), nil, &slabIDs)
	return
}

// RequestAppConnection requests an application connection to the indexer.
func (c *Client) RequestAppConnection(ctx context.Context, request RegisterAppRequest) (resp RegisterAppResponse, err error) {
	err = c.signedRequest(ctx, http.MethodPost, "/auth/connect", request, &resp)
	return
}

// CheckRequestStatus checks if an auth request has been approved.
// If the auth request is still pending, it returns false.
func (c *Client) CheckRequestStatus(ctx context.Context, statusURL string) (bool, error) {
	requestURL, body, err := sign(c.appkey, http.MethodGet, statusURL, nil)
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
	u, body, err := sign(c.appkey, http.MethodGet, fmt.Sprintf("%s/auth/check", c.baseURL), nil)
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

// NewClient creates a new AppClient that can be used to interact with the
// application API of the indexer. The address should be the full URL to the
// application API, including the scheme (e.g., "http://indexer.sia.tech").
func NewClient(address string, appKey types.PrivateKey) (*Client, error) {
	parsedURL, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse address %q: %w", address, err)
	}

	return &Client{
		baseURL: address,

		appkey:   appKey,
		hostname: parsedURL.Host,
	}, nil
}
