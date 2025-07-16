package app

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
)

const (
	defaultValidity = time.Hour
)

// Client is an HTTP client for the application API of the indexer.
type Client struct {
	c jape.Client

	// the following fields are used to sign requests
	appkey   types.PrivateKey
	hostname string
}

func (c *Client) sign(route string) string {
	// prepare request hash
	validUntil := time.Now().Add(defaultValidity)
	h := types.NewHasher()
	h.E.Write([]byte(c.hostname))
	h.E.WriteUint64(uint64(validUntil.Unix()))
	requestHash := h.Sum()

	// prepare query parameters
	val := url.Values{}
	val.Set("SiaIdx-ValidUntil", fmt.Sprint(validUntil.Unix()))
	val.Set("SiaIdx-Credential", c.appkey.PublicKey().String())
	val.Set("SiaIdx-Signature", c.appkey.SignHash(requestHash).String())

	// parse the route (which may contain existing query parameters)
	u, err := url.Parse(route)
	if err != nil {
		panic(err) // developer error
	}

	// merge query params
	q := u.Query()
	for k, v := range val {
		for _, s := range v {
			q.Add(k, s)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// Hosts returns all usable hosts.
func (c *Client) Hosts(ctx context.Context, opts ...api.URLQueryParameterOption) (hosts []hosts.Host, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.GET(ctx, c.sign("/hosts?"+values.Encode()), &hosts)
	return
}

// PinSlab pins a slab to the indexer.
func (c *Client) PinSlab(ctx context.Context, params slabs.SlabPinParams) (slabID slabs.SlabID, err error) {
	err = c.c.POST(ctx, c.sign("/slabs"), params, &slabID)
	return
}

// UnpinSlab unpins a slab from the indexer.
func (c *Client) UnpinSlab(ctx context.Context, slabID slabs.SlabID) error {
	return c.c.DELETE(ctx, c.sign(fmt.Sprintf("/slabs/%s", slabID)))
}

// Slab retrieves a slab from the indexer by its ID.
func (c *Client) Slab(ctx context.Context, slabID slabs.SlabID) (s slabs.PinnedSlab, err error) {
	err = c.c.GET(ctx, c.sign(fmt.Sprintf("/slabs/%s", slabID)), &s)
	return
}

// SlabIDs fetches the digests of slabs associated with the account. It supports
// pagination through the provided options.
func (c *Client) SlabIDs(ctx context.Context, opts ...api.URLQueryParameterOption) ([]slabs.SlabID, error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}

	var slabIDs []slabs.SlabID
	err := c.c.GET(ctx, c.sign("/slabs?"+values.Encode()), &slabIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch slab digests: %w", err)
	}

	return slabIDs, nil
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
		c: jape.Client{BaseURL: address},

		appkey:   appKey,
		hostname: parsedURL.Hostname(),
	}, nil
}
