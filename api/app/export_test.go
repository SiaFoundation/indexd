package app

import (
	"context"
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// ListObjectsBinary fetches the expanded object listing in the Sia binary
// encoding so external tests can cover the binary listing path.
func (c *Client) ListObjectsBinary(ctx context.Context, appKey types.PrivateKey, cursor slabs.Cursor, limit int) (resp []slabs.ObjectEvent, err error) {
	err = c.signedRequestBinary(ctx, appKey, http.MethodGet, listObjectsRoute(cursor, limit, true), nil, (*slabs.ObjectEvents)(&resp))
	return
}
