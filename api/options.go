package api

import (
	"fmt"
	"net/url"

	"go.uber.org/zap"
)

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

// WithExplorer sets the explorer for the API server.
func WithExplorer(e Explorer) ServerOption {
	return func(a *api) {
		a.explorer = e
	}
}

// WithLogger sets the logger for the API server.
func WithLogger(log *zap.Logger) ServerOption {
	return func(a *api) {
		a.log = log
	}
}

// URLQueryParameterOption is an option to configure the query string
// parameters.
type URLQueryParameterOption func(url.Values)

// WithOffset sets the 'offset' parameter.
func WithOffset(offset int) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("offset", fmt.Sprint(offset))
	}
}

// WithLimit sets the 'limit' parameter.
func WithLimit(limit int) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("limit", fmt.Sprint(limit))
	}
}
