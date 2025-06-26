package api

import (
	"fmt"
	"net/url"

	"go.uber.org/zap"
)

type (
	// API is the interface that all API types must implement.
	// It is used to apply options to the API.
	API interface {
		applyOption(Option)
	}

	// Option is an interface for options that can be applied to the API.
	Option interface {
		applyToAdmin(*adminAPI)
		applyToApplication(*applicationAPI)
	}

	loggerOption   struct{ log *zap.Logger }
	explorerOption struct{ e Explorer }
)

func (o loggerOption) applyToAdmin(api *adminAPI)             { api.log = o.log }
func (o loggerOption) applyToApplication(api *applicationAPI) { api.log = o.log }

func (e explorerOption) applyToAdmin(api *adminAPI)             { api.explorer = e.e }
func (e explorerOption) applyToApplication(api *applicationAPI) {}

// WithExplorer sets the explorer for the API.
func WithExplorer(e Explorer) Option {
	return explorerOption{e: e}
}

// WithLogger sets the logger for the server.
func WithLogger(log *zap.Logger) Option {
	return loggerOption{log: log}
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

// HostQueryParameterOption is an option to configure the query string for the
// Hosts endpoint.
type HostQueryParameterOption URLQueryParameterOption

// WithBlocked sets the 'blocked' parameter.
func WithBlocked(blocked bool) HostQueryParameterOption {
	return func(q url.Values) {
		q.Set("blocked", fmt.Sprint(blocked))
	}
}

// WithUsable sets the 'usable' parameter.
func WithUsable(usable bool) HostQueryParameterOption {
	return func(q url.Values) {
		q.Set("usable", fmt.Sprint(usable))
	}
}

// WithActiveContracts sets the 'activecontracts' parameter.
func WithActiveContracts(activeContracts bool) HostQueryParameterOption {
	return func(q url.Values) {
		q.Set("activecontracts", fmt.Sprint(activeContracts))
	}
}
