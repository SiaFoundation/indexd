package api

import (
	"fmt"
	"net/url"

	"go.uber.org/zap"
)

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

// WithDebug sets the debug mode for the API server. In debug mode, the server
// exposes additional debug endpoints that allow triggering certain actions.
func WithDebug() ServerOption {
	return func(a *api) {
		a.debug = true
	}
}

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

// ContractQueryParameterOption is an option to configure the query string for
// the Contracts endpoint.
type ContractQueryParameterOption URLQueryParameterOption

// WithRevisable sets the 'revisable' parameter. When revisable is set to
// 'true', only contracts that can still be used will be returned.
func WithRevisable(revisable bool) ContractQueryParameterOption {
	return func(q url.Values) {
		q.Set("revisable", fmt.Sprint(revisable))
	}
}

// WithGood sets the 'good' parameter. When good is set to 'true', all contracts
// that are considered bad will be filtered out. Examples of such contracts are
// contracts with blocked hosts or those that failed to renew when nearing their
// ProofHeight.
func WithGood(good bool) ContractQueryParameterOption {
	return func(q url.Values) {
		q.Set("good", fmt.Sprint(good))
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
