package admin

import (
	"fmt"
	"net/url"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/api"
	"go.uber.org/zap"
)

type (
	// Option is a function that applies an option to the admin API.
	Option func(*admin)
)

// WithExplorer sets the explorer for the admin API.
func WithExplorer(e Explorer) Option {
	return func(api *admin) {
		api.explorer = e
	}
}

// WithLogger sets the logger for admin API.
func WithLogger(log *zap.Logger) Option {
	return func(api *admin) {
		api.log = log
	}
}

// WithDebug sets the debug mode for admin API. In debug mode, the server
// exposes additional debug endpoints that allow triggering certain actions.
func WithDebug() Option {
	return func(api *admin) {
		api.debug = true
	}
}

// ContractQueryParameterOption is an option to configure the query string for
// the Contracts endpoint.
type ContractQueryParameterOption api.URLQueryParameterOption

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
type HostQueryParameterOption api.URLQueryParameterOption

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

// WithPublicKeys sets the 'hostkey' parameter (multiple times if there is more
// than one host key provided).
func WithPublicKeys(hks []types.PublicKey) HostQueryParameterOption {
	return func(q url.Values) {
		strs := make([]string, len(hks))
		for i := range hks {
			strs[i] = hks[i].String()
		}
		q["hostkey"] = strs
	}
}

// WithSort sets the 'sortby' and 'sortdir' parameter, it can be called multiple
// times to add multiple sorting options. The parameters must be provided in
// pairs, i.e. both 'sortby' and 'sortdir' must be set. Valid sort directions
// are "ASC" and "DESC".
func WithSort(sortby, sortdir string) HostQueryParameterOption {
	return func(q url.Values) {
		q.Add("sortby", sortby)
		q.Add("sortdir", sortdir)
	}
}

// AlertQueryParameterOption is an option to configure the query string for the
// Alerts endpoint.
type AlertQueryParameterOption api.URLQueryParameterOption

// WithSeverity sets the 'severity' parameter.
func WithSeverity(severity alerts.Severity) AlertQueryParameterOption {
	return func(q url.Values) {
		q.Set("severity", severity.String())
	}
}
