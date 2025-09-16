package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/jackc/pgx/v5/pgtype"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/jape"
)

const (
	defaultLimit = 100
	maxLimit     = 500
)

var (
	// ErrInvalidOffset is returned when the requested offset is invalid.
	ErrInvalidOffset = errors.New("offset must be non-negative")

	// ErrInvalidLimit is returned when the requested limit is invalid.
	ErrInvalidLimit = fmt.Errorf("limit must between 1 and %d", maxLimit)
)

type (
	// SortOptions configures sorting of query results.
	SortOptions struct {
		SortBy  string
		SortDir string
		SortCtx any
	}
)

// URLQueryParameterOption is an option to configure the query string
// parameters.
type URLQueryParameterOption func(url.Values)

// SortByDistance decorates the query to sort by distance to the given
// location.
func SortByDistance(location *pgtype.Point) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("sortby", "distance")
		q.Set("sortdir", "asc")
		q.Set("sortctx", fmt.Sprintf("(%f,%f)", location.P.X, location.P.Y))
	}
}

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

// WithServiceAccount sets the 'serviceaccount' parameter.
func WithServiceAccount(serviceAccount bool) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("serviceaccount", fmt.Sprint(serviceAccount))
	}
}

// WithProtocol sets the 'protocol' parameter in Hosts
func WithProtocol(protocol chain.Protocol) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("protocol", fmt.Sprint(protocol))
	}
}

// WithCountry sets the 'country' parameter in Hosts
func WithCountry(countryCode string) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("country", countryCode)
	}
}

// ParseOffsetLimit parses the 'offset' and 'limit' query parameters from the
// request context. It returns the offset and limit values, and a boolean
// indicating whether the parsing was successful. If the parameters are not
// present or invalid, it returns false and writes an appropriate error to the
// response body.
func ParseOffsetLimit(jc jape.Context) (offset int, limit int, ok bool) {
	if jc.DecodeForm("offset", &offset) != nil {
		return 0, 0, false
	} else if offset < 0 {
		jc.Error(ErrInvalidOffset, http.StatusBadRequest)
		return 0, 0, false
	}

	limit = defaultLimit
	if jc.DecodeForm("limit", &limit) != nil {
		return 0, 0, false
	} else if limit < 1 || limit > maxLimit {
		jc.Error(ErrInvalidLimit, http.StatusBadRequest)
		return 0, 0, false
	}

	return offset, limit, true
}

// ParseSortOptions parses the 'sortby', 'sortdir', and 'sortctx' query parameters
// from the request context. It returns the SortOptions, and a boolean indicating
// whether the parsing was successful. If the parameters are not present or
// invalid, it returns false and writes an appropriate error to the response body.
func ParseSortOptions(jc jape.Context) (opts SortOptions, ok bool) {
	if jc.DecodeForm("sortby", &opts.SortBy) != nil {
		return SortOptions{}, false
	} else if opts.SortBy == "" {
		return SortOptions{}, true
	} else if opts.SortBy != "distance" {
		jc.Error(fmt.Errorf("invalid sort by %q, must be one of [distance]", opts.SortBy), http.StatusBadRequest)
		return SortOptions{}, false
	}

	if jc.DecodeForm("sortdir", &opts.SortDir) != nil {
		return SortOptions{}, false
	} else if opts.SortDir != "" && opts.SortDir != "asc" && opts.SortDir != "desc" {
		jc.Error(fmt.Errorf("invalid sort direction %q, must be one of [asc desc]", opts.SortDir), http.StatusBadRequest)
		return SortOptions{}, false
	}

	var sortCtx string
	if jc.DecodeForm("sortctx", &sortCtx) != nil {
		return SortOptions{}, false
	} else if opts.SortBy == "distance" {
		var lat, lng float64
		if _, err := fmt.Sscanf(sortCtx, "(%f,%f)", &lat, &lng); err != nil {
			jc.Error(fmt.Errorf("invalid sort context %q for distance sorting, must be of the form (lat,lng)", sortCtx), http.StatusBadRequest)
			return SortOptions{}, false
		}
		opts.SortCtx = &pgtype.Point{
			P: pgtype.Vec2{
				X: lat,
				Y: lng,
			},
			Valid: true,
		}
	}

	return opts, true
}
