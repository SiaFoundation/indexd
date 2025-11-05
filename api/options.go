package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

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

	// ErrInvalidSortPair is returned when the requested sort parameters are
	// invalid.
	ErrInvalidSortPair = fmt.Errorf("'sortby' must be a valid field and 'desc' must be a boolean")

	// ErrMissingSortPair is returned when only one of the sort parameters is
	// provided.
	ErrMissingSortPair = errors.New("must provide both 'sortby' and 'desc' parameters")
)

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

// WithServiceAccount sets the 'serviceaccount' parameter.
func WithServiceAccount(serviceAccount bool) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("serviceaccount", fmt.Sprint(serviceAccount))
	}
}

// WithConnectKey sets the 'connectkey' parameter.
func WithConnectKey(connectKey string) URLQueryParameterOption {
	return func(q url.Values) {
		q.Set("connectkey", connectKey)
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

// SortOption represents a single sorting configuration parsed from request
// parameters.
type SortOption struct {
	Field      string
	Descending bool
}

// ParseSortOptions parses 'sortby' and 'desc' query parameters from the request
// context. It returns the parsed sort options and a boolean indicating whether
// parsing succeeded. If invalid parameters are provided an error is written to
// the response and false is returned.
func ParseSortOptions(jc jape.Context) (sorts []SortOption, ok bool) {
	sortBy := jc.Request.Form["sortby"]
	sortDesc := jc.Request.Form["desc"]
	if len(sortBy)+len(sortDesc) == 0 {
		return nil, true
	} else if len(sortBy) != len(sortDesc) {
		jc.Error(ErrMissingSortPair, http.StatusBadRequest)
		return nil, false
	}
	sorts = make([]SortOption, len(sortBy))
	for i := range sortBy {
		// validate sortby
		if sortBy[i] == "" {
			jc.Error(fmt.Errorf("%w: sortby is required", ErrInvalidSortPair), http.StatusBadRequest)
			return nil, false
		}
		// validate sort desc bool
		desc, err := strconv.ParseBool(sortDesc[i])
		if err != nil {
			jc.Error(fmt.Errorf("%w: invalid desc value %q", ErrInvalidSortPair, sortDesc[i]), http.StatusBadRequest)
			return nil, false
		}
		sorts[i] = SortOption{
			Field:      sortBy[i],
			Descending: desc,
		}
	}
	return sorts, true
}
