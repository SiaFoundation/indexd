package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

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
