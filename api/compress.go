package api

import (
	"net/http"

	"github.com/klauspost/compress/gzhttp"
)

const (
	// ApplicationJSON is the content type of most API requests and responses.
	ApplicationJSON = "application/json"

	// ApplicationCBOR is the content type of CBOR binary API requests and
	// responses.
	ApplicationCBOR = "application/cbor"

	// TextHTML is the content type of the HTML served by the application API.
	TextHTML = "text/html"
)

// compressibleContentTypes are the response content types worth compressing.
// Responses of any other type are passed through untouched.
var compressibleContentTypes = []string{
	ApplicationJSON,
	ApplicationCBOR,
	TextHTML,
}

var compressWrapper = newCompressWrapper()

func newCompressWrapper() func(http.Handler) http.HandlerFunc {
	wrapper, err := gzhttp.NewWrapper(gzhttp.ContentTypes(compressibleContentTypes))
	if err != nil {
		panic(err) // developer error
	}
	return wrapper
}

// CompressMiddleware wraps next so its responses are compressed for clients
// that advertise support for it.
func CompressMiddleware(next http.Handler) http.Handler {
	return compressWrapper(next)
}
