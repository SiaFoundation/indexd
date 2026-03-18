package app

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
)

// maxRequestSize determines the maximum size of an incoming
// HTTP request body. 1MB is quite generous. A Slab with the
// maximum 255 sectors is < 50KiB
const maxRequestSize = 1 << 20 // 1 MB

type authedHandler func(jape.Context, types.PublicKey)

var (
	// ErrInternalError is returned when a signed URL can not be authenticated
	// because of an unexpected issue.
	ErrInternalError = errors.New("internal error")

	// ErrUnknownAccount is returned when a signed URL can not be authenticated
	// because the account does not exist in the account store.
	ErrUnknownAccount = errors.New("unknown account")
)

const (
	queryParamCredential = "sc"
	queryParamSignature  = "ss"
	queryParamValidUntil = "sv"
)

func maxBytesMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
		next.ServeHTTP(w, r)
	})
}

// ValidateURLSignature extracts the signed public key from the request
// and verifies the signature and expiration. The caller must check the boolean
// return value before proceeding with the request. The response header is written
// if the signature is invalid, expired, or the request is malformed. The request
// body is limited to 1 MiB
//
// NOTE: This function does not check if the account exists, it only validates
// the signature and expiration. If you need to check for account existence, use
// validateSignedURLAuth instead.
func ValidateURLSignature(r *http.Request, w http.ResponseWriter, hostname string) (types.PublicKey, bool) {
	buf, err := io.ReadAll(r.Body)
	r.Body.Close()
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return types.PublicKey{}, false
	} else if err != nil {
		http.Error(w, "failed to read request body", http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	r.Body = io.NopCloser(bytes.NewReader(buf))

	// validate presence of required parameters
	if !isSignedRequest(r) {
		http.Error(w, fmt.Sprintf("missing required query parameters: %q, %q, %q", queryParamCredential, queryParamSignature, queryParamValidUntil), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}

	// extract query string parameters
	ts, err := parseValidUntil(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid %q parameter: %v", queryParamValidUntil, err), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	pk, err := parseCredential(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid %q parameter: %v", queryParamCredential, err), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	sig, err := parseSignature(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid %q parameter: %v", queryParamSignature, err), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}

	// check for expiration and verify the signature
	if ts.Before(time.Now().UTC()) || !pk.VerifyHash(requestHash(r.Method, hostname, r.URL.Path, ts, buf), sig) {
		http.Error(w, fmt.Sprintf("invalid signature for %q host %q", r.Method, filepath.Join(hostname, r.URL.Path)), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	return pk, true
}

// validateSignedURLAuth validates a signed URL by checking its required query
// parameters, verifying the signature and expiration, and confirming the
// account exists. If any check fails, it writes an HTTP error and returns
// false, otherwise it returns the public key and true.
func validateSignedURLAuth(jc jape.Context, hostname string, store Accounts) (types.PublicKey, bool) {
	req := jc.Request

	pk, ok := ValidateURLSignature(jc.Request, jc.ResponseWriter, hostname)
	if !ok {
		return types.PublicKey{}, false
	}

	// check if the account exists
	known, err := store.HasAccount(req.Context(), pk)
	if err != nil {
		jc.Error(ErrInternalError, http.StatusInternalServerError)
		return types.PublicKey{}, false
	} else if !known {
		jc.Error(ErrUnknownAccount, http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	return pk, true
}

func parseCredential(req *http.Request) (pk types.PublicKey, _ error) {
	n, err := base64.URLEncoding.Decode(pk[:], []byte(req.URL.Query().Get(queryParamCredential)))
	if err != nil {
		return types.PublicKey{}, errors.New("invalid base64 encoding for credential")
	} else if n != len(pk) {
		return types.PublicKey{}, fmt.Errorf("invalid credential length: expected %d bytes", len(pk))
	}
	return
}

func parseSignature(req *http.Request) (sig types.Signature, _ error) {
	n, err := base64.URLEncoding.Decode(sig[:], []byte(req.URL.Query().Get(queryParamSignature)))
	if err != nil {
		return types.Signature{}, errors.New("invalid base64 encoding for signature")
	} else if n != len(sig) {
		return types.Signature{}, fmt.Errorf("invalid signature length: expected %d bytes", len(sig))
	}
	return
}

func parseValidUntil(req *http.Request) (time.Time, error) {
	tsStr := req.URL.Query().Get(queryParamValidUntil)
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp %q, must be a unix timestamp", queryParamValidUntil)
	}
	return time.Unix(ts, 0), nil
}

func isSignedRequest(req *http.Request) bool {
	return req.URL.Query().Has(queryParamCredential) &&
		req.URL.Query().Has(queryParamSignature) &&
		req.URL.Query().Has(queryParamValidUntil)
}

func requestHash(method string, hostname, path string, validUntil time.Time, body []byte) types.Hash256 {
	h := types.NewHasher()
	h.E.Write([]byte(method))
	h.E.Write([]byte(hostname))
	h.E.Write([]byte(path))
	h.E.WriteUint64(uint64(validUntil.Unix()))
	if body != nil {
		h.E.Write(body)
	}
	return h.Sum()
}

func registerAppKeyHash(ephemeralKey types.PublicKey, requestID string) types.Hash256 {
	h := types.NewHasher()
	h.E.Write([]byte("registerAppKey"))
	h.E.Write(ephemeralKey[:])
	h.E.Write([]byte(requestID))
	return h.Sum()
}
