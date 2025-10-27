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

	// ErrSignatureExpired is returned when a signed URL can not be
	// authenticated because the valid until timestamp is in the past.
	ErrSignatureExpired = errors.New("signature expired")

	// ErrSignatureInvalid is returned when a signed URL can not be
	// authenticated because the signature was invalid.
	ErrSignatureInvalid = errors.New("invalid signature")

	// ErrUnknownAccount is returned when a signed URL can not be authenticated
	// because the account does not exist in the account store.
	ErrUnknownAccount = errors.New("unknown account")
)

const (
	queryParamCredential = "sc"
	queryParamSignature  = "ss"
	queryParamValidUntil = "sv"
)

// validateURLSignature extracts the signed public key from the request
// and verifies the signature and expiration. If successful, it returns the
// public key and true, otherwise it writes an error to the context and returns
// an empty public key and false.
func validateURLSignature(jc jape.Context, hostname, path string) (types.PublicKey, bool) {
	req := jc.Request
	defer req.Body.Close()

	buf, err := io.ReadAll(io.LimitReader(req.Body, maxRequestSize))
	if err != nil {
		jc.Error(errors.New("failed to read request body"), http.StatusBadRequest)
		return types.PublicKey{}, false
	}
	req.Body = io.NopCloser(bytes.NewReader(buf))

	// validate presence of required parameters
	if !isSignedRequest(req) {
		err := fmt.Errorf("missing required query parameters: %q, %q, %q", queryParamCredential, queryParamSignature, queryParamValidUntil)
		jc.Error(err, http.StatusUnauthorized)
		return types.PublicKey{}, false
	}

	// extract query string parameters
	ts, err := parseValidUntil(req)
	if err != nil {
		jc.Error(err, http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	pk, err := parseCredential(req)
	if err != nil {
		jc.Error(err, http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	sig, err := parseSignature(req)
	if err != nil {
		jc.Error(err, http.StatusUnauthorized)
		return types.PublicKey{}, false
	}

	// check for expiration and verify the signature
	if ts.Before(time.Now().UTC()) {
		jc.Error(ErrSignatureExpired, http.StatusUnauthorized)
		return types.PublicKey{}, false
	} else if !pk.VerifyHash(requestHash(req.Method, hostname, path, ts, buf), sig) {
		jc.Error(fmt.Errorf("failed to authenticate for %q host %q: %w", req.Method, filepath.Join(hostname, path), ErrSignatureInvalid), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	return pk, true
}

// validateSignedURLAuth validates a signed URL by checking its required query
// parameters, verifying the signature and expiration, and confirming the
// account exists. If any check fails, it writes an HTTP error and returns
// false, otherwise it returns the public key and true.
func validateSignedURLAuth(jc jape.Context, hostname, path string, store Accounts) (types.PublicKey, bool) {
	req := jc.Request

	pk, ok := validateURLSignature(jc, hostname, path)
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
