package app

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
)

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
	queryParamCredential = "SiaIdx-Credential"
	queryParamSignature  = "SiaIdx-Signature"
	queryParamValidUntil = "SiaIdx-ValidUntil"
)

// accountStore defines the interface for checking if a public key corresponds
// to a known account.
type accountStore interface {
	HasAccount(ctx context.Context, ak types.PublicKey) (bool, error)
}

// getSignedURLAuth extracts the signed public key from the request
// and verifies the signature and expiration. If successful, it returns the
// public key and true, otherwise it writes an error to the context and returns
// an empty public key and false.
func getSignedURLAuth(jc jape.Context, hostname string) (types.PublicKey, bool) {
	req := jc.Request

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
	} else if !pk.VerifyHash(requestHash(hostname, ts), sig) {
		jc.Error(fmt.Errorf("failed to authenticate for host %q: %w", hostname, ErrSignatureInvalid), http.StatusUnauthorized)
		return types.PublicKey{}, false
	}
	return pk, true
}

// checkSignedURLAuth validates a signed URL by checking its required query
// parameters, verifying the signature and expiration, and confirming the
// account exists. If any check fails, it writes an HTTP error and returns
// false, otherwise it returns the public key and true.
func checkSignedURLAuth(jc jape.Context, hostname string, store accountStore) (types.PublicKey, bool) {
	req := jc.Request

	pk, ok := getSignedURLAuth(jc, hostname)
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

func parseCredential(req *http.Request) (types.PublicKey, error) {
	credStr := req.URL.Query().Get(queryParamCredential)
	var pk types.PublicKey
	err := pk.UnmarshalText([]byte(credStr))
	if err != nil {
		return types.PublicKey{}, fmt.Errorf("invalid credential %q, must be a valid public key", queryParamCredential)
	}
	return pk, nil
}

func parseSignature(req *http.Request) (types.Signature, error) {
	sigStr := req.URL.Query().Get(queryParamSignature)
	sigBytes, err := hex.DecodeString(sigStr)
	if err != nil {
		return types.Signature{}, fmt.Errorf("invalid signature %q: must be a %d-byte hex string, %w", queryParamSignature, len(types.Signature{}), err)
	} else if len(sigBytes) != len(types.Signature{}) {
		return types.Signature{}, fmt.Errorf("invalid signature length: expected %d bytes", len(types.Signature{}))
	}

	var sig types.Signature
	copy(sig[:], sigBytes)
	return sig, nil
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

func requestHash(hostname string, validUntil time.Time) types.Hash256 {
	h := types.NewHasher()
	h.E.Write([]byte(hostname))
	h.E.WriteUint64(uint64(validUntil.Unix()))
	return h.Sum()
}
