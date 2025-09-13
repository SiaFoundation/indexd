package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/jape"
)

type mockAccounts struct{ tokens map[types.PublicKey]struct{} }

func (s *mockAccounts) HasAccount(_ context.Context, ak types.PublicKey) (bool, error) {
	_, found := s.tokens[ak]
	return found, nil
}

func (s *mockAccounts) Account(_ context.Context, ak types.PublicKey) (accounts.Account, error) {
	_, found := s.tokens[ak]
	if !found {
		return accounts.Account{}, accounts.ErrNotFound
	}
	return accounts.Account{}, nil
}

func (s *mockAccounts) ValidAppConnectKey(context.Context, string) (bool, error) {
	return true, nil
}

func (s *mockAccounts) UseAppConnectKey(ctx context.Context, connectKey string, appKey types.PublicKey, meta accounts.AccountMeta) error {
	return nil
}

func TestAuth(t *testing.T) {
	const hostname = "indexer.sia.tech"
	const path = "/foo"
	sk := types.GeneratePrivateKey()
	s := &mockAccounts{tokens: map[types.PublicKey]struct{}{sk.PublicKey(): {}}}

	server := httptest.NewServer(jape.Mux(map[string]jape.Handler{
		"GET /foo": func(jc jape.Context) {
			if _, ok := validateSignedURLAuth(jc, hostname, path, s); ok {
				jc.ResponseWriter.WriteHeader(http.StatusOK)
			}
		},
		"POST /foo": func(jc jape.Context) {
			if _, ok := validateSignedURLAuth(jc, hostname, path, s); ok {
				jc.ResponseWriter.WriteHeader(http.StatusOK)
			}
		},
	}))
	defer server.Close()

	doRequest := func(method string, params url.Values, buf []byte) (int, string) {
		var body io.Reader = http.NoBody
		if buf != nil {
			body = bytes.NewReader(buf)
		}
		t.Helper()
		req, err := http.NewRequest(method, server.URL+"/foo", body)
		if err != nil {
			t.Fatal(err)
		}
		req.URL.RawQuery = params.Encode()

		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		var bytes []byte
		if resp.StatusCode != http.StatusOK {
			var err error
			bytes, err = io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, string(bytes)
	}

	validUntil := time.Now().Add(time.Hour)
	goodParams := func(method string, body []byte) url.Values {
		val := url.Values{}
		val.Set(queryParamValidUntil, fmt.Sprint(validUntil.Unix()))
		val.Set(queryParamCredential, sk.PublicKey().String())
		val.Set(queryParamSignature, sk.SignHash(requestHash(method, hostname, path, validUntil, body)).String())
		return val
	}

	// assert authorized if everything is valid
	status, errorMsg := doRequest(http.MethodGet, goodParams(http.MethodGet, nil), nil)
	if status != http.StatusOK {
		t.Fatal("unexpected", status, errorMsg)
	}

	// assert authorized if the body does match
	params := goodParams(http.MethodPost, []byte("hello, world!"))
	status, errorMsg = doRequest(http.MethodPost, params, []byte("hello, world!"))
	if errorMsg != "" {
		t.Fatal("unexpected", errorMsg)
	} else if status != http.StatusOK {
		t.Fatal("unexpected", status)
	}

	// assert unauthorized if no auth was set
	status, _ = doRequest(http.MethodGet, url.Values{}, nil)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	}

	// assert unauthorized if 'SiaIdx-ValidUntil' is invalid
	params = goodParams(http.MethodGet, nil)
	params.Set(queryParamValidUntil, "invalid")
	status, errorMsg = doRequest(http.MethodGet, params, nil)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "must be a unix timestamp") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if 'SiaIdx-Credential' is invalid
	params = goodParams(http.MethodGet, nil)
	params.Set(queryParamCredential, "invalid")
	status, errorMsg = doRequest(http.MethodGet, params, nil)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "must be a valid public key") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if 'SiaIdx-Signature' is invalid
	params = goodParams(http.MethodGet, nil)
	params.Set(queryParamSignature, "invalid")
	status, errorMsg = doRequest(http.MethodGet, params, nil)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "must be a 64-byte hex string") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if the timestamp is in the past
	params = goodParams(http.MethodGet, nil)
	params.Set(queryParamValidUntil, fmt.Sprint(time.Now().Unix()-1))
	status, errorMsg = doRequest(http.MethodGet, params, nil)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, ErrSignatureExpired.Error()) {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if the body doesn't match
	params = goodParams(http.MethodPost, []byte("invalid"))
	status, errorMsg = doRequest(http.MethodPost, params, []byte("hello, world!"))
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "invalid signature") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if the method doesn't match
	params = goodParams(http.MethodPut, []byte("hello, world!"))
	status, errorMsg = doRequest(http.MethodPost, params, []byte("hello, world!"))
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "invalid signature") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if the account is unknown
	s.tokens = map[types.PublicKey]struct{}{}
	status, errorMsg = doRequest(http.MethodGet, goodParams(http.MethodGet, nil), nil)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, ErrUnknownAccount.Error()) {
		t.Fatal("unexpected", errorMsg)
	}
}
