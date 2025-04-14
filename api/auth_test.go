package api

import (
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
	"go.sia.tech/jape"
)

type mockStore struct{ tokens map[types.PublicKey]struct{} }

func (s *mockStore) HasAccount(_ context.Context, ak types.PublicKey) (bool, error) {
	_, found := s.tokens[ak]
	return found, nil
}

func TestAuth(t *testing.T) {
	const hostname = "indexer.sia.tech"
	sk := types.GeneratePrivateKey()
	s := &mockStore{tokens: map[types.PublicKey]struct{}{sk.PublicKey(): {}}}

	server := httptest.NewServer(wrapSignedAPI(hostname, s, map[string]authedHandler{
		"GET /foo": func(jc jape.Context, pk types.PublicKey) {
			if pk != sk.PublicKey() {
				jc.ResponseWriter.WriteHeader(http.StatusInternalServerError)
				return
			}
			jc.ResponseWriter.WriteHeader(http.StatusOK)
		},
	}))
	defer server.Close()

	doRequest := func(params url.Values) (int, string) {
		t.Helper()
		req, err := http.NewRequest("GET", server.URL+"/foo", http.NoBody)
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
	goodParams := func() url.Values {
		val := url.Values{}
		val.Set(queryParamValidUntil, fmt.Sprint(validUntil.Unix()))
		val.Set(queryParamCredential, sk.PublicKey().String())
		val.Set(queryParamSignature, sk.SignHash(requestHash(hostname, validUntil)).String())
		return val
	}

	// assert unauthorized if no auth was set
	status, _ := doRequest(url.Values{})
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	}

	// assert unauthorized if 'SiaIdx-ValidUntil' is invalid
	params := goodParams()
	params.Set(queryParamValidUntil, "invalid")
	status, errorMsg := doRequest(params)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "must be a unix timestamp") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if 'SiaIdx-Credential' is invalid
	params = goodParams()
	params.Set(queryParamCredential, "invalid")
	status, errorMsg = doRequest(params)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "must be a valid public key") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if 'SiaIdx-Signature' is invalid
	params = goodParams()
	params.Set(queryParamSignature, "invalid")
	status, errorMsg = doRequest(params)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, "must be a 64-byte hex string") {
		t.Fatal("unexpected", errorMsg)
	}

	// assert authorized if everything is valid
	status, errorMsg = doRequest(goodParams())
	if status != http.StatusOK {
		t.Fatal("unexpected", status, errorMsg)
	}

	// assert unauthorized if the timestamp is in the past
	params = goodParams()
	params.Set(queryParamValidUntil, fmt.Sprint(time.Now().Unix()-1))
	status, errorMsg = doRequest(params)
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, ErrSignatureExpired.Error()) {
		t.Fatal("unexpected", errorMsg)
	}

	// assert unauthorized if the account is unknown
	s.tokens = map[types.PublicKey]struct{}{}
	status, errorMsg = doRequest(goodParams())
	if status != http.StatusUnauthorized {
		t.Fatal("unexpected", status)
	} else if !strings.Contains(errorMsg, ErrUnknownAccount.Error()) {
		t.Fatal("unexpected", errorMsg)
	}
}
