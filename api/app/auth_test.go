package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/api"
	"go.sia.tech/jape"
	"lukechampine.com/frand"
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

func (s *mockAccounts) ValidAppConnectKey(context.Context, string) error {
	return nil
}

func (s *mockAccounts) RegisterAppKey(connectKey string, appKey types.PublicKey, meta accounts.AppMeta) error {
	return nil
}

func (s *mockAccounts) AppSecret(connectKey string, appID types.Hash256) (types.Hash256, error) {
	return frand.Entropy256(), nil
}

func TestAuthConnectRateLimit(t *testing.T) {
	s := &mockAccounts{tokens: make(map[types.PublicKey]struct{})}
	rl := api.NewIPRateLimiter(10*time.Millisecond, 2, time.Minute)
	handler, err := NewAPI("http://localhost:9982", nil, s, nil, nil, WithRateLimiter(rl))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClient(server.URL)
	req := RegisterAppRequest{
		AppID:       frand.Entropy256(),
		Name:        "test-app",
		Description: "A test app",
		ServiceURL:  "http://test-app.com",
	}

	// first 2 requests should succeed (burst)
	for i := 0; i < 2; i++ {
		if _, err := client.RequestAppConnection(context.Background(), req); err != nil {
			t.Fatalf("request %d: expected success, got %v", i, err)
		}
	}

	// 3rd request should be rate limited
	_, err = client.RequestAppConnection(context.Background(), req)
	if err == nil {
		t.Fatal("expected rate limit error")
	}
}

func TestAuth(t *testing.T) {
	sk := types.GeneratePrivateKey()
	s := &mockAccounts{tokens: map[types.PublicKey]struct{}{sk.PublicKey(): {}}}

	h := func(jc jape.Context) {
		hostname := jc.Request.Host
		path := jc.Request.URL.Path
		if _, ok := validateSignedURLAuth(jc, hostname, path, s); ok {
			jc.ResponseWriter.WriteHeader(http.StatusOK)
		}
	}
	server := httptest.NewServer(jape.Mux(map[string]jape.Handler{
		"GET /foo":  h,
		"POST /foo": h,
	}))
	defer server.Close()

	doRequest := func(method string, requestURL string, requestBody io.Reader) (int, string) {
		t.Helper()

		req, err := http.NewRequest(method, requestURL, requestBody)
		if err != nil {
			t.Fatal(err)
		}

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

	tests := []struct {
		name       string
		method     string
		validUntil time.Time
		body       []byte
		modify     func(httpMethod *string, url *url.URL, body []byte)
		ok         bool
	}{
		{
			name:       "valid",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify:     nil,
			ok:         true,
		},
		{
			name:       "valid",
			method:     "POST",
			validUntil: time.Now().Add(time.Hour),
			body:       []byte("hello world"),
			modify:     nil,
			ok:         true,
		},
		{
			name:       "missing parameters",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, u *url.URL, _ []byte) {
				u.RawQuery = ""
			},
			ok: false,
		},
		{
			name:       "invalid credential",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, u *url.URL, _ []byte) {
				values := u.Query()
				values.Set(queryParamCredential, "invalid")
				u.RawQuery = values.Encode()
			},
			ok: false,
		},
		{
			name:       "invalid signature",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, u *url.URL, _ []byte) {
				values := u.Query()
				values.Set(queryParamSignature, "invalid")
				u.RawQuery = values.Encode()
			},
			ok: false,
		},
		{
			name:       "invalid timestamp",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, u *url.URL, _ []byte) {
				values := u.Query()
				values.Set(queryParamValidUntil, "invalid")
				u.RawQuery = values.Encode()
			},
			ok: false,
		},
		{
			name:       "expired timestamp",
			method:     "GET",
			validUntil: time.Now().Add(-time.Hour),
			modify:     nil,
			ok:         false,
		},
		{
			name:       "method mismatch",
			method:     "POST",
			validUntil: time.Now().Add(time.Hour),
			modify: func(httpMethod *string, _ *url.URL, _ []byte) {
				*httpMethod = "GET"
			},
			ok: false,
		},
		{
			name:       "body mismatch",
			method:     "POST",
			validUntil: time.Now().Add(time.Hour),
			body:       []byte("hello world"),
			modify: func(_ *string, _ *url.URL, body []byte) {
				copy(body, "goodbye world")
			},
			ok: false,
		},
		{
			name:       "timestamp mismatch",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, url *url.URL, _ []byte) {
				values := url.Query()
				values.Set(queryParamValidUntil, fmt.Sprintf("%d", time.Now().Add(2*time.Hour).Unix()))
				url.RawQuery = values.Encode()
			},
			ok: false,
		},
		{
			name:       "public key mismatch",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, url *url.URL, _ []byte) {
				values := url.Query()
				cred := base64.URLEncoding.EncodeToString(frand.Bytes(32))
				values.Set(queryParamCredential, cred)
				url.RawQuery = values.Encode()
			},
			ok: false,
		},
		{
			name:       "signature mismatch",
			method:     "GET",
			validUntil: time.Now().Add(time.Hour),
			modify: func(_ *string, url *url.URL, _ []byte) {
				values := url.Query()
				sig := base64.URLEncoding.EncodeToString(frand.Bytes(64))
				values.Set(queryParamSignature, sig)
				url.RawQuery = values.Encode()
			},
			ok: false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("[%s] %s", tt.method, tt.name), func(t *testing.T) {
			u, _, err := sign(sk, tt.validUntil, tt.method, server.URL+"/foo", tt.body)
			if err != nil {
				t.Fatal(err)
			} else if tt.modify != nil {
				tt.modify(&tt.method, u, tt.body)
			}
			var body io.Reader = http.NoBody
			if tt.body != nil {
				body = bytes.NewReader(tt.body)
			}
			status, errorMsg := doRequest(tt.method, u.String(), body)
			if tt.ok && status != http.StatusOK {
				t.Fatal("unexpected", status, errorMsg)
			} else if !tt.ok && status != http.StatusUnauthorized {
				t.Fatal("expected unauthorized, got", status, errorMsg)
			}
		})
	}
}
