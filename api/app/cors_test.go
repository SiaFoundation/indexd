package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.sia.tech/jape"
)

func TestCORSOptions(t *testing.T) {
	// since this is for OPTIONs testing, the handlers themselves should not be called
	panicHandler := func(jc jape.Context) {
		panic("handler should not have been called")
	}
	mux := corsMux(
		map[string]jape.Handler{
			"GET /auth/connect":               panicHandler,
			"GET /auth/connect/:id/status":    panicHandler,
			"POST /auth/connect/:id/register": panicHandler,
			"GET /auth/check":                 panicHandler,
		},
		map[string]jape.Handler{
			// both disabled routes have the same path but different methods,
			// should be allowed since they would not conflict with each other
			// or the OPTIONS route.
			"GET /auth/connect/:id":  panicHandler,
			"POST /auth/connect/:id": panicHandler,
		},
	)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	enabledRoutes := []string{
		"/auth/connect",
		"/auth/connect/asdasd/status",
		"/auth/connect/asdasd/register",
		"/auth/check",
	}

	disabledRoutes := []string{
		"/auth/connect/asdasd", // both disabled routes have the same path but different methods.
	}

	expectedCORSHeaders := map[string]string{
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Methods": "GET, POST, DELETE",
		"Access-Control-Allow-Headers": "*",
	}

	doRequest := func(t *testing.T, method, path string) *http.Response {
		t.Helper()

		req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp
	}

	resp := doRequest(t, http.MethodOptions, "/non-existent")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected status code %d, got %d", http.StatusNotFound, resp.StatusCode)
	}
	for key := range expectedCORSHeaders {
		if resp.Header.Get(key) != "" {
			t.Fatalf("expected header %s to be empty, got %s", key, resp.Header.Get(key))
		}
	}

	for _, path := range enabledRoutes {
		resp := doRequest(t, http.MethodOptions, path)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected status code %d, got %d", http.StatusNoContent, resp.StatusCode)
		}
		for key, value := range expectedCORSHeaders {
			if resp.Header.Get(key) != value {
				t.Fatalf("expected header %s to be %s, got %s", key, value, resp.Header.Get(key))
			}
		}
	}

	for _, path := range disabledRoutes {
		resp := doRequest(t, http.MethodOptions, path)
		if resp.StatusCode != http.StatusOK {
			// httprouter returns 200 OK for OPTIONS requests by default
			// this is acceptable as long as the CORS headers are not present
			t.Fatalf("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
		}

		for key := range expectedCORSHeaders {
			if resp.Header.Get(key) != "" {
				t.Fatalf("expected header %s to be empty, got %s", key, resp.Header.Get(key))
			}
		}
	}
}

func TestCORSOptionsDuplicates(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic due to duplicate route, but did not panic")
		}
	}()

	// since this is for OPTIONs testing, the handlers themselves should not be called
	panicHandler := func(jc jape.Context) {
		panic("handler should not have been called")
	}

	corsMux(map[string]jape.Handler{
		"GET /auth/connect": panicHandler,
	}, map[string]jape.Handler{
		"POST /auth/:id": panicHandler,
	})
}
