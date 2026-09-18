package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestDoRequestHTTPError(t *testing.T) {
	do := func(h http.HandlerFunc) error {
		t.Helper()

		srv := httptest.NewServer(h)
		defer srv.Close()

		u, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatal(err)
		}

		_, err = doRequest(context.Background(), http.MethodGet, u, nil, applicationJSON)
		return err
	}

	// empty body falls back to status text
	err := do(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	} else if httpErr.StatusCode != http.StatusBadGateway {
		t.Fatal("unexpected", httpErr.StatusCode)
	} else if httpErr.Body != "" {
		t.Fatal("unexpected", httpErr.Body)
	} else if httpErr.Error() != "HTTP 502: Bad Gateway" {
		t.Fatal("unexpected", httpErr.Error())
	}

	// body is trimmed and used as the message
	err = do(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("  database is down\n"))
	})

	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	} else if httpErr.StatusCode != http.StatusInternalServerError {
		t.Fatal("unexpected", httpErr.StatusCode)
	} else if httpErr.Body != "database is down" {
		t.Fatal("unexpected", httpErr.Body)
	} else if httpErr.Error() != "HTTP 500: database is down" {
		t.Fatal("unexpected", httpErr.Error())
	}

	// non-standard status with empty body omits the trailing colon
	err = do(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(599)
	})

	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	} else if httpErr.StatusCode != 599 {
		t.Fatal("unexpected", httpErr.StatusCode)
	} else if httpErr.Body != "" {
		t.Fatal("unexpected", httpErr.Body)
	} else if httpErr.Error() != "HTTP 599" {
		t.Fatal("unexpected", httpErr.Error())
	}
}
