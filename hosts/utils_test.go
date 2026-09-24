package hosts

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestOnlineChecker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()

	for _, tt := range []struct {
		name      string
		addresses []string
		want      bool
	}{
		{"unreachable sites", []string{"invalid-address"}, false},
		{"reachable site", []string{"invalid-address", server.Listener.Addr().String()}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &onlineChecker{addresses: tt.addresses}
			if got := c.IsOnline(); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnlineCheckerCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	c := &onlineChecker{}
	if c.IsOnline() {
		t.Fatal("expected offline without any sites")
	}
	c.addresses = []string{server.Listener.Addr().String()}
	if c.IsOnline() {
		t.Fatal("expected cached offline result")
	}
	c.lastChecked = time.Now().Add(-onlineCheckInterval)
	if !c.IsOnline() {
		t.Fatal("expected online after cache expires")
	}

	server.Close()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if !c.IsOnline() {
				t.Error("expected cached online result")
			}
		})
	}
	wg.Wait()
	c.lastChecked = time.Now().Add(-onlineCheckInterval)
	if c.IsOnline() {
		t.Fatal("expected offline after cache expires")
	}
}

// mockResolverFallback is for testing the behavior of the default Resolver and
// making sure the error handling and fallback works correctly.
type mockResolverFallback struct {
	addrs []net.IPAddr
	err   error
}

func (m *mockResolverFallback) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return m.addrs, m.err
}

func TestResolver(t *testing.T) {
	mainAddr := []net.IPAddr{{IP: net.ParseIP("1.2.3.4")}}
	fallbackAddr := []net.IPAddr{{IP: net.ParseIP("5.6.7.8")}}

	for _, tt := range []struct {
		name     string
		main     *mockResolverFallback
		fallback *mockResolverFallback
		want     []net.IPAddr
		wantErr  error
	}{
		{
			name:     "main succeeds",
			main:     &mockResolverFallback{addrs: mainAddr, err: nil},
			fallback: &mockResolverFallback{addrs: fallbackAddr, err: nil},
			want:     mainAddr,
			wantErr:  nil,
		},
		{
			name:     "main fails, fallback succeeds",
			main:     &mockResolverFallback{addrs: nil, err: errors.New("main fail")},
			fallback: &mockResolverFallback{addrs: fallbackAddr, err: nil},
			want:     fallbackAddr,
			wantErr:  nil,
		},
		{
			name:     "main canceled",
			main:     &mockResolverFallback{addrs: nil, err: context.Canceled},
			fallback: &mockResolverFallback{addrs: fallbackAddr, err: nil},
			want:     nil,
			wantErr:  context.Canceled,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &resolver{
				main:     tt.main,
				fallback: tt.fallback,
			}

			got, err := r.LookupIPAddr(t.Context(), "example.com")

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got error %v, want %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
