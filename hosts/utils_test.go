package hosts

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"go.sia.tech/coreutils/syncer"
)

type mockSyncer struct{ peers []*syncer.Peer }

func (s *mockSyncer) Peers() []*syncer.Peer { return s.peers }

func TestOnlineChecker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()

	// assert offline if no peers or fallback sites
	s := &mockSyncer{}
	c := &onlineChecker{syncer: s}
	if c.IsOnline() {
		t.Fatal("unexpected")
	}

	// assert offline if unreachable fallback site
	c.addresses = []string{"192.0.2.1:443"}
	if c.IsOnline() {
		t.Fatal("unexpected")
	}

	// assert online if reachable fallback site
	c.addresses = append(c.addresses, server.Listener.Addr().String())
	if !c.IsOnline() {
		t.Fatal("expected")
	}
	c.addresses = []string{"192.0.2.1:443"} // reset

	// assert online if peers
	s.peers = append(s.peers, &syncer.Peer{})
	if !c.IsOnline() {
		t.Fatal("expected")
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
