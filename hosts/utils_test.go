package hosts

import (
	"net/http"
	"net/http/httptest"
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
