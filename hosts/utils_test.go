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

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// fakeSettingsTransport is a TransportClient that serves RPCSettings with a fixed
// set of settings.
type fakeSettingsTransport struct {
	hostKey  types.PublicKey
	settings proto4.HostSettings
}

func (t *fakeSettingsTransport) DialStream(context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		if id, err := proto4.ReadID(server); err != nil || id != proto4.RPCSettingsID {
			return
		}
		_ = proto4.WriteResponse(server, &proto4.RPCSettingsResponse{Settings: t.settings})
	}()
	return client, nil
}

func (t *fakeSettingsTransport) FrameSize() int           { return 1440 }
func (t *fakeSettingsTransport) PeerKey() types.PublicKey { return t.hostKey }
func (t *fakeSettingsTransport) Close() error             { return nil }

func TestFetchSignedSettings(t *testing.T) {
	hostKey := types.GeneratePrivateKey()
	hk := hostKey.PublicKey()

	signPrices := func(sk types.PrivateKey, validUntil time.Time) proto4.HostPrices {
		prices := proto4.HostPrices{
			ContractPrice: types.Siacoins(1),
			Collateral:    types.Siacoins(2),
			StoragePrice:  types.Siacoins(3),
			TipHeight:     100,
			ValidUntil:    validUntil,
		}
		prices.Signature = sk.SignHash(prices.SigHash())
		return prices
	}

	valid := signPrices(hostKey, time.Now().Add(time.Hour))
	unsigned := valid
	unsigned.Signature = types.Signature{}
	tampered := valid
	tampered.StoragePrice = types.ZeroCurrency

	for _, tt := range []struct {
		name    string
		prices  proto4.HostPrices
		wantErr error
	}{
		{"valid signature", valid, nil},
		// expiry is left to the PriceValidity usability check
		{"expired prices with valid signature", signPrices(hostKey, time.Now().Add(-time.Hour)), nil},
		{"unsigned prices", unsigned, proto4.ErrInvalidSignature},
		{"tampered prices", tampered, proto4.ErrInvalidSignature},
		{"prices signed by a different host key", signPrices(types.GeneratePrivateKey(), time.Now().Add(time.Hour)), proto4.ErrInvalidSignature},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transport := &fakeSettingsTransport{hostKey: hk, settings: proto4.HostSettings{Prices: tt.prices}}
			hs, err := fetchSignedSettings(t.Context(), transport, hk)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got error %v, want %v", err, tt.wantErr)
			} else if err != nil {
				return
			} else if hs.Prices.SigHash() != tt.prices.SigHash() || hs.Prices.Signature != tt.prices.Signature {
				t.Fatal("expected the host's prices to be returned")
			}
		})
	}
}

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
