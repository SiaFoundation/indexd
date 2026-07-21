package client

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// fakeSettingsTransport is a TransportClient that serves RPCSettings with a
// fixed set of settings and counts how often they were fetched.
type fakeSettingsTransport struct {
	hostKey  types.PrivateKey
	settings proto.HostSettings

	fetches int
}

func (t *fakeSettingsTransport) DialStream(context.Context) (net.Conn, error) {
	t.fetches++
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		if id, err := proto.ReadID(server); err != nil || id != proto.RPCSettingsID {
			return
		}
		_ = proto.WriteResponse(server, &proto.RPCSettingsResponse{Settings: t.settings})
	}()
	return client, nil
}

func (t *fakeSettingsTransport) FrameSize() int           { return 1440 }
func (t *fakeSettingsTransport) PeerKey() types.PublicKey { return t.hostKey.PublicKey() }
func (t *fakeSettingsTransport) Close() error             { return nil }

// testSettings returns host settings with prices claiming the given tip
// height, signed by the given host key.
func testSettings(hostKey types.PrivateKey, tipHeight uint64, validUntil time.Time) proto.HostSettings {
	prices := proto.HostPrices{
		TipHeight:  tipHeight,
		ValidUntil: validUntil,
	}
	prices.Signature = hostKey.SignHash(prices.SigHash())
	return proto.HostSettings{Prices: prices}
}

func TestCheckPricesHeight(t *testing.T) {
	c := &Client{heightTolerance: 10}

	tests := []struct {
		name        string
		tipHeight   uint64
		localHeight uint64
		wantErr     bool
	}{
		{"equal heights", 100, 100, false},
		{"host behind within tolerance", 95, 100, false},
		{"host behind at tolerance", 90, 100, false},
		{"host behind beyond tolerance", 89, 100, true},
		{"host far ahead is fine", 1000, 100, false},
		{"local at tolerance from genesis", 0, 10, false},
		{"local beyond tolerance from genesis", 0, 11, true},
		{"host height near max does not overflow", math.MaxUint64 - 5, 100, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.checkPricesHeight(proto.HostPrices{TipHeight: tt.tipHeight}, tt.localHeight)
			if (err != nil) != tt.wantErr {
				t.Fatalf("host height %d, local height %d: expected error %t, got %v", tt.tipHeight, tt.localHeight, tt.wantErr, err)
			} else if err != nil && !errors.Is(err, ErrPricesHeightDrift) {
				t.Fatalf("expected ErrPricesHeightDrift, got %v", err)
			}
		})
	}
}

func TestSettingsRefresh(t *testing.T) {
	hostKey := types.GeneratePrivateKey()
	hk := hostKey.PublicKey()
	validUntil := time.Now().Add(10 * time.Minute)

	const localHeight = 200
	staleSettings := testSettings(hostKey, 100, validUntil) // beyond tolerance behind localHeight
	freshSettings := testSettings(hostKey, 199, validUntil)

	transport := &fakeSettingsTransport{hostKey: hostKey, settings: freshSettings}
	c := &Client{
		heightTolerance: 36,
		cachedSettings:  map[types.PublicKey]proto.HostSettings{hk: staleSettings},
	}

	// settings returns the stale cached settings without fetching
	settings, cached, err := c.settings(context.Background(), hk, transport)
	if err != nil {
		t.Fatal(err)
	} else if !cached || transport.fetches != 0 {
		t.Fatalf("expected cached settings without a fetch, got cached=%t, fetches=%d", cached, transport.fetches)
	} else if settings.Prices.TipHeight != 100 {
		t.Fatalf("expected tip height 100, got %d", settings.Prices.TipHeight)
	}

	// the stale prices fail the height check
	if err := c.checkPricesHeight(settings.Prices, localHeight); !errors.Is(err, ErrPricesHeightDrift) {
		t.Fatalf("expected ErrPricesHeightDrift, got %v", err)
	}

	// refreshSettings bypasses the cache and fetches fresh settings that pass
	// the height check
	settings, err = c.refreshSettings(context.Background(), hk, transport)
	if err != nil {
		t.Fatal(err)
	} else if transport.fetches != 1 {
		t.Fatalf("expected 1 fetch, got %d", transport.fetches)
	} else if settings.Prices.TipHeight != 199 {
		t.Fatalf("expected tip height 199, got %d", settings.Prices.TipHeight)
	} else if err := c.checkPricesHeight(settings.Prices, localHeight); err != nil {
		t.Fatal(err)
	}

	// the fresh settings replaced the stale ones in the cache
	settings, cached, err = c.settings(context.Background(), hk, transport)
	if err != nil {
		t.Fatal(err)
	} else if !cached || transport.fetches != 1 {
		t.Fatalf("expected cached settings without a fetch, got cached=%t, fetches=%d", cached, transport.fetches)
	} else if settings.Prices.TipHeight != 199 {
		t.Fatalf("expected tip height 199, got %d", settings.Prices.TipHeight)
	}
}

func TestRefreshSettingsInvalidPrices(t *testing.T) {
	hostKey := types.GeneratePrivateKey()
	hk := hostKey.PublicKey()

	// prices signed by a different key than the host's
	transport := &fakeSettingsTransport{
		hostKey:  hostKey,
		settings: testSettings(types.GeneratePrivateKey(), 100, time.Now().Add(10*time.Minute)),
	}
	c := &Client{
		heightTolerance: 36,
		cachedSettings:  make(map[types.PublicKey]proto.HostSettings),
	}

	if _, err := c.refreshSettings(context.Background(), hk, transport); err == nil {
		t.Fatal("expected error for invalidly signed prices")
	}
	if len(c.cachedSettings) != 0 {
		t.Fatal("invalid settings should not be cached")
	}
}
