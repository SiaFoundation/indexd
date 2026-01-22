package hosts

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/testutils/mock"
	"go.uber.org/zap"
)

var cancelledCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// mockClient is a mock that implements the RHP4Client interface.
type mockClient struct {
	settings map[string]proto4.HostSettings
}

func (c *mockClient) ScanSiamux(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
	return c.scan(ctx, hk, addr)
}

func (c *mockClient) ScanQuic(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
	return c.scan(ctx, hk, addr)
}

func (c *mockClient) scan(ctx context.Context, _ types.PublicKey, addr string) (proto4.HostSettings, error) {
	select {
	case <-ctx.Done():
		return proto4.HostSettings{}, ctx.Err()
	default:
	}

	hs, ok := c.settings[addr]
	if ok {
		return hs, nil
	}
	return proto4.HostSettings{}, errors.New("") // mock host being unavailable on unknown address
}

func TestFetchSettings(t *testing.T) {
	c := &mockClient{
		settings: map[string]proto4.HostSettings{
			"foo.bar": {Release: t.Name()},
		},
	}

	// assert [context.Cancelled] is returned when context is cancelled
	_, err := fetchSettings(cancelledCtx, c, types.PublicKey{}, []chain.NetAddress{{Address: "foo.bar", Protocol: siamux.Protocol}}, zap.NewNop())
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// assert host has to be available on all addresses
	hs, err := fetchSettings(context.Background(), c, types.PublicKey{}, []chain.NetAddress{{Address: "foo.bar", Protocol: siamux.Protocol}, {Address: "bar.baz", Protocol: siamux.Protocol}}, zap.NewNop())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if hs != (proto4.HostSettings{}) {
		t.Fatal("unexpected", hs)
	}

	// assert host settings are returned
	hs, err = fetchSettings(context.Background(), c, types.PublicKey{}, []chain.NetAddress{{Address: "foo.bar", Protocol: siamux.Protocol}}, zap.NewNop())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if hs.Release != t.Name() {
		t.Fatal("unexpected", hs)
	}
}

// mockResolver is a mock that implements the Resolver interface.
type mockResolver struct {
	fail bool
	ips  map[string][]net.IPAddr
}

func (r *mockResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if r.fail {
		return nil, errors.New("lookup failed")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return r.ips[host], nil
}

// TestResolveHost unit tests the functionality of ResolveHost.
func TestResolveHost(t *testing.T) {
	r := &mockResolver{
		ips: map[string][]net.IPAddr{
			"h1.com": {{IP: net.IPv4(127, 0, 0, 1)}, {IP: net.IPv4(1, 2, 3, 4)}},
			"h2.com": {{IP: net.IPv4(8, 8, 8, 8)}},
		},
	}
	locator := &mock.Locator{}

	// assert [context.Cancelled] is returned when context is cancelled
	_, _, err := resolveHost(cancelledCtx, r, locator, []chain.NetAddress{{Address: "h1.com:1234", Protocol: siamux.Protocol}}, zap.NewNop())
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// assert incorrect addresses are filtered out
	addrs, _, err := resolveHost(context.Background(), r, locator, []chain.NetAddress{{Address: "h1.com", Protocol: siamux.Protocol}}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(addrs) != 0 {
		t.Fatal("unexpected", len(addrs))
	}

	// assert net addresses with private IPs are filtered out
	addrs, _, err = resolveHost(context.Background(), r, locator, []chain.NetAddress{{Address: "h1.com:1234", Protocol: siamux.Protocol}}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(addrs) != 0 {
		t.Fatal("unexpected", len(addrs))
	}

	// assert net addresses get resolved
	addrs, loc, err := resolveHost(context.Background(), r, locator, []chain.NetAddress{{Address: "h2.com:1234", Protocol: siamux.Protocol}}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(addrs) != 1 {
		t.Fatal("unexpected", len(addrs))
	} else if loc != mock.Location {
		t.Fatalf("expected location %v, got %v", mock.Location, loc)
	}
}

func TestCalculateNextScanTime(t *testing.T) {
	now := time.Now().Round(time.Minute)
	interval := 24 * time.Hour

	tests := []struct {
		name                string
		success             bool
		consecScanFailures  int
		interval            time.Duration
		offsetHours         int
		expBackoffHours     int
		expBackoffHoursMax  int
		expectedMinNextScan time.Time
		expectedMaxNextScan time.Time
	}{
		{
			name:                "random next scan",
			success:             true,
			offsetHours:         6,
			expectedMinNextScan: now.Add(18 * time.Hour),
			expectedMaxNextScan: now.Add(30 * time.Hour),
		},
		{
			name:                "exact next scan",
			success:             true,
			offsetHours:         0,
			expectedMinNextScan: now.Add(24 * time.Hour),
			expectedMaxNextScan: now.Add(24 * time.Hour),
		},
		{
			name:                "first exp backoff",
			success:             false,
			consecScanFailures:  1,
			expBackoffHours:     8,
			expBackoffHoursMax:  128,
			expectedMinNextScan: now.Add(8 * time.Hour),
			expectedMaxNextScan: now.Add(8 * time.Hour),
		},
		{
			name:                "max exp backoff",
			success:             false,
			consecScanFailures:  16,
			expBackoffHours:     8,
			expBackoffHoursMax:  128,
			expectedMinNextScan: now.Add(128 * time.Hour),
			expectedMaxNextScan: now.Add(128 * time.Hour),
		},
		{
			name:                "capped exp backoff",
			success:             false,
			consecScanFailures:  17,
			expBackoffHours:     8,
			expBackoffHoursMax:  128,
			expectedMinNextScan: now.Add(128 * time.Hour),
			expectedMaxNextScan: now.Add(128 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 100 {
				nextScan := calculateNextScanTime(
					now,
					tt.success,
					tt.consecScanFailures,
					interval,
					tt.offsetHours,
					tt.expBackoffHours,
					tt.expBackoffHoursMax,
				)
				if nextScan.Before(tt.expectedMinNextScan) || nextScan.After(tt.expectedMaxNextScan) {
					t.Errorf("Expected next scan time between %v and %v, got %v", tt.expectedMinNextScan, tt.expectedMaxNextScan, nextScan)
				}
			}
		})
	}
}
