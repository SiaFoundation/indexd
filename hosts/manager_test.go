package hosts

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/indexd/geoip"
	"go.uber.org/zap"
)

var cancelledCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// mockStore is a mock that implements the Store interface.
type mockStore struct {
	hosts map[types.PublicKey]Host
}

func (s *mockStore) AddHostAnnouncement(hk types.PublicKey, ha chain.V2HostAnnouncement, _ time.Time) error {
	s.hosts[hk] = Host{Addresses: ha}
	return nil
}

func (s *mockStore) Host(ctx context.Context, hk types.PublicKey) (Host, error) {
	h, ok := s.hosts[hk]
	if !ok {
		return h, errors.New("not found")
	}
	return h, nil
}

func (s *mockStore) Hosts(ctx context.Context, offset, limit int, opts ...HostQueryOpt) ([]Host, error) {
	return nil, nil
}

func (s *mockStore) HostsForScanning(ctx context.Context) ([]types.PublicKey, error) { return nil, nil }

func (s *mockStore) BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reason string) error {
	return nil
}

func (s *mockStore) BlockedHosts(ctx context.Context, offset, limit int) ([]types.PublicKey, error) {
	return nil, nil
}

func (s *mockStore) UnblockHost(ctx context.Context, hk types.PublicKey) error {
	return nil
}

func (s *mockStore) PruneHosts(ctx context.Context, lastSuccessfulScanCutoff time.Time, minConsecutiveFailedScans int) (int64, error) {
	return 0, nil
}

func (s *mockStore) UpdateHost(ctx context.Context, hk types.PublicKey, networks []net.IPNet, hs proto4.HostSettings, loc geoip.Location, scanSucceeded bool, nextScan time.Time) error {
	return nil
}

func (s *mockStore) UsabilitySettings(context.Context) (UsabilitySettings, error) {
	return UsabilitySettings{}, nil
}

func (s *mockStore) UpdateUsabilitySettings(_ context.Context, us UsabilitySettings) error {
	return nil
}

func (s *mockStore) HostsForPruning(ctx context.Context) ([]types.PublicKey, error) {
	return nil, nil
}

func (s *mockStore) HostsForPinning(ctx context.Context) ([]types.PublicKey, error) {
	return nil, nil
}

func (s *mockStore) HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error) {
	return nil, nil
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

// mockClient is a mock that implements the RHP4Client interface.
type mockClient struct {
	settings map[string]proto4.HostSettings
}

func (c *mockClient) Settings(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
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

type mockLocator struct{}

func (m *mockLocator) Close() error {
	return nil
}

func (m *mockLocator) Locate(addr net.IP) (geoip.Location, error) {
	return geoip.Location{
		CountryCode: "US",
		Latitude:    10,
		Longitude:   -20,
	}, nil
}

func TestHostManager(t *testing.T) {
	db := &mockStore{hosts: make(map[types.PublicKey]Host)}

	// create host manager
	mgr, err := NewManager(&mockSyncer{peers: []*syncer.Peer{{}}}, &mockLocator{}, db, WithAnnouncementMaxAge(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// create host keys
	h1 := types.GeneratePrivateKey()
	h2 := types.GeneratePrivateKey()
	h3 := types.GeneratePrivateKey()
	h4 := types.GeneratePrivateKey()

	// process chain update
	cs := consensus.State{}
	err = mgr.UpdateChainState(db, []chain.ApplyUpdate{
		{
			Block: types.Block{
				Timestamp: time.Now(),
				V2: &types.V2BlockData{
					Transactions: []types.V2Transaction{
						{
							// invalid protocol
							Attestations: []types.Attestation{
								chain.V2HostAnnouncement{
									{Protocol: "invalid", Address: "1.2.3.4:5678"},
									{Protocol: siamux.Protocol, Address: "1.2.3.4:5678"},
								}.ToAttestation(cs, h1),
							},
						},
						{
							// empty address
							Attestations: []types.Attestation{
								chain.V2HostAnnouncement{
									{Protocol: siamux.Protocol, Address: ""},
								}.ToAttestation(cs, h2),
							},
						},
						{
							// too many addresses per protocol
							Attestations: []types.Attestation{
								chain.V2HostAnnouncement{
									{Protocol: siamux.Protocol, Address: "1.2.3.4:5678"},
									{Protocol: siamux.Protocol, Address: "2.2.3.4:5678"},
									{Protocol: siamux.Protocol, Address: "3.2.3.4:5678"},
									{Protocol: quic.Protocol, Address: "1.2.3.4:5678"},
									{Protocol: quic.Protocol, Address: "2.2.3.4:5678"},
									{Protocol: quic.Protocol, Address: "3.2.3.4:5678"},
								}.ToAttestation(cs, h3),
							},
						},
					},
				},
			},
		},
		{
			// old announcement
			Block: types.Block{
				Timestamp: time.Now().Add(-2 * time.Minute),
				V2: &types.V2BlockData{
					Transactions: []types.V2Transaction{
						{
							Attestations: []types.Attestation{
								chain.V2HostAnnouncement{
									{Protocol: siamux.Protocol, Address: "1.2.3.4:5678"},
								}.ToAttestation(cs, h4),
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert h1 and h3 got added
	if len(db.hosts) != 2 {
		t.Fatal("unexpected number of hosts", len(db.hosts))
	} else if _, ok := db.hosts[h1.PublicKey()]; !ok {
		t.Fatal("unexpected")
	} else if _, ok := db.hosts[h3.PublicKey()]; !ok {
		t.Fatal("unexpected")
	}
}

func TestFetchSettings(t *testing.T) {
	c := &mockClient{
		settings: map[string]proto4.HostSettings{
			"foo.bar": {Release: t.Name()},
		},
	}

	// assert [context.Cancelled] is returned when context is cancelled
	_, err := fetchSettings(cancelledCtx, c, types.PublicKey{}, []chain.NetAddress{testMuxAddr("foo.bar")}, zap.NewNop())
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// assert host has to be available on all addresses
	hs, err := fetchSettings(context.Background(), c, types.PublicKey{}, []chain.NetAddress{testMuxAddr("foo.bar"), testMuxAddr("bar.baz")}, zap.NewNop())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if hs != (proto4.HostSettings{}) {
		t.Fatal("unexpected", hs)
	}

	// assert host settings are returned
	hs, err = fetchSettings(context.Background(), c, types.PublicKey{}, []chain.NetAddress{testMuxAddr("foo.bar")}, zap.NewNop())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if hs.Release != t.Name() {
		t.Fatal("unexpected", hs)
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

// TestResolveHost unit tests the functionality of ResolveHost.
func TestResolveHost(t *testing.T) {
	r := &mockResolver{
		ips: map[string][]net.IPAddr{
			"h1.com": {{IP: net.IPv4(127, 0, 0, 1)}, {IP: net.IPv4(1, 2, 3, 4)}},
			"h2.com": {{IP: net.IPv4(8, 8, 8, 8)}},
		},
	}

	// assert [context.Cancelled] is returned when context is cancelled
	_, _, err := resolveHost(cancelledCtx, r, []chain.NetAddress{testMuxAddr("h1.com:1234")}, zap.NewNop())
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// assert incorrect addresses are filtered out
	addrs, _, err := resolveHost(context.Background(), r, []chain.NetAddress{testMuxAddr("h1.com")}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(addrs) != 0 {
		t.Fatal("unexpected", len(addrs))
	}

	// assert net addresses with private IPs are filtered out
	addrs, _, err = resolveHost(context.Background(), r, []chain.NetAddress{testMuxAddr("h1.com:1234")}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(addrs) != 0 {
		t.Fatal("unexpected", len(addrs))
	}

	// assert net addresses get resolved and networks are returned
	addrs, networks, err := resolveHost(context.Background(), r, []chain.NetAddress{testMuxAddr("h2.com:1234")}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(addrs) != 1 {
		t.Fatal("unexpected", len(addrs))
	} else if len(networks) != 1 {
		t.Fatal("unexpected", len(networks))
	}
}

func testMuxAddr(addr string) chain.NetAddress {
	return chain.NetAddress{Protocol: siamux.Protocol, Address: addr}
}
