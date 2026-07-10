package hosts_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/subscriber"
	"go.sia.tech/indexd/testutils"
	"go.sia.tech/indexd/testutils/mock"
	"go.uber.org/zap/zaptest"
)

type mockSyncer struct{ peers []*syncer.Peer }

func (s *mockSyncer) Peers() []*syncer.Peer { return s.peers }

func TestHostManager(t *testing.T) {
	db := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	defer db.Close()

	// create host manager
	mgr, err := hosts.NewManager(&mockSyncer{peers: []*syncer.Peer{{}}}, &mock.Locator{}, nil, db, alerts.NewManager(), hosts.WithAnnouncementMaxAge(time.Minute))
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
	if err := db.UpdateChainState(func(tx subscriber.UpdateTx) error {
		err = mgr.UpdateChainState(tx, []chain.ApplyUpdate{
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
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	hosts, err := db.Hosts(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	// assert h1 and h3 got added
	if len(hosts) != 2 {
		t.Fatal("unexpected number of hosts", len(hosts))
	} else if !((hosts[0].PublicKey == h1.PublicKey() && hosts[1].PublicKey == h3.PublicKey()) || (hosts[0].PublicKey == h3.PublicKey() && hosts[1].PublicKey == h1.PublicKey())) {
		t.Fatal("unexpected")
	}
}

type blockingScanner struct {
	delayMux  time.Duration
	delayQuic time.Duration
	settings  proto4.HostSettings
}

func TestUnblockUsableHostsAfterScanning(t *testing.T) {
	db := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	defer db.Close()

	goodSettings := proto4.HostSettings{
		Release:             "test",
		ProtocolVersion:     rhp.ProtocolVersion502,
		AcceptingContracts:  true,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		MaxContractDuration: 90 * 144,
		MaxCollateral:       types.Siacoins(1000),
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1),
			Collateral:    types.Siacoins(100).Div64(1e12).Div64(4320),
			StoragePrice:  types.NewCurrency64(1),
			ValidUntil:    time.Now().Add(24 * time.Hour),
		},
	}
	badSettings := goodSettings
	badSettings.AcceptingContracts = false

	hostKey := types.PublicKey{1}
	db.AddTestHost(t, hosts.Host{
		PublicKey: hostKey,
		Addresses: []chain.NetAddress{
			{Protocol: siamux.Protocol, Address: "1.1.1.1:9983"},
			{Protocol: quic.Protocol, Address: "1.1.1.1:9984"},
		},
		Settings: badSettings,
	})
	if err := db.BlockHosts([]types.PublicKey{hostKey}, []string{"AcceptingContracts"}); err != nil {
		t.Fatal(err)
	}

	mgr, err := hosts.NewManager(
		&mockSyncer{peers: []*syncer.Peer{{}}},
		&mock.Locator{},
		nil,
		db,
		alerts.NewManager(),
		hosts.WithScanner(&blockingScanner{settings: goodSettings}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if host, err := db.Host(hostKey); err != nil {
		t.Fatal(err)
	} else if !host.Blocked || host.Usability.Usable() {
		t.Fatalf("expected an unusable blocked host before scanning, got blocked=%v usable=%v", host.Blocked, host.Usability.Usable())
	}

	mgr.ScanHosts(t.Context(), []types.PublicKey{hostKey})

	if host, err := db.Host(hostKey); err != nil {
		t.Fatal(err)
	} else if host.Blocked || !host.Usability.Usable() {
		t.Fatalf("expected a usable unblocked host after scanning, got blocked=%v usable=%v reasons=%v", host.Blocked, host.Usability.Usable(), host.BlockedReasons)
	}
}

func (bs *blockingScanner) ScanSiamux(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
	time.Sleep(bs.delayMux)
	if err := ctx.Err(); err != nil {
		return proto4.HostSettings{}, err
	}
	return bs.settings, nil
}

func (bs *blockingScanner) ScanQuic(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
	time.Sleep(bs.delayQuic)
	if err := ctx.Err(); err != nil {
		return proto4.HostSettings{}, err
	}
	return bs.settings, nil
}

func TestScanTimeout(t *testing.T) {
	runTest := func(t *testing.T, addr chain.NetAddress, scanner hosts.Scanner, release *string) {
		db := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
		defer db.Close()

		hostKey := types.PublicKey{1}
		if err := db.UpdateChainState(func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hostKey, chain.V2HostAnnouncement{addr}, time.Now())
		}); err != nil {
			t.Fatal(err)
		}

		// create host manager
		mgr, err := hosts.NewManager(&mockSyncer{peers: []*syncer.Peer{{}}}, &mock.Locator{}, nil, db, alerts.NewManager(), hosts.WithScanner(scanner))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		host, err := mgr.ScanHost(ctx, hostKey)
		if release == nil {
			// we are expecting it to fail with "deadline exceeded"
			if err == nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected error %v, got %v", context.DeadlineExceeded, err)
			}
		} else if err != nil {
			t.Fatal(err)
		} else if host.Settings.Release != *release {
			t.Fatal("unexpected", host.Settings)
		}
	}

	t.Run("siamux_fail", func(t *testing.T) {
		scanner := &blockingScanner{
			delayMux: 500 * time.Millisecond,
		}
		runTest(t, chain.NetAddress{Address: "1.1.1.1:1111", Protocol: siamux.Protocol}, scanner, nil)
	})

	t.Run("quic_fail", func(t *testing.T) {
		scanner := &blockingScanner{
			delayQuic: 500 * time.Millisecond,
		}
		runTest(t, chain.NetAddress{Address: "1.1.1.1:1111", Protocol: quic.Protocol}, scanner, nil)
	})

	t.Run("siamux_succeed", func(t *testing.T) {
		scanner := &blockingScanner{
			settings: proto4.HostSettings{
				Release: "siamux",
			},
		}
		runTest(t, chain.NetAddress{Address: "1.1.1.1:1111", Protocol: siamux.Protocol}, scanner, &scanner.settings.Release)
	})

	t.Run("quic_succeed", func(t *testing.T) {
		scanner := &blockingScanner{
			settings: proto4.HostSettings{
				Release: "quic",
			},
		}
		runTest(t, chain.NetAddress{Address: "1.1.1.1:1111", Protocol: quic.Protocol}, scanner, &scanner.settings.Release)
	})
}

func TestIsBadQUICAddress(t *testing.T) {
	tests := []struct {
		name string
		addr chain.NetAddress
		want bool
	}{
		{"siamux", chain.NetAddress{Protocol: siamux.Protocol, Address: "1.1.1.1:22"}, false},
		{"good port", chain.NetAddress{Protocol: quic.Protocol, Address: "1.1.1.1:4848"}, false},
		{"bad port", chain.NetAddress{Protocol: quic.Protocol, Address: "1.1.1.1:22"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hosts.IsBadQUICAddress(tc.addr); got != tc.want {
				t.Fatalf("IsBadQUICAddress(%v) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}
