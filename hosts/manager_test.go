package hosts_test

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
)

func TestHostManager(t *testing.T) {
	// create db
	db := testutils.NewDB(t, zap.NewNop())
	defer db.Close()

	// create host manager
	mgr, err := hosts.NewManager(hosts.WithAnnouncementMaxAge(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// create host keys
	h1 := types.GeneratePrivateKey()
	h2 := types.GeneratePrivateKey()
	h3 := types.GeneratePrivateKey()
	h4 := types.GeneratePrivateKey()

	// create a helper to find addresses for a host by public key
	addresses := func(hk types.PublicKey, hosts []hosts.Host) []chain.NetAddress {
		t.Helper()
		for _, h := range hosts {
			if h.PublicKey == hk {
				return h.Addresses
			}
		}
		t.Fatal("host not found", hk)
		return nil
	}

	// create a helper to count number of addresses of given protocol
	count := func(protocol chain.Protocol, addresses []chain.NetAddress) (cnt int) {
		t.Helper()
		for _, na := range addresses {
			if na.Protocol == protocol {
				cnt++
			}
		}
		return
	}

	// process chain update
	cs := consensus.State{}
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return mgr.UpdateChainState(tx, []chain.ApplyUpdate{
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
	}); err != nil {
		t.Fatal(err)
	}

	// assert two hosts got indexed
	hosts, err := db.Hosts(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}

	// assert h1 got indexed
	h1Addr := addresses(h1.PublicKey(), hosts)
	if len(h1Addr) != 1 {
		t.Fatal("unexpected", len(h1Addr))
	} else if h1Addr[0].Address != "1.2.3.4:5678" || h1Addr[0].Protocol != siamux.Protocol {
		t.Fatalf("unexpected address %+v", h1Addr[0])
	}

	// assert h3 got indexed
	h3Addr := addresses(h3.PublicKey(), hosts)
	if len(h3Addr) != 4 {
		t.Fatal("unexpected", len(h3Addr))
	} else if cnt := count(quic.Protocol, h3Addr); cnt != 2 {
		t.Fatal("unexpected QUIC addresses", cnt)
	} else if count(siamux.Protocol, h3Addr) != 2 {
		t.Fatal("unexpected siamux addresses", cnt)
	}

	// process chain update that updates h1 addresses
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return mgr.UpdateChainState(tx, []chain.ApplyUpdate{
			{
				Block: types.Block{
					Timestamp: time.Now(),
					V2: &types.V2BlockData{
						Transactions: []types.V2Transaction{
							{
								Attestations: []types.Attestation{
									chain.V2HostAnnouncement{
										{Protocol: quic.Protocol, Address: "[::]:4848"},
									}.ToAttestation(cs, h1),
								},
							},
						},
					},
				},
			},
		})
	}); err != nil {
		t.Fatal(err)
	}

	// assert it was updated
	indexed, err := db.Hosts(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	h1Addr = addresses(h1.PublicKey(), indexed)
	if len(h1Addr) != 1 {
		t.Fatal("unexpected", len(h1Addr))
	} else if h1Addr[0].Address != "[::]:4848" || h1Addr[0].Protocol != quic.Protocol {
		t.Fatalf("unexpected address %+v", h1Addr[0])
	}
}
