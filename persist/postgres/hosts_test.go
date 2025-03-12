package postgres

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
)

func TestAddHostAnnouncement(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// assert host is not found
	hk := types.PublicKey{1}
	_, err := db.Host(context.Background(), hk)
	if !errors.Is(err, ErrHostNotFound) {
		t.Fatal("expected ErrHostNotFound, got", err)
	}

	// announce the host
	now := time.Now().Round(time.Microsecond)
	ha1 := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	ha2 := chain.NetAddress{Protocol: siamux.Protocol, Address: "1.2.3.4:5678"}
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha1, ha2}, now)
	}); err != nil {
		t.Fatal(err)
	}

	// assert host got inserted, as well as its addresses
	h, err := db.Host(context.Background(), hk)
	if err != nil {
		t.Fatal("unexpected", err)
	} else if h.LastAnnouncement != now {
		t.Fatal("unexpected", h.LastAnnouncement)
	} else if len(h.Addresses) != 2 {
		t.Fatal("unexpected", len(h.Addresses))
	} else if h.Addresses[0].Address != ha1.Address || h.Addresses[0].Protocol != ha1.Protocol {
		t.Fatal("unexpected", h.Addresses[0])
	} else if h.Addresses[1].Address != ha2.Address || h.Addresses[1].Protocol != ha2.Protocol {
		t.Fatal("unexpected", h.Addresses[1])
	}

	// reannounce host
	now = now.Add(time.Minute)
	ha3 := chain.NetAddress{Protocol: siamux.Protocol, Address: "8.7.6.5:4321"}
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha3}, now)
	}); err != nil {
		t.Fatal(err)
	}

	// assert host is updated and addresses are overwritten
	h, err = db.Host(context.Background(), hk)
	if err != nil {
		t.Fatal("unexpected", err)
	} else if h.LastAnnouncement != now {
		t.Fatal("unexpected", h.LastAnnouncement)
	} else if len(h.Addresses) != 1 {
		t.Fatal("unexpected", len(h.Addresses))
	} else if h.Addresses[0].Address != ha3.Address || h.Addresses[0].Protocol != ha3.Protocol {
		t.Fatal("unexpected", h.Addresses[0])
	}
}

func TestHost(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	hk := types.GeneratePrivateKey().PublicKey()
	hs := testHostSettings(hk)

	// assert [ErrHostNotFound] is returned
	_, err := db.Host(context.Background(), hk)
	if !errors.Is(err, ErrHostNotFound) {
		t.Fatal("expected ErrHostNotFound, got", err)
	}

	// add a host
	ha1 := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha1}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// update the host
	networks := []net.IPNet{{IP: net.IPv4(1, 2, 3, 4), Mask: net.CIDRMask(32, 32)}}
	err = db.UpdateHost(context.Background(), hk, networks, hs, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	// assert host is found and address, networks and settings are populated
	if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(h.Settings, hs) {
		t.Fatal("expected settings to match", h.Settings)
	} else if len(h.Addresses) != 1 {
		t.Fatal("unexpected", len(h.Addresses))
	} else if len(h.Networks) != 1 {
		t.Fatal("unexpected networks", h.Networks)
	}
}

func TestHostsForScanning(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	hk1 := types.PublicKey{1}
	hk2 := types.PublicKey{2}
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return errors.Join(
			tx.AddHostAnnouncement(hk1, chain.V2HostAnnouncement{}, time.Now()),
			tx.AddHostAnnouncement(hk2, chain.V2HostAnnouncement{}, time.Now()),
		)
	}); err != nil {
		t.Fatal(err)
	}

	// assert both hosts are returned
	hosts, err := db.HostsForScanning(context.Background())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts))
	}

	// simulate scanning h1 successfully
	nextScan := time.Now().Round(time.Microsecond).Add(time.Minute)
	err = db.UpdateHost(context.Background(), hk1, nil, proto4.HostSettings{}, true, nextScan)
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert only h2 is returned
	hosts, err = db.HostsForScanning(context.Background())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0] != hk2 {
		t.Fatal("unexpected", hosts[0])
	}

	// simulate scanning h2 successfully
	err = db.UpdateHost(context.Background(), hk2, nil, proto4.HostSettings{}, true, nextScan)
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert no hosts are returned
	hosts, err = db.HostsForScanning(context.Background())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 0 {
		t.Fatal("unexpected", len(hosts))
	}
}

func TestPruneHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// create helper to add a host
	addHost := func() types.PublicKey {
		t.Helper()
		hk := types.GeneratePrivateKey().PublicKey()
		if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
		}); err != nil {
			t.Fatal(err)
		}
		return hk
	}

	// add two hosts
	addHost()
	addHost()

	// assert both get pruned when no params are given
	n, err := db.PruneHosts(context.Background(), time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("unexpected", n)
	}

	// re-add the hosts
	h1 := addHost()
	h2 := addHost()

	// assert none get pruned when we require at least one failed scan
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("unexpected", n)
	}

	// simulate failed scan for h1
	err = db.UpdateHost(context.Background(), h1, nil, proto4.HostSettings{}, false, time.Time{})
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert h1 gets pruned
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	} else if _, err = db.Host(context.Background(), h1); !errors.Is(err, ErrHostNotFound) {
		t.Fatal("expected ErrHostNotFound, got", err)
	}

	// simulate failed scan for h2
	err = db.UpdateHost(context.Background(), h2, nil, proto4.HostSettings{}, false, time.Time{})
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert h2 gets pruned
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	}

	// re-add both hosts, simulate both a successful and failed scan
	h1 = addHost()
	h2 = addHost()
	err = errors.Join(
		db.UpdateHost(context.Background(), h1, nil, proto4.HostSettings{}, true, time.Time{}),
		db.UpdateHost(context.Background(), h1, nil, proto4.HostSettings{}, false, time.Time{}),
		db.UpdateHost(context.Background(), h2, nil, proto4.HostSettings{}, true, time.Time{}),
		db.UpdateHost(context.Background(), h2, nil, proto4.HostSettings{}, false, time.Time{}),
	)
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert both do not get pruned if we set the cutoff in the past
	n, err = db.PruneHosts(context.Background(), time.Now().Add(-time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("unexpected", n)
	}

	// add contract to h2
	err = db.AddFormedContract(context.Background(), types.FileContractID{1}, h2, 0, 0, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)
	if err != nil {
		t.Fatal(err)
	}

	// assert only h1 got pruned if we set the cutoff in the future
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	} else if _, err = db.Host(context.Background(), h1); !errors.Is(err, ErrHostNotFound) {
		t.Fatal("expected ErrHostNotFound, got", err)
	}

	// delete all contracts
	if err := db.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, "DELETE FROM contracts")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// assert h2 gets pruned now as well
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	}
}

func TestUpdateHost(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// assert [ErrHostNotFound] is returned
	hk := types.GeneratePrivateKey().PublicKey()
	err := db.UpdateHost(context.Background(), hk, nil, proto4.HostSettings{}, false, time.Time{})
	if !errors.Is(err, ErrHostNotFound) {
		t.Fatal("expected ErrHostNotFound, got", err)
	}

	// add a host
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// assert host settings are not inserted if the scan failed
	hs := testHostSettings(hk)
	err = db.UpdateHost(context.Background(), hk, nil, hs, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.Settings != (proto4.HostSettings{Prices: proto4.HostPrices{ValidUntil: time.Time{}.Local()}}) {
		t.Fatal("expected no settings", h.Settings, proto4.HostSettings{})
	} else if !h.LastSuccessfulScan.IsZero() {
		t.Fatal("expected no last successful scan")
	}

	// assert consecutive failed scans are incremented
	err = db.UpdateHost(context.Background(), hk, nil, hs, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.ConsecutiveFailedScans != 2 {
		t.Fatal("unexpected", h.ConsecutiveFailedScans)
	}

	now := time.Now().Round(time.Minute)
	nextScan := now.Add(time.Hour)
	networks := []net.IPNet{{IP: net.IPv4(1, 2, 3, 4), Mask: net.CIDRMask(32, 32)}}

	// assert host is properly updated on successful scan
	err = db.UpdateHost(context.Background(), hk, networks, hs, true, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(h.Settings, hs) {
		t.Fatal("expected settings to match")
	} else if h.TotalScans != 3 {
		t.Fatal("unexpected", h.TotalScans)
	} else if h.ConsecutiveFailedScans != 0 {
		t.Fatal("unexpected", h.ConsecutiveFailedScans)
	} else if h.LastSuccessfulScan.IsZero() {
		t.Fatal("expected last successful scan to be set")
	} else if !h.NextScan.Equal(nextScan) {
		t.Fatal("unexpected next scan", h.NextScan)
	} else if h.FailedScans != 2 {
		t.Fatal("unexpected failed scans", h.FailedScans)
	} else if len(h.Networks) != 1 {
		t.Fatal("unexpected networks", h.Networks)
	} else if h.Networks[0].String() != networks[0].String() {
		t.Fatal("unexpected network", h.Networks)
	}

	// assert networks are overwritten
	networks = []net.IPNet{{IP: net.IPv4(4, 3, 2, 1), Mask: net.CIDRMask(32, 32)}}
	err = db.UpdateHost(context.Background(), hk, networks, hs, true, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if len(h.Networks) != 1 {
		t.Fatal("unexpected networks", h.Networks)
	} else if h.Networks[0].String() != networks[0].String() {
		t.Fatal("unexpected network", h.Networks)
	}
}

func testHostSettings(pk types.PublicKey) proto4.HostSettings {
	return proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       types.StandardAddress(pk),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
			ValidUntil:    time.Now().Add(time.Hour).Round(time.Microsecond),
			TipHeight:     1,
		},
	}
}
