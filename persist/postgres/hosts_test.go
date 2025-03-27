package postgres

import (
	"context"
	"errors"
	"math"
	"net"
	"reflect"
	"slices"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAddHostAnnouncement(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// assert host is not found
	hk := types.PublicKey{1}
	_, err := db.Host(context.Background(), hk)
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
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

func TestBlocklist(t *testing.T) {
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

	assertBlocked := func(hk types.PublicKey, blocked bool) {
		t.Helper()
		h, err := db.Host(context.Background(), hk)
		if err != nil {
			t.Fatal(err)
		} else if h.Blocked != blocked {
			t.Fatal("unexpected", h.Blocked)
		}
		hks, err := db.BlockedHosts(context.Background(), 0, 10)
		if err != nil {
			t.Fatal(err)
		} else if slices.Contains(hks, hk) != blocked {
			t.Fatal("expected host to be in blocklist")
		}
	}

	block := func(hks ...types.PublicKey) {
		t.Helper()
		if err := db.BlockHosts(context.Background(), hks); err != nil {
			t.Fatal(err)
		}
	}
	unblock := func(hks ...types.PublicKey) {
		t.Helper()
		for _, hk := range hks {
			if err := db.UnblockHost(context.Background(), hk); err != nil {
				t.Fatal(err)
			}
		}
	}

	// assert both are returned
	assertBlocked(hk1, false)
	assertBlocked(hk2, false)

	// block h1 and assert noop on duplicate insert
	block(hk1, hk1)

	// assert only h2 is returned
	assertBlocked(hk1, true)
	assertBlocked(hk2, false)

	// block h2 and assert no hosts are returned
	block(hk2)
	assertBlocked(hk1, true)
	assertBlocked(hk2, true)

	// unblock h2 assert noop on duplicate or remove of unknown key
	unblock(hk2, hk2, types.GeneratePrivateKey().PublicKey())

	// assert h2 is returned
	assertBlocked(hk1, true)
	assertBlocked(hk2, false)

	// unblock h1 and assert we're back to normal
	unblock(hk1)
	assertBlocked(hk1, false)
	assertBlocked(hk2, false)
}

func TestHost(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	hk := types.GeneratePrivateKey().PublicKey()
	hs := newTestHostSettings(hk)

	// assert [hosts.ErrNotFound] is returned
	_, err := db.Host(context.Background(), hk)
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
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
	err = db.UpdateHost(context.Background(), hk, networks, hs, true, time.Now())
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

func TestHostChecks(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))
	hk := types.PublicKey{1}

	// define helper to assert the settings passed the check with given name
	assertCheckOK := func(check string) {
		t.Helper()

		h, err := db.Host(context.Background(), hk)
		if err != nil {
			t.Fatal(err)
		}

		v := reflect.ValueOf(h.Usability)
		f := v.FieldByName(check)
		if !f.IsValid() {
			t.Fatalf("field '%s' not found", check)
		} else if f.Kind() != reflect.Bool {
			t.Fatalf("field '%s' is not a boolean", check)
		} else if !f.Bool() {
			t.Fatalf("expected field '%s' to be true", check)
		}
	}

	// define test variables
	oneSC := types.Siacoins(1)
	oneH := types.NewCurrency64(1)
	oneTB := uint64(1e12)
	settingPeriod := uint64(6084)
	settingMinCollataral := types.Siacoins(1).Div64(oneTB)
	settingMaxStoragePrice := types.Siacoins(2).Div64(oneTB)
	settingMaxIngressPrice := types.Siacoins(3).Div64(oneTB)
	settingMaxEgressPrice := types.Siacoins(4).Div64(oneTB)

	// update global settings
	if err := db.UpdateUsabilitySettings(context.Background(), hosts.UsabilitySettings{
		MaxEgressPrice:     settingMaxEgressPrice,
		MaxIngressPrice:    settingMaxIngressPrice,
		MaxStoragePrice:    settingMaxStoragePrice,
		MinCollateral:      settingMinCollataral,
		MinProtocolVersion: [3]uint8{1, 0, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(context.Background(), contracts.MaintenanceSettings{
		Period:          settingPeriod,
		RenewWindow:     settingPeriod / 2,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// add a host
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// assert unscanned host is unusable
	if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.Usability.Usable() {
		t.Fatal("expected host to be not usable")
	}

	// update host with settings that fail all checks
	err := db.UpdateHost(context.Background(), hk, nil, proto4.HostSettings{
		Release:             "test",
		ProtocolVersion:     [3]uint8{0, 0, 0},
		AcceptingContracts:  false,
		WalletAddress:       types.StandardAddress(hk),
		MaxCollateral:       oneH.Mul64(oneTB).Mul64(settingPeriod).Sub(oneH),
		MaxContractDuration: settingPeriod - 1,
		RemainingStorage:    frand.Uint64n(1e3),
		TotalStorage:        frand.Uint64n(1e3) + 1e3,
		Prices: proto4.HostPrices{
			ContractPrice:   oneSC.Add(oneH),
			StoragePrice:    settingMaxStoragePrice.Add(oneH),
			IngressPrice:    settingMaxIngressPrice.Add(oneH),
			EgressPrice:     settingMaxEgressPrice.Add(oneH),
			Collateral:      settingMinCollataral.Sub(oneH),
			FreeSectorPrice: oneSC.Div64(oneTB).Add(oneH),
			ValidUntil:      time.Now().Add(59 * time.Minute).Round(time.Microsecond),
			TipHeight:       frand.Uint64n(1e3),
		},
	}, true, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// fail scan to ensure we fail on uptime
	err = db.UpdateHost(context.Background(), hk, nil, proto4.HostSettings{}, false, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// assert host fails all checks
	h, err := db.Host(context.Background(), hk)
	if err != nil {
		t.Fatal(err)
	} else {
		v := reflect.ValueOf(h.Usability)
		for i := range v.NumField() {
			if v.Field(i).Bool() {
				t.Fatal(v.Type().Field(i).Name, "expected to be false")
			}
		}
	}
	hs := h.Settings

	// adjust recent uptime so we pass the check
	if _, err := db.pool.Exec(context.Background(), `UPDATE hosts SET recent_uptime = .91`); err != nil {
		t.Fatal(err)
	}
	assertCheckOK("Uptime")

	// adjust max duration so we pass the check
	hs.MaxContractDuration = settingPeriod
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("MaxContractDuration")

	// adjust max collateral so we pass the check
	hs.MaxCollateral = hs.Prices.Collateral.Mul64(oneTB).Mul64(settingPeriod)
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("MaxCollateral")

	// adjust protocol to pass the check
	hs.ProtocolVersion = [3]uint8{1, 0, 0}
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("ProtocolVersion")

	// adjust price validity so we pass the check
	hs.Prices.ValidUntil = time.Now().Add(time.Second * 3601)
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("PriceValidity")

	// adjust accepting contracts so we pass the check
	hs.AcceptingContracts = true
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("AcceptingContracts")

	// adjust contract price so we pass the check
	hs.Prices.ContractPrice = oneSC.Sub(oneH)
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("ContractPrice")

	// adjust collateral so we pass the check
	hs.Prices.Collateral = hs.Prices.StoragePrice.Mul64(2)
	hs.MaxCollateral = hs.Prices.Collateral.Mul64(oneTB).Mul64(settingPeriod)
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("Collateral")

	// adjust storage price so we pass the check
	hs.Prices.StoragePrice = settingMaxStoragePrice
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("StoragePrice")

	// adjust egress price so we pass the check
	hs.Prices.EgressPrice = settingMaxEgressPrice
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("EgressPrice")

	// adjust ingress price so we pass the check
	hs.Prices.IngressPrice = settingMaxIngressPrice
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("IngressPrice")

	// adjust free sector price so we pass the check
	hs.Prices.FreeSectorPrice = oneSC.Div64(oneTB)
	_ = db.UpdateHost(context.Background(), hk, nil, hs, true, time.Now())
	assertCheckOK("FreeSectorPrice")

	// assert host is usable
	if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if !h.Usability.Usable() {
		t.Fatal("expected host to be usable")
	}
}

func TestHostsRecentUptime(t *testing.T) {
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))
	hk := types.PublicKey{1}

	// simulateScans simulates n scans that were either successful or failed
	// depending on the up parameter, the scans are 24 hours apart
	simulateScans := func(up bool, n int) {
		t.Helper()
		if up {
			for range n {
				_, err1 := db.pool.Exec(context.Background(), `UPDATE hosts SET last_failed_scan = '0001-01-01 00:00:00+00'::timestamptz, last_successful_scan = NOW() - INTERVAL '24 hours'`)
				if err := errors.Join(err1, db.UpdateHost(context.Background(), hk, nil, newTestHostSettings(hk), true, time.Time{})); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			for range n {
				_, err1 := db.pool.Exec(context.Background(), `UPDATE hosts SET last_successful_scan = '0001-01-01 00:00:00+00'::timestamptz, last_failed_scan = NOW() - INTERVAL '24 hours'`)
				if err := errors.Join(err1, db.UpdateHost(context.Background(), hk, nil, proto4.HostSettings{}, false, time.Time{})); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	uptime := func() float64 {
		t.Helper()
		h, err := db.Host(context.Background(), hk)
		if err != nil {
			t.Fatal(err)
		}
		return h.RecentUptime
	}

	// add a host
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// assert default uptime
	if uptime() != .894 {
		t.Fatal("unexpected", uptime())
	}

	// assert one week of uptime passes the uptime check
	simulateScans(true, 7)
	if uptime() > .9 {
		t.Fatal("unexpected", uptime())
	}
	simulateScans(true, 1)
	if uptime() < .9 {
		t.Fatal("unexpected", uptime())
	}

	// assert fresh hosts don't fail check immediately
	simulateScans(true, 30)
	simulateScans(false, 2)
	if uptime() < .9 {
		t.Fatal("unexpected", uptime())
	}

	// assert good uptime hosts need 11 days to fail the check
	simulateScans(true, 180)
	simulateScans(false, 11)
	if uptime() > .9 {
		t.Fatal("unexpected", uptime())
	}

	// assert great uptime hosts aren't invincible
	simulateScans(true, 360)
	simulateScans(false, 13)
	if uptime() > .9 {
		t.Fatal("unexpected", uptime())
	}

	// assert uptime is halved after the the half life
	_, err := db.pool.Exec(context.Background(), `UPDATE hosts SET recent_uptime = .999999999`)
	if err != nil {
		t.Fatal(err)
	}
	simulateScans(false, uptimeHalfLife/60/60/24)
	if math.Round(uptime()*10)/10 != .5 {
		t.Fatal("unexpected", uptime())
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
	err = db.UpdateHost(context.Background(), h1, nil, proto4.HostSettings{}, false, time.Now())
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert h1 gets pruned
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	} else if _, err := db.Host(context.Background(), h1); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// simulate failed scan for h2
	err = db.UpdateHost(context.Background(), h2, nil, proto4.HostSettings{}, false, time.Now())
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
		db.UpdateHost(context.Background(), h1, nil, proto4.HostSettings{}, true, time.Now()),
		db.UpdateHost(context.Background(), h1, nil, proto4.HostSettings{}, false, time.Now()),
		db.UpdateHost(context.Background(), h2, nil, proto4.HostSettings{}, true, time.Now()),
		db.UpdateHost(context.Background(), h2, nil, proto4.HostSettings{}, false, time.Now()),
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
	err = db.AddFormedContract(context.Background(), types.FileContractID{1}, h2, 0, 0, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)
	if err != nil {
		t.Fatal(err)
	}

	// assert only h1 got pruned if we set the cutoff in the future
	n, err = db.PruneHosts(context.Background(), time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	} else if _, err = db.Host(context.Background(), h1); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// delete all contracts
	if _, err := db.pool.Exec(context.Background(), "DELETE FROM contracts"); err != nil {
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

	// assert [hosts.ErrNotFound] is returned
	hk := types.GeneratePrivateKey().PublicKey()
	err := db.UpdateHost(context.Background(), hk, nil, proto4.HostSettings{}, false, time.Now())
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// add a host
	if err := db.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// assert host settings are not inserted if the scan failed
	hs := newTestHostSettings(hk)
	err = db.UpdateHost(context.Background(), hk, nil, hs, false, time.Now())
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.Settings != (proto4.HostSettings{}) {
		t.Fatal("expected no settings", h.Settings, proto4.HostSettings{})
	} else if !h.LastSuccessfulScan.IsZero() {
		t.Fatal("expected no last successful scan")
	} else if h.LastFailedScan.IsZero() {
		t.Fatal("expected last failed scan to be set")
	}

	// assert consecutive failed scans are incremented
	err = db.UpdateHost(context.Background(), hk, nil, hs, false, time.Now())
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
	} else if h.ConsecutiveFailedScans != 0 {
		t.Fatal("unexpected", h.ConsecutiveFailedScans)
	} else if h.LastSuccessfulScan.IsZero() {
		t.Fatal("expected last successful scan to be set")
	} else if !h.NextScan.Equal(nextScan) {
		t.Fatal("unexpected next scan", h.NextScan)
	} else if h.RecentUptime == 0.895 {
		t.Fatal("expected recent uptime to be updated")
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

func newTestHostSettings(pk types.PublicKey) proto4.HostSettings {
	return proto4.HostSettings{
		Release:             "test",
		ProtocolVersion:     [3]uint8{1, 0, 0},
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
