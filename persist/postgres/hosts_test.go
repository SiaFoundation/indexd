package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

var testNetworks = []net.IPNet{{IP: net.IPv4(1, 2, 3, 4), Mask: net.CIDRMask(32, 32)}}

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

	// add the host
	db.addTestHost(t, hk)

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
	} else if h.LostSectors != 0 {
		t.Fatal("expected lost sectors to be 0, got", h.LostSectors)
	}

	// pin a sector and mark it as lost
	r1 := types.Hash256{1}
	if err := db.AddAccount(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if _, err := db.PinSlab(context.Background(), proto4.Account(hk), time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors:       []slabs.SectorPinParams{{Root: r1, HostKey: hk}},
	}); err != nil {
		t.Fatal(err)
	} else if err := db.MarkSectorsLost(context.Background(), hk, []types.Hash256{r1}); err != nil {
		t.Fatal("unexpected", err)
	}

	// assert lost sectors is properly set on the host
	if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.LostSectors != 1 {
		t.Fatal("expected one lost sector, got", h.LostSectors)
	}

	// assert lost sectors is also set when querying hosts
	if hosts, err := db.Hosts(context.Background(), 0, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("expected one host, got", len(hosts))
	} else if hosts[0].PublicKey != hk {
		t.Fatal("expected host public key to match", hosts[0].PublicKey)
	} else if hosts[0].LostSectors != 1 {
		t.Fatal("expected one lost sector, got", hosts[0].LostSectors)
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
	sectorsPerTB := uint64(250000)
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
	db.addTestHost(t, hk)

	// assert unscanned host is unusable
	if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.Usability.Usable() {
		t.Fatal("expected host to be not usable")
	}

	// update host with settings that fail all checks
	err := db.UpdateHost(context.Background(), hk, testNetworks, proto4.HostSettings{
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
			FreeSectorPrice: oneSC.Div64(sectorsPerTB).Add(oneH),
			ValidUntil:      time.Now().Add(14 * time.Minute).Round(time.Microsecond),
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
	if _, err := db.pool.Exec(context.Background(), `UPDATE hosts SET recent_uptime = .9`); err != nil {
		t.Fatal(err)
	}
	assertCheckOK("Uptime")

	// adjust max duration so we pass the check
	hs.MaxContractDuration = settingPeriod
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("MaxContractDuration")

	// adjust max collateral so we pass the check
	hs.MaxCollateral = hs.Prices.Collateral.Mul64(oneTB).Mul64(settingPeriod)
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("MaxCollateral")

	// adjust protocol to pass the check
	hs.ProtocolVersion = [3]uint8{1, 0, 0}
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("ProtocolVersion")

	// adjust price validity so we pass the check
	hs.Prices.ValidUntil = time.Now().Add(time.Second * 3601)
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("PriceValidity")

	// adjust accepting contracts so we pass the check
	hs.AcceptingContracts = true
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("AcceptingContracts")

	// adjust contract price so we pass the check
	hs.Prices.ContractPrice = oneSC.Sub(oneH)
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("ContractPrice")

	// adjust collateral so we pass the check
	hs.Prices.Collateral = hs.Prices.StoragePrice.Mul64(2)
	hs.MaxCollateral = hs.Prices.Collateral.Mul64(oneTB).Mul64(settingPeriod)
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("Collateral")

	// adjust storage price so we pass the check
	hs.Prices.StoragePrice = settingMaxStoragePrice
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("StoragePrice")

	// adjust egress price so we pass the check
	hs.Prices.EgressPrice = settingMaxEgressPrice
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("EgressPrice")

	// adjust ingress price so we pass the check
	hs.Prices.IngressPrice = settingMaxIngressPrice
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("IngressPrice")

	// adjust free sector price so we pass the check
	hs.Prices.FreeSectorPrice = oneSC.Div64(sectorsPerTB)
	_ = db.UpdateHost(context.Background(), hk, testNetworks, hs, true, time.Now())
	assertCheckOK("FreeSectorPrice")

	// assert host is usable
	if h, err := db.Host(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if !h.Usability.Usable() {
		t.Fatal("expected host to be usable")
	}
}

func TestHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// update global settings to make hosts pass all checks
	if err := db.UpdateUsabilitySettings(context.Background(), hosts.UsabilitySettings{
		MaxEgressPrice:     types.MaxCurrency,
		MaxIngressPrice:    types.MaxCurrency,
		MaxStoragePrice:    types.MaxCurrency,
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: [3]uint8{1, 0, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(context.Background(), contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// ip mask for testing
	_, network, err := net.ParseCIDR("127.0.0.1/24")
	if err != nil {
		t.Fatal(err)
	}

	goodUsability := hosts.Usability{
		Uptime:              true,
		MaxContractDuration: true,
		MaxCollateral:       true,
		ProtocolVersion:     true,
		PriceValidity:       true,
		AcceptingContracts:  true,

		ContractPrice:   true,
		Collateral:      true,
		StoragePrice:    true,
		IngressPrice:    true,
		EgressPrice:     true,
		FreeSectorPrice: true,
	}

	// make sure all settings have the same validity to simplify the assertions
	validUntil := time.Now().Round(time.Microsecond).Add(24 * time.Hour)
	newSettings := func(hk types.PublicKey) proto4.HostSettings {
		settings := newTestHostSettings(hk)
		settings.Prices.ValidUntil = validUntil
		return settings
	}

	// helper to add hosts
	addHost := func(i byte, usable, blocked bool, contract bool) types.PublicKey {
		t.Helper()
		hk := types.PublicKey{i}
		db.addTestHost(t, hk)

		// "scan" host - first scan fails
		settings := newSettings(hk)
		if !usable {
			settings.AcceptingContracts = false
		}
		err = db.UpdateHost(context.Background(), hk, []net.IPNet{*network}, settings, false, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		err = db.UpdateHost(context.Background(), hk, []net.IPNet{*network}, settings, true, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}

		// block host
		if blocked {
			err := db.BlockHosts(context.Background(), []types.PublicKey{hk}, "test")
			if err != nil {
				t.Fatal(err)
			}
		}

		// form contract
		if contract {
			db.addTestContract(t, hk)
		}
		return hk
	}

	// add hosts
	hk1 := addHost(1, true, false, true)  // good, pending contract
	hk2 := addHost(2, false, false, true) // bad, pending contract
	hk3 := addHost(3, true, true, false)  // good, blocked, no contract
	hk4 := addHost(4, false, true, false) //  bad, blocked, no contract

	assertHosts := func(hks []types.PublicKey, offset, limit int, queryOpts ...hosts.HostQueryOpt) {
		t.Helper()
		hosts, err := db.Hosts(context.Background(), offset, limit, queryOpts...)
		if err != nil {
			t.Fatal(err)
		} else if len(hosts) != len(hks) {
			t.Fatalf("expected %v hosts, got %v", len(hks), len(hosts))
		}
		for i, host := range hosts {
			if host.PublicKey != hks[i] {
				t.Fatalf("expected hk %v, got %v", hks[i], host.PublicKey)
			} else if host.LastAnnouncement.IsZero() {
				t.Fatal("host should be announced")
			} else if host.LastFailedScan.IsZero() {
				t.Fatal("should have a failed scan")
			} else if host.LastSuccessfulScan.IsZero() {
				t.Fatal("should have been scanned successfully")
			} else if host.NextScan.IsZero() {
				t.Fatal("next scan should be scheduled")
			} else if host.ConsecutiveFailedScans != 0 {
				t.Fatal("shouldn't have consecutive failed scan")
			} else if host.RecentUptime == 0 {
				t.Fatal("host should have uptime")
			} else if host.Addresses == nil {
				t.Fatal("host should have an address")
			} else if host.Networks == nil {
				t.Fatal("host should have a network")
			}

			blocked := false
			settings := newSettings(host.PublicKey)
			usability := goodUsability
			switch host.PublicKey {
			case hk1:
			// hk1 is good and unblocked
			case hk2:
				settings.AcceptingContracts = false
				usability.AcceptingContracts = false
			case hk3:
				blocked = true
			case hk4:
				blocked = true
				settings.AcceptingContracts = false
				usability.AcceptingContracts = false
			default:
				t.Fatal("unknown host")
			}
			if host.Settings != settings {
				t.Fatal("invalid settings")
			} else if host.Blocked != blocked {
				t.Fatalf("expected blocked %v, got %v", blocked, host.Blocked)
			} else if host.Blocked && host.BlockedReason != "test" {
				t.Fatalf("expected blocked reason 'test', got %v", host.BlockedReason)
			} else if host.Usability != usability {
				t.Fatalf("expected usability %+v, got %+v", usability, host.Usability)
			}
		}
	}

	// all hosts
	assertHosts([]types.PublicKey{hk1, hk2, hk3, hk4}, 0, 4)

	// assert limit works
	assertHosts([]types.PublicKey{hk1, hk2, hk3}, 0, 3)

	// assert offset works
	assertHosts([]types.PublicKey{hk2, hk3, hk4}, 1, 3)

	// only usable/unusable hosts
	assertHosts([]types.PublicKey{hk1, hk3}, 0, 4, hosts.WithUsable(true))
	assertHosts([]types.PublicKey{hk2, hk4}, 0, 4, hosts.WithUsable(false))

	// only blocked/unblocked hosts
	assertHosts([]types.PublicKey{hk3, hk4}, 0, 4, hosts.WithBlocked(true))
	assertHosts([]types.PublicKey{hk1, hk2}, 0, 4, hosts.WithBlocked(false))

	// only hosts with active contracts
	assertHosts([]types.PublicKey{hk1, hk2}, 0, 4, hosts.WithActiveContracts(true))
	assertHosts([]types.PublicKey{hk3, hk4}, 0, 4, hosts.WithActiveContracts(false))

	// mix filters
	assertHosts([]types.PublicKey{hk3}, 0, 4, hosts.WithUsable(true), hosts.WithBlocked(true))
	assertHosts([]types.PublicKey{hk2}, 0, 4, hosts.WithUsable(false), hosts.WithBlocked(false), hosts.WithActiveContracts(true))

	// mix filters and offset/limit
	assertHosts([]types.PublicKey{hk1}, 0, 1, hosts.WithUsable(true))
	assertHosts([]types.PublicKey{hk3}, 1, 1, hosts.WithUsable(true))
	assertHosts([]types.PublicKey{hk2}, 0, 1, hosts.WithUsable(false))
	assertHosts([]types.PublicKey{hk4}, 1, 1, hosts.WithUsable(false))
	assertHosts([]types.PublicKey{hk3}, 0, 1, hosts.WithBlocked(true))
	assertHosts([]types.PublicKey{hk4}, 1, 1, hosts.WithBlocked(true))
	assertHosts([]types.PublicKey{hk1}, 0, 1, hosts.WithBlocked(false))
	assertHosts([]types.PublicKey{hk2}, 1, 1, hosts.WithBlocked(false))
	assertHosts([]types.PublicKey{hk2}, 1, 1, hosts.WithActiveContracts(true))
}

func TestUsableHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// update global settings to make hosts pass all checks
	if err := db.UpdateUsabilitySettings(context.Background(), hosts.UsabilitySettings{
		MaxEgressPrice:     types.MaxCurrency,
		MaxIngressPrice:    types.MaxCurrency,
		MaxStoragePrice:    types.MaxCurrency,
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: [3]uint8{1, 0, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(context.Background(), contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// ip mask for testing
	_, network, err := net.ParseCIDR("127.0.0.1/24")
	if err != nil {
		t.Fatal(err)
	}

	// helper to add hosts
	addHost := func(i byte, usable, blocked bool, contract bool) types.PublicKey {
		t.Helper()

		hk := types.PublicKey{i}
		db.addTestHost(t, hk)

		// handle usable
		settings := newTestHostSettings(hk)
		if !usable {
			settings.AcceptingContracts = false
		}
		if err := db.UpdateHost(context.Background(), hk, []net.IPNet{*network}, settings, true, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}

		// handle blocked
		if blocked {
			err := db.BlockHosts(context.Background(), []types.PublicKey{hk}, "test")
			if err != nil {
				t.Fatal(err)
			}
		}

		// handle contract
		if contract {
			db.addTestContract(t, hk)
		}
		return hk
	}

	// add hosts in all possible configurations
	_ = addHost(1, false, false, false)
	_ = addHost(2, false, false, true)
	_ = addHost(3, false, true, false)
	_ = addHost(4, false, true, true)
	_ = addHost(5, true, false, false)
	uh1 := addHost(6, true, false, true)
	_ = addHost(7, true, true, false)
	_ = addHost(8, true, true, true)
	uh2 := addHost(9, true, false, true) // second usable host

	// assert only h6 and h9 are returned
	hosts, err := db.UsableHosts(context.Background(), 0, 10)
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 || hosts[1].PublicKey != uh2 {
		t.Fatal("unexpected hosts", hosts[0], hosts[1])
	} else if hosts[0].Addresses == nil || hosts[1].Addresses == nil {
		t.Fatal("expected hosts to have addresses")
	}

	// assert offset and limit are applied
	if hosts, err := db.UsableHosts(context.Background(), 0, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 {
		t.Fatal("unexpected host", hosts[0].PublicKey)
	} else if hosts, err := db.UsableHosts(context.Background(), 1, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh2 {
		t.Fatal("unexpected host", hosts[0].PublicKey)
	} else if hosts, err := db.UsableHosts(context.Background(), 2, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 0 {
		t.Fatal("expected no hosts, got", len(hosts))
	}
}

func TestHostsForScanning(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// assert both hosts are returned
	hosts, err := db.HostsForScanning(context.Background())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts))
	}

	// simulate scanning h1 successfully
	nextScan := time.Now().Round(time.Microsecond).Add(time.Minute)
	err = db.UpdateHost(context.Background(), hk1, testNetworks, proto4.HostSettings{}, true, nextScan)
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
	err = db.UpdateHost(context.Background(), hk2, testNetworks, proto4.HostSettings{}, true, nextScan)
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

func TestHostsWithLostSectors(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add account
	account := proto4.Account{1}
	if err := db.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// add a contract for each host
	db.addTestContract(t, hk1)
	db.addTestContract(t, hk2)

	// pin a slab that adds 2 sectors to each host
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	root4 := frand.Entropy256()
	_, err := db.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     10,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    root1,
				HostKey: hk1,
			},
			{
				Root:    root2,
				HostKey: hk1,
			},
			{
				Root:    root3,
				HostKey: hk2,
			},
			{
				Root:    root4,
				HostKey: hk2,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertHosts := func(hks []types.PublicKey) {
		t.Helper()
		hosts, err := db.HostsWithLostSectors(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if len(hks) != len(hosts) {
			t.Fatalf("expected %d hosts, got %d", len(hks), len(hosts))
		}
		for i, host := range hosts {
			if host != hks[i] {
				t.Fatalf("expected PublicKey %v, got %v", hks[i], host)
			}
		}
	}

	// assert no hosts are returned because no sectors are lost yet
	assertHosts(nil)

	if err := db.MarkSectorsLost(context.Background(), hk1, []types.Hash256{root1}); err != nil {
		t.Fatal(err)
	}

	// hk1 should be returned after one of its sectors is marked as lost
	assertHosts([]types.PublicKey{hk1})

	if err := db.MarkSectorsLost(context.Background(), hk1, []types.Hash256{root2}); err != nil {
		t.Fatal(err)
	}

	// still only hk1 should be returned after another one of its sectors is
	// marked as lost
	assertHosts([]types.PublicKey{hk1})

	if err := db.MarkSectorsLost(context.Background(), hk2, []types.Hash256{root3}); err != nil {
		t.Fatal(err)
	}

	// hk1 and hk2 should be returned after both have lost sectors
	assertHosts([]types.PublicKey{hk1, hk2})
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
				if err := errors.Join(err1, db.UpdateHost(context.Background(), hk, testNetworks, newTestHostSettings(hk), true, time.Time{})); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			for range n {
				_, err1 := db.pool.Exec(context.Background(), `UPDATE hosts SET last_successful_scan = '0001-01-01 00:00:00+00'::timestamptz, last_failed_scan = NOW() - INTERVAL '24 hours'`)
				if err := errors.Join(err1, db.UpdateHost(context.Background(), hk, testNetworks, proto4.HostSettings{}, false, time.Time{})); err != nil {
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
	hk = db.addTestHost(t)

	// manually override the default uptime to .894, this very specific value
	// gives us the uptime properties that are laid out in the spec. The recent
	// uptime defaults to 0.9 to ensure hosts pass uptime checks by default.
	_, err := db.pool.Exec(context.Background(), `UPDATE hosts SET recent_uptime = .894`)
	if err != nil {
		t.Fatal(err)
	}

	// assert uptime
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
	_, err = db.pool.Exec(context.Background(), `UPDATE hosts SET recent_uptime = .999999999`)
	if err != nil {
		t.Fatal(err)
	}
	simulateScans(false, uptimeHalfLife/60/60/24)
	if math.Round(uptime()*10)/10 != .5 {
		t.Fatal("unexpected", uptime())
	}
}

func TestPruneHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	db.addTestHost(t)
	db.addTestHost(t)

	// assert both get pruned when no params are given
	n, err := db.PruneHosts(context.Background(), time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("unexpected", n)
	}

	// re-add the hosts
	h1 := db.addTestHost(t)
	h2 := db.addTestHost(t)

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
	h1 = db.addTestHost(t)
	h2 = db.addTestHost(t)
	err = errors.Join(
		db.UpdateHost(context.Background(), h1, testNetworks, proto4.HostSettings{}, true, time.Now()),
		db.UpdateHost(context.Background(), h1, testNetworks, proto4.HostSettings{}, false, time.Now()),
		db.UpdateHost(context.Background(), h2, testNetworks, proto4.HostSettings{}, true, time.Now()),
		db.UpdateHost(context.Background(), h2, testNetworks, proto4.HostSettings{}, false, time.Now()),
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
	db.addTestContract(t, h2)

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
	if _, err := db.pool.Exec(context.Background(), "DELETE FROM contract_sectors_map; DELETE FROM contracts;"); err != nil {
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
	db.addTestHost(t, hk)

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

	// assert successful scan needs networks
	err = db.UpdateHost(context.Background(), hk, nil, hs, true, nextScan)
	if !errors.Is(err, hosts.ErrNoNetworks) {
		t.Fatalf("expected hosts.ErrNoNetworks, got %v", err)
	}

	// assert updating with a failed scan doesn't affect the host's networks
	err = db.UpdateHost(context.Background(), hk, []net.IPNet{{IP: net.IPv4(9, 9, 9, 9)}}, hs, false, nextScan)
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

// BenchmarkHosts is a set of benchmarks that verify the performance of the host
// methods in the store that use common table expressions.
//
// M1 Max | Host       | 2 ms/op
// M1 Max | Hosts_10   | 7 ms/op
// M1 Max | Hosts_100  | 9.5 ms/op
// M1 Max | Hosts_1000 | 11.5 ms/op
// M1 Max | UpdateHost | 1.5 ms/op
func BenchmarkHosts(b *testing.B) {
	// define parameters
	const (
		numHosts     = 10_000
		numBlocklist = 1000
	)

	// prepare test variables
	networks := []net.IPNet{
		{IP: net.IPv4(1, 2, 3, 4), Mask: net.CIDRMask(32, 32)},
		{IP: net.IPv4(2, 3, 4, 5), Mask: net.CIDRMask(32, 32)},
	}

	// prepare database
	hosts := make([]types.PublicKey, numHosts)
	store := initPostgres(b, zap.NewNop())
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		for i := range numHosts {
			var hostID int64
			hk := types.GeneratePrivateKey().PublicKey()
			err := tx.QueryRow(context.Background(), `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}
			hosts[i] = hk

			na := chain.NetAddress{Protocol: siamux.Protocol, Address: "foo"}
			_, err = tx.Exec(ctx, `INSERT INTO host_addresses (host_id, net_address, protocol) VALUES ($1, $2, $3)`, hostID, na.Address, sqlNetworkProtocol(na.Protocol))
			if err != nil {
				return fmt.Errorf("failed to insert host address: %w", err)
			}

			_, err = tx.Exec(ctx, `INSERT INTO host_resolved_cidrs (host_id, cidr) VALUES ($1, $2), ($1, $3)`, hostID, networks[0].String(), networks[1].String())
			if err != nil {
				return err
			}
		}

		// we LEFT JOIN the blocklist so we populate it with random entries
		for range numBlocklist {
			_, err := tx.Exec(ctx, "INSERT INTO hosts_blocklist (public_key, reason) VALUES ($1, 'none') ON CONFLICT (public_key) DO NOTHING", sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.Run("Host", func(b *testing.B) {
		var i int
		for b.Loop() {
			_, err := store.Host(context.Background(), hosts[i%numHosts])
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	for _, limit := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("Hosts_%d", limit), func(b *testing.B) {
			for b.Loop() {
				offset := frand.Intn(numHosts - limit)
				hosts, err := store.Hosts(context.Background(), offset, limit)
				if err != nil {
					b.Fatal(err)
				} else if len(hosts) != limit {
					b.Fatalf("expected %d hosts, got %d", limit, len(hosts)) // sanity check
				}
			}
		})
	}

	b.Run("UpdateHost", func(b *testing.B) {
		ts := time.Now()
		hs := proto4.HostSettings{
			ProtocolVersion:     [3]uint8{1, 2, 3},
			Release:             b.Name(),
			WalletAddress:       types.Address{1},
			AcceptingContracts:  true,
			MaxCollateral:       types.NewCurrency64(frand.Uint64n(1e6)),
			MaxContractDuration: frand.Uint64n(1e6),
			RemainingStorage:    frand.Uint64n(1e6),
			TotalStorage:        frand.Uint64n(1e6),
			Prices: proto4.HostPrices{
				ContractPrice:   types.NewCurrency64(frand.Uint64n(1e6)),
				Collateral:      types.NewCurrency64(frand.Uint64n(1e6)),
				StoragePrice:    types.NewCurrency64(frand.Uint64n(1e6)),
				IngressPrice:    types.NewCurrency64(frand.Uint64n(1e6)),
				EgressPrice:     types.NewCurrency64(frand.Uint64n(1e6)),
				FreeSectorPrice: types.NewCurrency64(frand.Uint64n(1e6)),
				TipHeight:       frand.Uint64n(1e6),
				ValidUntil:      ts,
			},
		}

		var i int
		for b.Loop() {
			hk := hosts[i%numHosts]
			succeeded := frand.Intn(2) == 0
			err := store.UpdateHost(context.Background(), hk, networks, hs, succeeded, ts)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
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
			ValidUntil:    time.Now().Add(24 * time.Hour).Round(time.Microsecond),
			TipHeight:     1,
			Signature:     types.Signature{1, 2, 3},
		},
	}
}

func TestHostsForPinning(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// add account
	acc := proto4.Account{1}
	if err := db.AddAccount(context.Background(), types.PublicKey(acc)); err != nil {
		t.Fatal(err)
	}

	// pin a slab with sector on both hosts
	r1 := frand.Entropy256()
	if _, err := db.PinSlab(context.Background(), acc, time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.SectorPinParams{
			{
				Root:    r1,
				HostKey: hk1,
			},
			{
				Root:    frand.Entropy256(),
				HostKey: hk2,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// assert no hosts are returned, they don't have active contracts
	hks, err := db.HostsForPinning(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}

	// add contract for both hosts
	fcid1 := db.addTestContract(t, hk1)
	_ = db.addTestContract(t, hk2)

	// assert both hosts are returned now
	hks, err = db.HostsForPinning(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hks))
	}

	// pin sectors for h1
	if err := db.PinSectors(context.Background(), fcid1, []types.Hash256{r1}); err != nil {
		t.Fatal(err)
	}

	// assert only h2 is returned
	hks, err = db.HostsForPinning(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 {
		t.Fatalf("expected one host, got %d", len(hks))
	} else if hks[0] != hk2 {
		t.Fatalf("expected host %v, got %v", hk2, hks[0])
	}

	// block host 2
	if err := db.BlockHosts(context.Background(), []types.PublicKey{hk2}, "test"); err != nil {
		t.Fatal(err)
	}

	// assert no hosts are returned
	hks, err = db.HostsForPinning(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}
}

func TestHostsForPruning(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// add account
	acc := proto4.Account{1}
	if err := db.AddAccount(context.Background(), types.PublicKey(acc)); err != nil {
		t.Fatal(err)
	}

	// add contract for both hosts
	fcid1 := db.addTestContract(t, hk1)
	fcid2 := db.addTestContract(t, hk2)

	// assert there's no hosts for pruning yet
	if hks, err := db.HostsForPruning(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}

	// update next prune so h2 should be returned for pruning
	if _, err := db.pool.Exec(context.Background(), `UPDATE contracts SET next_prune = NOW() - INTERVAL '1 second' WHERE contract_id = $1`, sqlHash256(fcid2)); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 || hks[0] != hk2 {
		t.Fatalf("expected h2 for pruning, got %v", hks)
	}

	// block host 2 and assert it is not returned anymore
	if err := db.BlockHosts(context.Background(), []types.PublicKey{hk2}, "test"); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}

	// update next prune so h1 should be returned for pruning
	if _, err := db.pool.Exec(context.Background(), `UPDATE contracts SET next_prune = NOW() - INTERVAL '1 second' WHERE contract_id = $1`, sqlHash256(fcid1)); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 || hks[0] != hk1 {
		t.Fatalf("expected h1 for pruning, got %v", hks)
	}

	// update good status so h1 is not returned anymore
	if _, err := db.pool.Exec(context.Background(), `UPDATE contracts SET good = FALSE WHERE contract_id = $1`, sqlHash256(fcid1)); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}
}

// BenchmarkHostsForPruning benchmarks HostsForPruning.
func BenchmarkHostsForPruning(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// add account
	account := proto4.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors

		nHosts            = 1000
		nContractsPerHost = 100
		nBlocklistHosts   = 1000
	)

	randomTime := func() time.Time {
		maxOneHour := time.Duration(frand.Uint64n(60*60)) * time.Second
		return time.Now().Add(-30 * time.Minute).Add(maxOneHour)
	}

	// prepare database
	hostToContractID := make(map[types.PublicKey]types.FileContractID, nHosts)
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		for range nHosts {
			// add host
			hk := types.PublicKey(frand.Entropy256())
			var hostID int64
			err := tx.QueryRow(ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}

			// add contracts
			var fcid types.FileContractID
			for range nContractsPerHost {
				frand.Read(fcid[:])
				if _, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, raw_revision, proof_height, expiration_height, contract_price, initial_allowance, miner_fee, total_collateral, remaining_allowance, state, good, next_prune) VALUES ($1, $2, $3, 0, 0, $4, $5, $6, $7, $8, $9, $10, $11);`,
					hostID,
					sqlHash256(fcid[:]),
					sqlFileContract(newTestRevision(hk)),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlContractState(uint8(frand.Uint64n(5))), // random contract state (40% active)
					frand.Uint64n(2) == 0,                     // random good state (50% good)
					randomTime(),                              // random next prune time

				); err != nil {
					return err
				}
			}
			hostToContractID[hk] = fcid
		}

		// we LEFT JOIN the blocklist so we populate it with random entries
		for range nBlocklistHosts {
			_, err := tx.Exec(ctx, "INSERT INTO hosts_blocklist (public_key, reason) VALUES ($1, 'none') ON CONFLICT (public_key) DO NOTHING", sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		batch, err := store.HostsForPruning(context.Background())
		if err != nil {
			b.Fatal(err)
		} else if len(batch) != nHosts {
			b.Fatal("unexpected number of hosts", len(batch))
		}
	}
}

func TestHostsForIntegrityChecks(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// add account
	acc := proto4.Account{1}
	if err := db.AddAccount(context.Background(), types.PublicKey(acc)); err != nil {
		t.Fatal(err)
	}

	// helper to pin sector with a given checkTime
	pinSector := func(hk types.PublicKey, root types.Hash256, nextCheck time.Time) {
		t.Helper()
		_, err := db.PinSlab(context.Background(), acc, nextCheck, slabs.SlabPinParams{
			EncryptionKey: [32]byte{},
			MinShards:     1,
			Sectors: []slabs.SectorPinParams{
				{
					Root:    root,
					HostKey: hk,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// pin 2 sectors for each host, one with a check time in the past and one in
	// the future
	root1 := types.Hash256{1}
	root2 := types.Hash256{2}
	root3 := types.Hash256{3}
	root4 := types.Hash256{4}

	now := time.Now().Round(time.Microsecond).Add(-time.Microsecond)
	oneHAgo := now.Add(-time.Hour)
	oneHFromNow := now.Add(time.Hour)

	pinSector(hk1, root1, oneHAgo)
	pinSector(hk2, root2, oneHAgo)
	pinSector(hk1, root3, oneHFromNow)
	pinSector(hk2, root4, oneHFromNow)

	hosts, err := db.HostsForIntegrityChecks(context.Background(), oneHFromNow, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hosts))
	} else if hosts[0] != hk1 || hosts[1] != hk2 {
		t.Fatalf("expected hosts %v, got %v", []types.PublicKey{hk1, hk2}, hosts)
	}

	// apply limit
	hosts, err = db.HostsForIntegrityChecks(context.Background(), oneHFromNow, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	} else if hosts[0] != hk1 {
		t.Fatalf("expected host %v, got %v", hk1, hosts[0])
	}

	// using a maxLastCheck time in the past should cause no hosts to be returned
	hosts, err = db.HostsForIntegrityChecks(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hosts))
	}

	// unpinning the sector on host 2 which is up for a check should cause host
	// 2 to not be returned anymore
	if err := db.MarkSectorsLost(context.Background(), hk2, []types.Hash256{root2}); err != nil {
		t.Fatal(err)
	}

	hosts, err = db.HostsForIntegrityChecks(context.Background(), oneHFromNow, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	} else if hosts[0] != hk1 {
		t.Fatalf("expected host %v, got %v", hk1, hosts[0])
	}

	// block host 1 so that it's also not returned anymore
	if err := db.BlockHosts(context.Background(), []types.PublicKey{hk1}, ""); err != nil {
		t.Fatal(err)
	}
	hosts, err = db.HostsForIntegrityChecks(context.Background(), oneHFromNow, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hosts))
	}
}

// BenchmarkHostsForPinning benchmarks HostsForPinning.
func BenchmarkHostsForPinning(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// add account
	account := proto4.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors

		nHosts            = 1000
		nContractsPerHost = 100
		nBlocklistHosts   = 1000
		nSectorsPerHost   = dbBaseSize / proto4.SectorSize / nHosts
	)

	// prepare database
	hostToContractID := make(map[types.PublicKey]types.FileContractID, nHosts)
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		for range nHosts {
			// add host
			hk := types.PublicKey(frand.Entropy256())
			var hostID int64
			err := tx.QueryRow(ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}

			// add contracts
			var fcid types.FileContractID
			for range nContractsPerHost {
				frand.Read(fcid[:])
				size := frand.Uint64n(1e9)
				if _, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, raw_revision, proof_height, expiration_height, contract_price, initial_allowance, miner_fee, total_collateral, remaining_allowance, state, good, size, capacity) VALUES ($1, $2, $3, 0, 0, $4, $5, $6, $7, $8, $9, $10, $11, $12);`,
					hostID,
					sqlHash256(fcid[:]),
					sqlFileContract(newTestRevision(hk)),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.NewCurrency64(frand.Uint64n(5))), // random remaining allowance
					sqlContractState(uint8(frand.Uint64n(5))),          // random contract state (40% active)
					frand.Uint64n(2) == 0,                              // random good state (50% good)
					size,                                               // random size
					size+frand.Uint64n(1e3),                            // random capacity
				); err != nil {
					return err
				}
			}
			hostToContractID[hk] = fcid
		}

		// we LEFT JOIN the blocklist so we populate it with random entries
		for range nBlocklistHosts {
			_, err := tx.Exec(ctx, "INSERT INTO hosts_blocklist (public_key, reason) VALUES ($1, 'none') ON CONFLICT (public_key) DO NOTHING", sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// add sectors
	for hk, fcid := range hostToContractID {
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.SectorPinParams
			var roots []types.Hash256
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.SectorPinParams{
					Root:    root,
					HostKey: hk,
				})
				roots = append(roots, root)
			}
			if _, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}

			// pin 10% of the sectors
			frand.Shuffle(len(roots), func(i, j int) { roots[i], roots[j] = roots[j], roots[i] })
			if err := store.PinSectors(context.Background(), fcid, roots[:len(roots)/10]); err != nil {
				b.Fatal(err, fcid)
			}
		}
	}

	for b.Loop() {
		batch, err := store.HostsForPinning(context.Background())
		if err != nil {
			b.Fatal(err)
		} else if len(batch) != nHosts {
			b.Fatal("unexpected number of hosts", len(batch))
		}
	}
}

// BenchmarkHostsForIntegrityCheck benchmarks HostsForIntegrityCheck.
func BenchmarkHostsForIntegrityCheck(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto4.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	const (
		nHosts          = 10000
		dbBaseSize      = 1 << 40 // 1TiB of sectors
		nSectorsPerHost = dbBaseSize / proto4.SectorSize / nHosts
	)

	// add hosts
	for range nHosts {
		hk := store.addTestHost(b)

		// add sectors
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.SectorPinParams
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.SectorPinParams{
					Root:    root,
					HostKey: hk,
				})
			}
			if _, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	// 10% of the sectors have a next check time in the past
	_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET next_integrity_check = NOW() - INTERVAL '1 hour' WHERE id % 10 = 0`)
	if err != nil {
		b.Fatal(err)
	}

	// run benchmark for various batch sizes
	futureTime := time.Now().Add(time.Hour)
	for _, batchSize := range []int{50, 100, 200} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			for b.Loop() {
				batch, err := store.HostsForIntegrityChecks(context.Background(), futureTime, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(batch) == 0 {
					b.Fatal("expected hosts, got none")
				}
			}
		})
	}
}

// BenchmarkHostsWithLostSectors benchmarks HostsWithLostSectors.
func BenchmarkHostsWithLostSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto4.Account{1}

	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	const (
		nHosts          = 10000
		dbBaseSize      = 1 << 40 // 1TiB of sectors
		nSectorsPerHost = dbBaseSize / proto4.SectorSize / nHosts
	)

	// add hosts
	for range nHosts {
		hk := store.addTestHost(b)

		// add sectors
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.SectorPinParams
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.SectorPinParams{
					Root:    root,
					HostKey: hk,
				})
			}
			if _, err := store.PinSlab(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	// 10% of hosts have lost sectors
	_, err := store.pool.Exec(context.Background(), `UPDATE hosts SET lost_sectors = floor(random() * 100) + 1 WHERE id % 10 = 0`)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		batch, err := store.HostsWithLostSectors(context.Background())
		if err != nil {
			b.Fatal(err)
		} else if len(batch) == 0 {
			b.Fatal("expected hosts, got none")
		}
	}
}

func (s *Store) addTestHost(t testing.TB, hks ...types.PublicKey) types.PublicKey {
	t.Helper()

	var hk types.PublicKey
	switch len(hks) {
	case 0:
		hk = types.GeneratePrivateKey().PublicKey() // generate a random host key
	case 1:
		hk = hks[0]
	default:
		panic("developer error")
	}

	ha := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	if err := s.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	return hk
}
