package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/geoip"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAddHostAnnouncement(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// assert host is not found
	hk := types.PublicKey{1}
	_, err := db.Host(hk)
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// announce the host
	now := time.Now().Round(time.Microsecond)
	ha1 := chain.NetAddress{Protocol: quic.Protocol, Address: "[::]:4848"}
	ha2 := chain.NetAddress{Protocol: siamux.Protocol, Address: "1.2.3.4:5678"}
	if err := db.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha1, ha2}, now)
	}); err != nil {
		t.Fatal(err)
	}

	// assert host got inserted, as well as its addresses
	h, err := db.Host(hk)
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
	if err := db.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha3}, now)
	}); err != nil {
		t.Fatal(err)
	}

	// assert host is updated and addresses are overwritten
	h, err = db.Host(hk)
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

func TestBlockHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	store := initPostgres(t, log.Named("postgres"))

	hk1 := store.addTestHost(t, types.PublicKey{1})
	hk2 := store.addTestHost(t, types.PublicKey{2})
	hk3 := types.PublicKey{3} // not in DB

	// assert block reasons
	reasons := []string{"a", "b"}
	if err := store.BlockHosts([]types.PublicKey{hk1, hk2}, reasons); err != nil {
		t.Fatal(err)
	}
	for _, hk := range []types.PublicKey{hk1, hk2} {
		host, err := store.Host(hk)
		if err != nil {
			t.Fatal(err)
		} else if !host.Blocked {
			t.Fatal("expected host to be blocked")
		} else if !reflect.DeepEqual(host.BlockedReasons, reasons) {
			t.Fatal("expected host to have correct blocked reasons", host.BlockedReasons)
		}
	}

	// assert blocking unknown host works and doesn't error
	if err := store.BlockHosts([]types.PublicKey{hk3}, reasons); err != nil {
		t.Fatal(err)
	}
	if hks, err := store.BlockedHosts(0, 10); err != nil {
		t.Fatal(err)
	} else if len(hks) != 3 {
		t.Fatal("expected 3 blocked hosts, got", len(hks))
	}

	// assert reasons are deduplicated when blocking the same host
	reasons = []string{"b", "c"}
	if err := store.BlockHosts([]types.PublicKey{hk1}, reasons); err != nil {
		t.Fatal(err)
	}
	host, err := store.Host(hk1)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(host.BlockedReasons, []string{"a", "b", "c"}) {
		t.Fatal("expected host to have correct blocked reasons", host.BlockedReasons)
	}
}

func TestHost(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	hk := types.GeneratePrivateKey().PublicKey()
	hs := newTestHostSettings(hk)

	// assert [hosts.ErrNotFound] is returned
	_, err := db.Host(hk)
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// add the host
	db.addTestHost(t, hk)
	db.addTestContract(t, hk)

	// update the host
	err = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// assert host is found and addresses and settings are populated
	if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(h.Settings, hs) {
		t.Fatal("expected settings to match", h.Settings)
	} else if len(h.Addresses) != 1 {
		t.Fatal("unexpected", len(h.Addresses))
	} else if h.LostSectors != 0 {
		t.Fatal("expected lost sectors to be 0, got", h.LostSectors)
	}

	// pin a sector and mark it as lost
	r1 := types.Hash256{1}
	db.addTestAccount(t, hk)
	if _, err := db.PinSlabs(proto4.Account(hk), time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors:       []slabs.PinnedSector{{Root: r1, HostKey: hk}},
	}); err != nil {
		t.Fatal(err)
	} else if err := db.MarkSectorsLost(hk, []types.Hash256{r1}); err != nil {
		t.Fatal("unexpected", err)
	}

	// assert lost sectors is properly set on the host
	if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if h.LostSectors != 1 {
		t.Fatal("expected one lost sector, got", h.LostSectors)
	}

	// assert lost sectors is also set when querying hosts
	if hosts, err := db.Hosts(0, 1); err != nil {
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

		h, err := db.Host(hk)
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
	if err := db.UpdateUsabilitySettings(hosts.UsabilitySettings{
		MaxEgressPrice:     settingMaxEgressPrice,
		MaxIngressPrice:    settingMaxIngressPrice,
		MaxStoragePrice:    settingMaxStoragePrice,
		MinCollateral:      settingMinCollataral,
		MinProtocolVersion: rhp.ProtocolVersion400,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(contracts.MaintenanceSettings{
		Period:          settingPeriod,
		RenewWindow:     settingPeriod / 2,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// add a host
	db.addTestHost(t, hk)

	// assert unscanned host is unusable
	if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if h.Usability.Usable() {
		t.Fatal("expected host to be not usable")
	}

	// update host with settings that fail all checks
	err := db.UpdateHostScan(hk, proto4.HostSettings{
		Release:             "test",
		ProtocolVersion:     proto4.ProtocolVersion{0, 0, 0},
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
	}, geoip.Location{}, true, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// fail scan to ensure we fail on uptime
	err = db.UpdateHostScan(hk, proto4.HostSettings{}, geoip.Location{}, false, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// assert host fails all checks
	h, err := db.Host(hk)
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
	if _, err := db.pool.Exec(t.Context(), `UPDATE hosts SET recent_uptime = .9`); err != nil {
		t.Fatal(err)
	}
	assertCheckOK("Uptime")

	// adjust max duration so we pass the check
	hs.MaxContractDuration = settingPeriod
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("MaxContractDuration")

	// adjust max collateral so we pass the check
	hs.MaxCollateral = hs.Prices.Collateral.Mul64(oneTB).Mul64(settingPeriod)
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("MaxCollateral")

	// adjust protocol to pass the check
	hs.ProtocolVersion = rhp.ProtocolVersion400
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("ProtocolVersion")

	// adjust price validity so we pass the check
	hs.Prices.ValidUntil = time.Now().Add(time.Second * 1801)
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("PriceValidity")

	// adjust accepting contracts so we pass the check
	hs.AcceptingContracts = true
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("AcceptingContracts")

	// adjust contract price so we pass the check
	hs.Prices.ContractPrice = oneSC.Sub(oneH)
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("ContractPrice")

	// adjust collateral so we pass the check
	hs.Prices.Collateral = hs.Prices.StoragePrice.Mul64(2)
	hs.MaxCollateral = hs.Prices.Collateral.Mul64(oneTB).Mul64(settingPeriod)
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("Collateral")

	// adjust storage price so we pass the check
	hs.Prices.StoragePrice = settingMaxStoragePrice
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("StoragePrice")

	// adjust egress price so we pass the check
	hs.Prices.EgressPrice = settingMaxEgressPrice
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("EgressPrice")

	// adjust ingress price so we pass the check
	hs.Prices.IngressPrice = settingMaxIngressPrice
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("IngressPrice")

	// adjust free sector price so we pass the check
	hs.Prices.FreeSectorPrice = oneSC.Div64(sectorsPerTB)
	_ = db.UpdateHostScan(hk, hs, geoip.Location{}, true, time.Now())
	assertCheckOK("FreeSectorPrice")

	// assert host is usable
	if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if !h.Usability.Usable() {
		t.Fatal("expected host to be usable")
	}

	// assert valid until takes into account the last successful scan time
	// instead of the current time when calculating whether the price validity
	// check passes
	if _, err := db.pool.Exec(t.Context(), `
		UPDATE hosts SET
			last_successful_scan = last_successful_scan - INTERVAL '1 day',
			settings_valid_until = settings_valid_until - INTERVAL '1 day'`); err != nil {
		t.Fatal(err)
	}
	assertCheckOK("PriceValidity")
}

func TestHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// update global settings to make hosts pass all checks
	if err := db.UpdateUsabilitySettings(hosts.UsabilitySettings{
		MaxEgressPrice:     types.MaxCurrency,
		MaxIngressPrice:    types.MaxCurrency,
		MaxStoragePrice:    types.MaxCurrency,
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: rhp.ProtocolVersion400,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
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
		err := db.UpdateHostScan(hk, settings, geoip.Location{}, false, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		err = db.UpdateHostScan(hk, settings, geoip.Location{}, true, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}

		// block host
		if blocked {
			err := db.BlockHosts([]types.PublicKey{hk}, []string{t.Name()})
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
		hosts, err := db.Hosts(offset, limit, queryOpts...)
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
			} else if host.Blocked && !reflect.DeepEqual(host.BlockedReasons, []string{t.Name()}) {
				t.Fatalf("expected blocked reasons 'test', got %v", host.BlockedReasons)
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

	// public keys
	assertHosts([]types.PublicKey{hk1, hk2}, 0, 4, hosts.WithPublicKeys([]types.PublicKey{hk1, hk2}))
	assertHosts([]types.PublicKey{hk1, hk2, hk3, hk4}, 0, 4, hosts.WithPublicKeys([]types.PublicKey{hk1, hk2, hk3, hk4}))

	// mix filters
	assertHosts([]types.PublicKey{hk3}, 0, 4, hosts.WithUsable(true), hosts.WithBlocked(true))
	assertHosts([]types.PublicKey{hk2}, 0, 4, hosts.WithUsable(false), hosts.WithBlocked(false), hosts.WithActiveContracts(true))
	assertHosts([]types.PublicKey{hk2}, 0, 4, hosts.WithUsable(false), hosts.WithPublicKeys([]types.PublicKey{hk1, hk2}))

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
	assertHosts([]types.PublicKey{hk3}, 1, 1, hosts.WithPublicKeys([]types.PublicKey{hk2, hk3, hk4}))

	// helper to update hosts
	updateHost := func(hk types.PublicKey, column string, value any) {
		t.Helper()

		switch column {
		case "settings_storage_price",
			"settings_contract_price",
			"settings_collateral",
			"settings_ingress_price",
			"settings_egress_price",
			"settings_free_sector_price",
			"settings_max_collateral":
			value = sqlCurrency(value.(types.Currency))
		case "settings_protocol_version":
			value = sqlProtocolVersion(value.(proto4.ProtocolVersion))
		default:
		}
		res, err := db.pool.Exec(t.Context(), fmt.Sprintf(`UPDATE hosts SET %s = $1 WHERE public_key = $2`, column), value, sqlPublicKey(hk))
		if err != nil {
			t.Fatal(err)
		} else if res.RowsAffected() != 1 {
			t.Fatalf("expected 1 row to be affected, got %d", res.RowsAffected())
		}
	}

	assertHostsOrder := func(hks []types.PublicKey, offset, limit int, queryOpts ...hosts.HostQueryOpt) {
		t.Helper()
		hosts, err := db.Hosts(offset, limit, queryOpts...)
		if err != nil {
			t.Fatal(err)
		} else if len(hosts) != len(hks) {
			t.Fatalf("expected %v hosts, got %v", len(hks), len(hosts))
		}
		for i, host := range hosts {
			if host.PublicKey != hks[i] {
				t.Fatalf("expected hk %v, got %v", hks[i], host.PublicKey)
			}
		}
	}

	// sorting
	withHKFilter := hosts.WithPublicKeys([]types.PublicKey{hk1, hk2})
	settings := newSettings(types.PublicKey{})
	sortCases := []struct {
		name        string
		column      string
		low, higher any
	}{
		{"recentUptime", "recent_uptime", 0.91, 0.92},
		{"settings.protocolVersion", "settings_protocol_version", rhp.ProtocolVersion400, rhp.ProtocolVersion501},
		{"settings.acceptingContracts", "settings_accepting_contracts", false, true},
		{"settings.maxCollateral", "settings_max_collateral", types.Siacoins(1), types.Siacoins(2)},
		{"settings.maxContractDuration", "settings_max_contract_duration", int64(10), int64(20)},
		{"settings.remainingStorage", "settings_remaining_storage", int64(50), int64(100)},
		{"settings.totalStorage", "settings_total_storage", int64(60), int64(120)},
		{"settings.prices.contractPrice", "settings_contract_price", settings.Prices.ContractPrice, settings.Prices.ContractPrice.Mul64(2)},
		{"settings.prices.collateral", "settings_collateral", settings.Prices.Collateral, settings.Prices.Collateral.Mul64(2)},
		{"settings.prices.storagePrice", "settings_storage_price", settings.Prices.StoragePrice, settings.Prices.StoragePrice.Mul64(2)},
		{"settings.prices.ingressPrice", "settings_ingress_price", settings.Prices.IngressPrice, settings.Prices.IngressPrice.Mul64(2)},
		{"settings.prices.egressPrice", "settings_egress_price", settings.Prices.EgressPrice, settings.Prices.EgressPrice.Mul64(2)},
		{"settings.prices.freeSectorPrice", "settings_free_sector_price", types.NewCurrency64(1), types.NewCurrency64(2)},
	}
	for _, tc := range sortCases {
		updateHost(hk1, tc.column, tc.low)
		updateHost(hk2, tc.column, tc.higher)
		assertHostsOrder([]types.PublicKey{hk1, hk2}, 0, 2, withHKFilter, hosts.WithSorting(tc.name, false))
		assertHostsOrder([]types.PublicKey{hk2, hk1}, 0, 2, withHKFilter, hosts.WithSorting(tc.name, true))
	}
}

func TestUsableHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// update global settings to make hosts pass all checks
	if err := db.UpdateUsabilitySettings(hosts.UsabilitySettings{
		MaxEgressPrice:     types.MaxCurrency,
		MaxIngressPrice:    types.MaxCurrency,
		MaxStoragePrice:    types.MaxCurrency,
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: rhp.ProtocolVersion400,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// helper to add hosts
	addHost := func(i byte, loc geoip.Location, protocols []chain.Protocol, usable, blocked bool, contract bool) types.PublicKey {
		t.Helper()

		hk := types.PublicKey{i}
		db.addTestHost(t, hk)

		var netAddrs []chain.NetAddress
		for _, protocol := range protocols {
			netAddrs = append(netAddrs, chain.NetAddress{Protocol: protocol, Address: "[::]:4848"})
		}
		if err := db.UpdateChainState(func(tx subscriber.UpdateTx) error {
			return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement(netAddrs), time.Now())
		}); err != nil {
			t.Fatal(err)
		}

		// handle usable
		settings := newTestHostSettings(hk)
		if !usable {
			settings.AcceptingContracts = false
		}
		if err := db.UpdateHostScan(hk, settings, loc, true, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}

		// handle contract
		if contract {
			db.addTestContract(t, hk, types.FileContractID(hk))
		}

		// handle blocked
		if blocked {
			err := db.BlockHosts([]types.PublicKey{hk}, []string{t.Name()})
			if err != nil {
				t.Fatal(err)
			}
		}

		return hk
	}

	locationUS := geoip.Location{
		CountryCode: "US",
		Latitude:    10,
		Longitude:   -20,
	}
	locationAU := geoip.Location{
		CountryCode: "AU",
		Latitude:    30,
		Longitude:   -40,
	}
	bothProtocols := []chain.Protocol{siamux.Protocol, quic.Protocol}
	siamuxProtocol := []chain.Protocol{siamux.Protocol}
	// add hosts in all possible configurations
	_ = addHost(1, locationUS, siamuxProtocol, false, false, false)
	_ = addHost(2, locationUS, siamuxProtocol, false, false, true)
	_ = addHost(3, locationUS, siamuxProtocol, false, true, false)
	_ = addHost(4, locationUS, siamuxProtocol, false, true, true)
	_ = addHost(5, locationAU, siamuxProtocol, true, false, false)
	uh1 := addHost(6, locationUS, siamuxProtocol, true, false, true)
	_ = addHost(7, locationUS, siamuxProtocol, true, true, false)
	_ = addHost(8, locationUS, bothProtocols, true, true, true)
	uh2 := addHost(9, locationAU, bothProtocols, true, false, true) // second usable host

	// assert only h6 and h9 are returned
	if hosts, err := db.UsableHosts(0, 10); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 || hosts[1].PublicKey != uh2 {
		t.Fatal("unexpected hosts", hosts[0], hosts[1])
	} else if hosts[0].Addresses == nil || hosts[1].Addresses == nil {
		t.Fatal("expected hosts to have addresses")
	}

	// assert offset and limit are applied
	if hosts, err := db.UsableHosts(0, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 {
		t.Fatal("unexpected host", hosts[0].PublicKey)
	} else if hosts, err := db.UsableHosts(1, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh2 {
		t.Fatal("unexpected host", hosts[0].PublicKey)
	} else if hosts, err := db.UsableHosts(2, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 0 {
		t.Fatal("expected no hosts, got", len(hosts))
	}

	// filter protocols
	if hosts, err := db.UsableHosts(0, 10, hosts.WithProtocol(siamux.Protocol)); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 || hosts[1].PublicKey != uh2 {
		t.Fatal("unexpected hosts", hosts[0], hosts[1])
	} else if hosts[0].Addresses == nil || hosts[1].Addresses == nil {
		t.Fatal("expected hosts to have addresses")
	}

	if hosts, err := db.UsableHosts(0, 10, hosts.WithProtocol(quic.Protocol)); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh2 {
		t.Fatal("unexpected host", hosts[0])
	} else if hosts[0].Addresses == nil {
		t.Fatal("expected host to have address")
	}

	// filter by country
	if hosts, err := db.UsableHosts(0, 10, hosts.WithCountry(locationUS.CountryCode)); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 {
		t.Fatal("unexpected host", hosts[0])
	} else if hosts[0].Location() != locationUS {
		t.Fatalf("expected location %v, got %v", locationUS, hosts[0].Location())
	}

	if hosts, err := db.UsableHosts(0, 10, hosts.WithCountry(locationAU.CountryCode)); err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh2 {
		t.Fatal("unexpected host", hosts[0])
	} else if hosts[0].Location() != locationAU {
		t.Fatalf("expected location %v, got %v", locationAU, hosts[0].Location())
	}

	// block usable hosts and add 3 new usable hosts in the EU
	db.BlockHosts([]types.PublicKey{uh1, uh2}, []string{t.Name()})
	uh1 = addHost(10, geoip.Location{
		CountryCode: "ES",    // Spain
		Latitude:    41.3851, // Barcelona
		Longitude:   2.1734,
	}, siamuxProtocol, true, false, true)
	uh2 = addHost(11, geoip.Location{
		CountryCode: "DE",    // Germany
		Latitude:    50.1109, // Frankfurt
		Longitude:   8.6821,
	}, siamuxProtocol, true, false, true)
	uh3 := addHost(12, geoip.Location{
		CountryCode: "FR",    // France
		Latitude:    48.8566, // Paris
		Longitude:   2.3522,
	}, siamuxProtocol, true, false, true)

	// assert sorting by distance works
	if hosts, err := db.UsableHosts(0, 10, hosts.SortByDistance(50.8503, 4.3517)); err != nil { // Brussels
		t.Fatal("unexpected", err)
	} else if len(hosts) != 3 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh3 {
		t.Fatal("unexpected host", hosts[0])
	} else if hosts[1].PublicKey != uh2 {
		t.Fatal("unexpected host", hosts[1])
	} else if hosts[2].PublicKey != uh1 {
		t.Fatal("unexpected host", hosts[2])
	}

	// now try fetching it from Portugal
	if hosts, err := db.UsableHosts(0, 10, hosts.SortByDistance(38.7223, -9.1393)); err != nil { // Lisbon
		t.Fatal("unexpected", err)
	} else if len(hosts) != 3 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != uh1 {
		t.Fatal("unexpected host", hosts[0])
	} else if hosts[1].PublicKey != uh3 {
		t.Fatal("unexpected host", hosts[1])
	} else if hosts[2].PublicKey != uh2 {
		t.Fatal("unexpected host", hosts[2])
	}

	// add host without location
	_ = addHost(13, geoip.Location{
		CountryCode: "FR", // France
		Latitude:    0,
		Longitude:   0,
	}, siamuxProtocol, true, false, true)
}

func TestHostsForScanning(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// assert both hosts are returned
	hosts, err := db.HostsForScanning()
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts))
	}

	// simulate scanning h1 successfully
	nextScan := time.Now().Round(time.Microsecond).Add(time.Minute)
	err = db.UpdateHostScan(hk1, proto4.HostSettings{}, geoip.Location{}, true, nextScan)
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert only h2 is returned
	hosts, err = db.HostsForScanning()
	if err != nil {
		t.Fatal("unexpected", err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0] != hk2 {
		t.Fatal("unexpected", hosts[0])
	}

	// simulate scanning h2 successfully
	err = db.UpdateHostScan(hk2, proto4.HostSettings{}, geoip.Location{}, true, nextScan)
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert no hosts are returned
	hosts, err = db.HostsForScanning()
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
	db.addTestAccount(t, types.PublicKey(account))

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
	_, err := db.PinSlabs(account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
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
		hosts, err := db.HostsWithLostSectors()
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

	if err := db.MarkSectorsLost(hk1, []types.Hash256{root1}); err != nil {
		t.Fatal(err)
	}

	// hk1 should be returned after one of its sectors is marked as lost
	assertHosts([]types.PublicKey{hk1})

	if err := db.MarkSectorsLost(hk1, []types.Hash256{root2}); err != nil {
		t.Fatal(err)
	}

	// still only hk1 should be returned after another one of its sectors is
	// marked as lost
	assertHosts([]types.PublicKey{hk1})

	if err := db.MarkSectorsLost(hk2, []types.Hash256{root3}); err != nil {
		t.Fatal(err)
	}

	// hk1 and hk2 should be returned after both have lost sectors
	assertHosts([]types.PublicKey{hk1, hk2})
}

func TestResetLostSectors(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add account
	account := proto4.Account{1}
	db.addTestAccount(t, types.PublicKey(account))

	// add two hosts
	hk1 := db.addTestHost(t)
	hk2 := db.addTestHost(t)

	// add a contract for each host
	db.addTestContract(t, hk1)
	db.addTestContract(t, hk2)

	// pin a slab that adds sectors to each host
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	_, err := db.PinSlabs(account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: root1, HostKey: hk1},
			{Root: root2, HostKey: hk2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// mark sectors as lost
	if err := db.MarkSectorsLost(hk1, []types.Hash256{root1}); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkSectorsLost(hk2, []types.Hash256{root2}); err != nil {
		t.Fatal(err)
	}

	// verify both hosts have lost sectors
	h1, err := db.Host(hk1)
	if err != nil {
		t.Fatal(err)
	} else if h1.LostSectors != 1 {
		t.Fatalf("expected 1 lost sector for hk1, got %d", h1.LostSectors)
	}

	h2, err := db.Host(hk2)
	if err != nil {
		t.Fatal(err)
	} else if h2.LostSectors != 1 {
		t.Fatalf("expected 1 lost sector for hk2, got %d", h2.LostSectors)
	}

	// reset lost sectors for hk1
	if err := db.ResetLostSectors(hk1); err != nil {
		t.Fatal(err)
	}

	// verify hk1 has 0 lost sectors
	h1, err = db.Host(hk1)
	if err != nil {
		t.Fatal(err)
	} else if h1.LostSectors != 0 {
		t.Fatalf("expected 0 lost sectors for hk1 after reset, got %d", h1.LostSectors)
	}

	// verify hk2 still has 1 lost sector (unchanged)
	h2, err = db.Host(hk2)
	if err != nil {
		t.Fatal(err)
	} else if h2.LostSectors != 1 {
		t.Fatalf("expected 1 lost sector for hk2, got %d", h2.LostSectors)
	}

	// verify only hk2 is returned by HostsWithLostSectors
	hostsWithLost, err := db.HostsWithLostSectors()
	if err != nil {
		t.Fatal(err)
	} else if len(hostsWithLost) != 1 {
		t.Fatalf("expected 1 host with lost sectors, got %d", len(hostsWithLost))
	} else if hostsWithLost[0] != hk2 {
		t.Fatalf("expected hk2, got %v", hostsWithLost[0])
	}

	// test resetting a non-existent host returns ErrNotFound
	if err := db.ResetLostSectors(types.PublicKey{99}); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatalf("expected hosts.ErrNotFound, got %v", err)
	}
}

func TestStuckHosts(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	addHost := func() types.PublicKey {
		t.Helper()

		hk := db.addTestHost(t)
		db.addTestContract(t, hk)
		if _, err := db.pool.Exec(t.Context(), "UPDATE hosts SET usage_total_spent = $1 WHERE public_key = $2", sqlCurrency(types.NewCurrency64(1)), sqlPublicKey(hk)); err != nil {
			t.Fatal(err)
		}
		return hk
	}

	// add three hosts
	hk1 := addHost()
	hk2 := addHost()
	hk3 := addHost()

	assertStuckHosts := func(expected []types.PublicKey) {
		t.Helper()
		stuckHosts, err := db.StuckHosts()
		if err != nil {
			t.Fatal(err)
		}
		if len(stuckHosts) != len(expected) {
			t.Fatalf("expected %d stuck hosts, got %d", len(expected), len(stuckHosts))
		}
		for _, exp := range expected {
			found := false
			for _, sh := range stuckHosts {
				if sh.PublicKey == exp {
					if sh.StuckSince.IsZero() {
						t.Fatalf("stuck since time should not be zero for host %v", exp)
					}
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected host %v to be stuck", exp)
			}
		}

		stats, err := db.HostStats(0, 100)
		if err != nil {
			t.Fatal(err)
		}

		var count int
		for _, h := range stats {
			if h.Stuck {
				count++
			}
		}
		if count != len(expected) {
			t.Fatalf("expected %d stuck hosts, got %d", len(expected), count)
		}
	}

	// initially no hosts should be stuck
	assertStuckHosts(nil)

	// mark hk1 as stuck
	if err := db.UpdateStuckHosts([]types.PublicKey{hk1}, nil); err != nil {
		t.Fatal(err)
	}

	// hk1 should still not be returned as stuck yet (less than 24 hours)
	assertStuckHosts(nil)

	// manually set stuck_since to 25 hours ago for hk1 to simulate the 24h threshold
	_, err := db.pool.Exec(t.Context(), `UPDATE hosts SET stuck_since = NOW() - INTERVAL '25 hours' WHERE public_key = $1`, sqlPublicKey(hk1))
	if err != nil {
		t.Fatal(err)
	}

	// now hk1 should be stuck
	assertStuckHosts([]types.PublicKey{hk1})

	// mark hk2 as stuck too (but recent, so not returned yet)
	if err := db.UpdateStuckHosts([]types.PublicKey{hk2}, nil); err != nil {
		t.Fatal(err)
	}

	// still only hk1 should be returned (hk2 is recent)
	assertStuckHosts([]types.PublicKey{hk1})

	// manually set stuck_since to 25 hours ago for hk2
	_, err = db.pool.Exec(t.Context(), `UPDATE hosts SET stuck_since = NOW() - INTERVAL '25 hours' WHERE public_key = $1`, sqlPublicKey(hk2))
	if err != nil {
		t.Fatal(err)
	}

	// now both hk1 and hk2 should be stuck
	assertStuckHosts([]types.PublicKey{hk1, hk2})

	// test that UpdateStuckHosts clears hosts in the unstuck list
	if err := db.UpdateStuckHosts(nil, []types.PublicKey{hk1}); err != nil {
		t.Fatal(err)
	}

	// now only hk2 should be stuck
	assertStuckHosts([]types.PublicKey{hk2})

	// check UpdateStuckHosts preserves stuck_since if already set
	var stuckSince time.Time
	err = db.pool.QueryRow(t.Context(), `SELECT stuck_since FROM hosts WHERE public_key = $1`, sqlPublicKey(hk2)).Scan(&stuckSince)
	if err != nil {
		t.Fatal(err)
	}

	// call UpdateStuckHosts again with same host
	if err := db.UpdateStuckHosts([]types.PublicKey{hk2}, nil); err != nil {
		t.Fatal(err)
	}

	// stuck_since should not have changed
	var stuckSinceAfter time.Time
	err = db.pool.QueryRow(t.Context(), `SELECT stuck_since FROM hosts WHERE public_key = $1`, sqlPublicKey(hk2)).Scan(&stuckSinceAfter)
	if err != nil {
		t.Fatal(err)
	}
	if !stuckSince.Equal(stuckSinceAfter) {
		t.Fatalf("expected stuck_since to remain %v, got %v", stuckSince, stuckSinceAfter)
	}

	// test clearing stuck host
	if err := db.UpdateStuckHosts(nil, []types.PublicKey{hk2}); err != nil {
		t.Fatal(err)
	}

	// no hosts should be stuck anymore
	assertStuckHosts(nil)

	// test marking multiple hosts as stuck at once
	if err := db.UpdateStuckHosts([]types.PublicKey{hk1, hk2, hk3}, nil); err != nil {
		t.Fatal(err)
	}

	// manually set all to 25 hours ago
	_, err = db.pool.Exec(t.Context(), `UPDATE hosts SET stuck_since = NOW() - INTERVAL '25 hours' WHERE public_key = ANY($1)`,
		[]sqlPublicKey{sqlPublicKey(hk1), sqlPublicKey(hk2), sqlPublicKey(hk3)})
	if err != nil {
		t.Fatal(err)
	}

	// all three should be stuck
	assertStuckHosts([]types.PublicKey{hk1, hk2, hk3})
}

func TestHostsWithUnpinnableSectors(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// add account
	account := proto4.Account{1}
	db.addTestAccount(t, types.PublicKey(account))

	// host1 has no sectors and no contracts -> not returned
	db.addTestHost(t)

	// host2 has no sectors, but a contract -> not returned
	hk2 := db.addTestHost(t)
	hk2FCID := db.addTestContract(t, hk2)

	// host3 has a sector, but no contracts -> returned
	hk3 := db.addTestHost(t)

	// host4 has a sector and a contract -> not returned
	hk4 := db.addTestHost(t)
	hk4FCID := db.addTestContract(t, hk4)

	_, err := db.PinSlabs(account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				Root:    types.Hash256(hk3),
				HostKey: hk3,
			},
			{
				Root:    types.Hash256(hk4),
				HostKey: hk4,
			},
			{
				Root:    frand.Entropy256(),
				HostKey: hk4,
			},
			{
				Root:    frand.Entropy256(),
				HostKey: hk4,
			},
			{
				Root:    frand.Entropy256(),
				HostKey: hk4,
			},
			{
				Root:    frand.Entropy256(),
				HostKey: hk4,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertHostsWithUnpinnableSectors := func(expected []types.PublicKey) {
		t.Helper()
		hosts, err := db.HostsWithUnpinnableSectors()
		if err != nil {
			t.Fatal(err)
		} else if len(expected) != len(hosts) {
			t.Fatalf("expected %d hosts, got %d", len(expected), len(hosts))
		}
		for i, host := range hosts {
			if host != expected[i] {
				t.Fatalf("expected PublicKey %v, got %v", expected[i], host)
			}
		}
	}

	assertHostsWithUnpinnableSectors([]types.PublicKey{hk3})

	// manually pin the sector for h3 to a random contract
	if res, err := db.pool.Exec(t.Context(), `UPDATE sectors SET contract_sectors_map_id = 1 WHERE sector_root = $1`, sqlHash256(types.Hash256(hk3))); err != nil {
		t.Fatal(err)
	} else if res.RowsAffected() != 1 {
		t.Fatalf("expected 1 row to be affected, got %d", res.RowsAffected())
	}

	// assert no hosts are returned now because the sector is no longer unpinned
	assertHostsWithUnpinnableSectors(nil)

	// mark contract for host4 as resolved - contract should now be invalid
	if err := db.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractState(hk4FCID, contracts.ContractStateResolved)
	}); err != nil {
		t.Fatal(err)
	}

	assertHostsWithUnpinnableSectors([]types.PublicKey{hk4})

	// reset the contract state, but mark the contract bad - contract should
	// still be considered invalid and host4 returned
	if res, err := db.pool.Exec(t.Context(), `UPDATE contracts SET good = FALSE, state = 0 WHERE contract_id = $1`, sqlHash256(hk4FCID)); err != nil {
		t.Fatal(err)
	} else if res.RowsAffected() != 1 {
		t.Fatalf("expected 1 row to be affected, got %d", res.RowsAffected())
	}

	assertHostsWithUnpinnableSectors([]types.PublicKey{hk4})

	// reset the contract to good, but now indicate it's renewed - contract should
	// still be considered invalid and host4 returned
	if res, err := db.pool.Exec(t.Context(), `UPDATE contracts SET good = TRUE, renewed_to = $1 WHERE contract_id = $2`, sqlHash256(hk2FCID), sqlHash256(hk4FCID)); err != nil {
		t.Fatal(err)
	} else if res.RowsAffected() != 1 {
		t.Fatalf("expected 1 row to be affected, got %d", res.RowsAffected())
	}

	assertHostsWithUnpinnableSectors([]types.PublicKey{hk4})

	// reset renewed_to - contract should be considered valid and no hosts returned
	if res, err := db.pool.Exec(t.Context(), `UPDATE contracts SET renewed_to = NULL WHERE contract_id = $1`, sqlHash256(hk4FCID)); err != nil {
		t.Fatal(err)
	} else if res.RowsAffected() != 1 {
		t.Fatalf("expected 1 row to be affected, got %d", res.RowsAffected())
	}

	assertHostsWithUnpinnableSectors(nil)
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
				_, err1 := db.pool.Exec(t.Context(), `UPDATE hosts SET last_failed_scan = '0001-01-01 00:00:00+00'::timestamptz, last_successful_scan = NOW() - INTERVAL '24 hours'`)
				if err := errors.Join(err1, db.UpdateHostScan(hk, newTestHostSettings(hk), geoip.Location{}, true, time.Time{})); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			for range n {
				_, err1 := db.pool.Exec(t.Context(), `UPDATE hosts SET last_successful_scan = '0001-01-01 00:00:00+00'::timestamptz, last_failed_scan = NOW() - INTERVAL '24 hours'`)
				if err := errors.Join(err1, db.UpdateHostScan(hk, proto4.HostSettings{}, geoip.Location{}, false, time.Time{})); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	uptime := func() float64 {
		t.Helper()
		h, err := db.Host(hk)
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
	_, err := db.pool.Exec(t.Context(), `UPDATE hosts SET recent_uptime = .894`)
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
	_, err = db.pool.Exec(t.Context(), `UPDATE hosts SET recent_uptime = .999999999`)
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
	n, err := db.PruneHosts(time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("unexpected", n)
	}

	// re-add the hosts
	h1 := db.addTestHost(t)
	h2 := db.addTestHost(t)

	// assert none get pruned when we require at least one failed scan
	n, err = db.PruneHosts(time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("unexpected", n)
	}

	// simulate failed scan for h1
	err = db.UpdateHostScan(h1, proto4.HostSettings{}, geoip.Location{}, false, time.Now())
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert h1 gets pruned
	n, err = db.PruneHosts(time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	} else if _, err := db.Host(h1); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// simulate failed scan for h2
	err = db.UpdateHostScan(h2, proto4.HostSettings{}, geoip.Location{}, false, time.Now())
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert h2 gets pruned
	n, err = db.PruneHosts(time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	}

	// re-add both hosts, simulate both a successful and failed scan
	h1 = db.addTestHost(t)
	h2 = db.addTestHost(t)
	err = errors.Join(
		db.UpdateHostScan(h1, proto4.HostSettings{}, geoip.Location{}, true, time.Now()),
		db.UpdateHostScan(h1, proto4.HostSettings{}, geoip.Location{}, false, time.Now()),
		db.UpdateHostScan(h2, proto4.HostSettings{}, geoip.Location{}, true, time.Now()),
		db.UpdateHostScan(h2, proto4.HostSettings{}, geoip.Location{}, false, time.Now()),
	)
	if err != nil {
		t.Fatal("unexpected", err)
	}

	// assert both do not get pruned if we set the cutoff in the past
	n, err = db.PruneHosts(time.Now().Add(-time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("unexpected", n)
	}

	// add contract to h2
	db.addTestContract(t, h2)

	// assert only h1 got pruned if we set the cutoff in the future
	n, err = db.PruneHosts(time.Now().Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("unexpected", n)
	} else if _, err = db.Host(h1); !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// delete all contracts
	if _, err := db.pool.Exec(t.Context(), "DELETE FROM contract_sectors_map; DELETE FROM contracts;"); err != nil {
		t.Fatal(err)
	}

	// assert h2 gets pruned now as well
	n, err = db.PruneHosts(time.Now().Add(time.Second), 1)
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
	err := db.UpdateHostScan(hk, proto4.HostSettings{}, geoip.Location{}, false, time.Now())
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected [hosts.ErrNotFound], got", err)
	}

	// add a host
	db.addTestHost(t, hk)

	// assert host settings are not inserted if the scan failed
	hs := newTestHostSettings(hk)
	err = db.UpdateHostScan(hk, hs, geoip.Location{}, false, time.Now())
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if h.Settings != (proto4.HostSettings{}) {
		t.Fatal("expected no settings", h.Settings, proto4.HostSettings{})
	} else if !h.LastSuccessfulScan.IsZero() {
		t.Fatal("expected no last successful scan")
	} else if h.LastFailedScan.IsZero() {
		t.Fatal("expected last failed scan to be set")
	}

	// assert consecutive failed scans are incremented
	err = db.UpdateHostScan(hk, hs, geoip.Location{}, false, time.Now())
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if h.ConsecutiveFailedScans != 2 {
		t.Fatal("unexpected", h.ConsecutiveFailedScans)
	}

	now := time.Now().Round(time.Minute)
	nextScan := now.Add(time.Hour)

	location := geoip.Location{
		CountryCode: "US",
		Latitude:    10,
		Longitude:   -20,
	}
	// assert host is properly updated on successful scan
	err = db.UpdateHostScan(hk, hs, location, true, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(hk); err != nil {
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
	} else if h.CountryCode != location.CountryCode {
		t.Fatal("unexpected country code", h.CountryCode)
	} else if h.Latitude != location.Latitude {
		t.Fatal("unexpected Latitude", h.Latitude)
	} else if h.Longitude != location.Longitude {
		t.Fatal("unexpected Longitude", h.Longitude)
	}

	// assert updating with a failed scan doesn't affect the host's location or
	// country code
	err = db.UpdateHostScan(hk, hs, geoip.Location{}, false, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if h.CountryCode != location.CountryCode {
		t.Fatal("unexpected country code", h.CountryCode)
	} else if h.Latitude != location.Latitude {
		t.Fatal("unexpected Latitude", h.Latitude)
	} else if h.Longitude != location.Longitude {
		t.Fatal("unexpected Longitude", h.Longitude)
	}

	// assert updating with a null location doesn't affect the host's location
	// or country code
	err = db.UpdateHostScan(hk, hs, geoip.Location{}, true, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.Host(hk); err != nil {
		t.Fatal(err)
	} else if h.CountryCode != location.CountryCode {
		t.Fatal("unexpected country code", h.CountryCode)
	} else if h.Latitude != location.Latitude {
		t.Fatal("unexpected Latitude", h.Latitude)
	} else if h.Longitude != location.Longitude {
		t.Fatal("unexpected Longitude", h.Longitude)
	}
}

func TestUpdateHostPrices(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	if err := db.UpdateUsabilitySettings(hosts.UsabilitySettings{
		MaxEgressPrice:     types.Siacoins(10),
		MaxIngressPrice:    types.Siacoins(10),
		MaxStoragePrice:    types.Siacoins(10),
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: rhp.ProtocolVersion400,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMaintenanceSettings(contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// add a host
	hostKey := types.GeneratePrivateKey().PublicKey()
	db.addTestHost(t, hostKey)

	// generate host settings with prices below the gouging thresholds
	settings := newTestHostSettings(hostKey)
	settings.Prices.ValidUntil = time.Now().Add(24 * time.Hour)
	settings.Prices.EgressPrice = types.Siacoins(5).Div64(1e12)              // 5 SC / TB
	settings.Prices.IngressPrice = types.Siacoins(5).Div64(1e12)             // 5 SC / TB / month
	settings.Prices.StoragePrice = types.Siacoins(5).Div64(1e12).Div64(4320) // 5 SC / TB / month
	settings.Prices.Collateral = types.Siacoins(10).Div64(1e12).Div64(4320)  // 10 SC / TB / mo
	settings.MaxCollateral = types.Siacoins(100)

	// add a successful scan with non-gouging prices
	if err := db.UpdateHostScan(hostKey, settings, geoip.Location{}, true, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// check that the host is usable
	host, err := db.Host(hostKey)
	if err != nil {
		t.Fatal(err)
	} else if !host.IsGood() {
		t.Fatal("expected host to be good")
	}

	// try to update the prices to a lower valid until time which should fail
	// and the host should continue to be good
	settings.Prices.ValidUntil = time.Now().Add(30 * time.Second)
	if err := db.UpdateHostPrices(hostKey, settings.Prices); err != nil {
		t.Fatal(err)
	} else if host, err := db.Host(hostKey); err != nil {
		t.Fatal(err)
	} else if !host.IsGood() {
		t.Fatal("expected host to be good")
	}

	// update host settings with gouging prices
	settings.Prices.ValidUntil = time.Now().Add(24 * time.Hour)
	settings.Prices.EgressPrice = types.Siacoins(15)
	if err := db.UpdateHostPrices(hostKey, settings.Prices); err != nil {
		t.Fatal(err)
	} else if host, err := db.Host(hostKey); err != nil {
		t.Fatal(err)
	} else if host.IsGood() {
		t.Fatal("expected host to be not good due to egress price gouging")
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
		numBlocklist = 1_000
		numContracts = 25 // ~40% is active
	)

	// prepare database
	hosts := make([]types.PublicKey, numHosts)
	store := initPostgres(b, zap.NewNop())
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for i := range numHosts {
			var hostID int64
			hk := types.GeneratePrivateKey().PublicKey()
			err := tx.QueryRow(b.Context(), `INSERT INTO hosts (public_key, lost_sectors, usage_account_funding, usage_total_spent, last_announcement) VALUES ($1, $2, $3, $4, NOW()) RETURNING id;`,
				sqlPublicKey(hk),
				frand.Uint64n(1e3), // random lost sectors
				sqlCurrency(types.NewCurrency64(frand.Uint64n(1e6))), // random account funding
				sqlCurrency(types.NewCurrency64(frand.Uint64n(1e6))), // random total spent
			).Scan(&hostID)
			if err != nil {
				return err
			}
			hosts[i] = hk

			na := chain.NetAddress{Protocol: siamux.Protocol, Address: "foo"}
			_, err = tx.Exec(ctx, `INSERT INTO host_addresses (host_id, net_address, protocol) VALUES ($1, $2, $3)`, hostID, na.Address, sqlNetworkProtocol(na.Protocol))
			if err != nil {
				return fmt.Errorf("failed to insert host address: %w", err)
			}

			for range numContracts {
				insertRandomContract(b, tx, hostID, hk)
			}
		}

		// we LEFT JOIN the blocklist so we populate it with random entries
		for range numBlocklist {
			_, err := tx.Exec(ctx, "INSERT INTO hosts_blocklist (public_key) VALUES ($1)", sqlPublicKey(types.GeneratePrivateKey().PublicKey()))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	if err := errors.Join(vErr1, vErr2); err != nil {
		b.Fatal(err)
	}

	b.Run("Host", func(b *testing.B) {
		var i int
		for b.Loop() {
			_, err := store.Host(hosts[i%numHosts])
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
				hosts, err := store.Hosts(offset, limit)
				if err != nil {
					b.Fatal(err)
				} else if len(hosts) != limit {
					b.Fatalf("expected %d hosts, got %d", limit, len(hosts)) // sanity check
				}
			}
		})
	}

	for _, limit := range []int{100, 500} {
		b.Run(fmt.Sprintf("HostStats_%d", limit), func(b *testing.B) {
			for b.Loop() {
				offset := frand.Intn(numHosts - limit)
				stats, err := store.HostStats(offset, limit)
				if err != nil {
					b.Fatal(err)
				} else if len(stats) != limit {
					b.Fatalf("expected %d host stats, got %d", limit, len(stats)) // sanity check
				}
			}
		})
	}

	b.Run("UpdateHost", func(b *testing.B) {
		ts := time.Now()
		hs := proto4.HostSettings{
			ProtocolVersion:     proto4.ProtocolVersion{1, 2, 3},
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
			err := store.UpdateHostScan(hk, hs, geoip.Location{}, succeeded, ts)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkUsableHosts is a set of benchmarks that verify the performance of
// the UsableHosts method.
func BenchmarkUsableHosts(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// update global settings to make hosts pass all checks
	if err := store.UpdateUsabilitySettings(hosts.UsabilitySettings{
		MaxEgressPrice:     types.MaxCurrency,
		MaxIngressPrice:    types.MaxCurrency,
		MaxStoragePrice:    types.MaxCurrency,
		MinCollateral:      types.ZeroCurrency,
		MinProtocolVersion: rhp.ProtocolVersion400,
	}); err != nil {
		b.Fatal(err)
	}
	if err := store.UpdateMaintenanceSettings(contracts.MaintenanceSettings{
		Period:          2,
		RenewWindow:     1,
		WantedContracts: 1,
	}); err != nil {
		b.Fatal(err)
	}

	randomCountry := func() string {
		countries := []string{"US", "DE", "FR", "CN", "JP", "IN", "BR", "RU", "GB", "IT", "ES", "CA", "AU"}
		return countries[frand.Intn(len(countries))]
	}

	randomLocation := func() pgtype.Point {
		return pgtype.Point{
			P: pgtype.Vec2{
				X: frand.Float64()*180 - 90,
				Y: frand.Float64()*360 - 180,
			},
			Valid: true,
		}
	}

	randomProtocol := func() chain.Protocol {
		protocols := []chain.Protocol{siamux.Protocol, quic.Protocol}
		return protocols[frand.Intn(len(protocols))]
	}

	// define parameters
	const (
		numHosts     = 10_000
		defaultLimit = 100
		maxLimit     = 500
	)

	// prepare random hosts
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for range numHosts {
			hk := types.GeneratePrivateKey().PublicKey()
			hs := newTestHostSettings(hk)

			var hostID int64
			err := tx.QueryRow(b.Context(), `
				INSERT INTO hosts (
					public_key,
					last_announcement,
					last_successful_scan,
					settings_protocol_version,
					settings_release,
					settings_wallet_address,
					settings_accepting_contracts,
					settings_max_collateral,
					settings_max_contract_duration,
					settings_remaining_storage,
					settings_total_storage,
					settings_contract_price,
					settings_collateral,
					settings_storage_price,
					settings_ingress_price,
					settings_egress_price,
					settings_free_sector_price,
					settings_tip_height,
					settings_valid_until,
					settings_signature,
					location,
					country_code
				) VALUES ($1, NOW(), NOW(), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING id;`,
				sqlPublicKey(hk),
				sqlProtocolVersion(hs.ProtocolVersion),
				hs.Release,
				sqlHash256(hs.WalletAddress),
				hs.AcceptingContracts,
				sqlCurrency(hs.MaxCollateral),
				hs.MaxContractDuration,
				hs.RemainingStorage,
				hs.TotalStorage,
				sqlCurrency(hs.Prices.ContractPrice),
				sqlCurrency(hs.Prices.Collateral),
				sqlCurrency(hs.Prices.StoragePrice),
				sqlCurrency(hs.Prices.IngressPrice),
				sqlCurrency(hs.Prices.EgressPrice),
				sqlCurrency(hs.Prices.FreeSectorPrice),
				hs.Prices.TipHeight,
				hs.Prices.ValidUntil,
				sqlSignature(hs.Prices.Signature),
				randomLocation(),
				randomCountry(),
			).Scan(&hostID)
			if err != nil {
				return err
			}

			// add host addresses (50% QUIC)
			if _, err := tx.Exec(ctx, `
				INSERT INTO host_addresses (host_id, net_address, protocol)
				VALUES ($1, $2, $3)`, hostID, "foo.com", sqlNetworkProtocol(randomProtocol())); err != nil {
				return fmt.Errorf("failed to insert host address: %w", err)
			}

			// add contract
			insertRandomContract(b, tx, hostID, hk)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	if err := errors.Join(vErr1, vErr2); err != nil {
		b.Fatal(err)
	}

	for _, test := range []struct {
		name  string
		limit int
	}{
		{"DefaultLimit", defaultLimit},
		{"MaxLimit", maxLimit},
	} {
		b.Run("UsableHosts_"+test.name, func(b *testing.B) {
			for b.Loop() {
				hosts, err := store.UsableHosts(0, test.limit)
				if err != nil {
					b.Fatal(err)
				} else if len(hosts) != test.limit {
					b.Fatalf("sanity check failed, found %d usable hosts", len(hosts))
				}
			}
		})

		b.Run("UsableHosts_WithProtocol_"+test.name, func(b *testing.B) {
			for b.Loop() {
				hosts, err := store.UsableHosts(0, test.limit, hosts.WithProtocol(randomProtocol()))
				if err != nil {
					b.Fatal(err)
				} else if len(hosts) < test.limit/2 {
					b.Fatalf("sanity check failed, found %d usable hosts", len(hosts))
				}
			}
		})

		b.Run("UsableHosts_WithCountry_"+test.name, func(b *testing.B) {
			for b.Loop() {
				hosts, err := store.UsableHosts(0, test.limit, hosts.WithCountry(randomCountry()))
				if err != nil {
					b.Fatal(err)
				} else if len(hosts) < test.limit/10 {
					b.Fatalf("sanity check failed, found %d usable hosts", len(hosts))
				}
			}
		})

		b.Run("UsableHosts_WithProximity_"+test.name, func(b *testing.B) {
			for b.Loop() {
				point := randomLocation()
				hosts, err := store.UsableHosts(0, test.limit, hosts.SortByDistance(point.P.X, point.P.Y))
				if err != nil {
					b.Fatal(err)
				} else if len(hosts) != test.limit {
					b.Fatalf("sanity check failed, found %d usable hosts", len(hosts))
				}
			}
		})
	}
}

func newTestHostSettings(pk types.PublicKey) proto4.HostSettings {
	return proto4.HostSettings{
		Release:             "test",
		ProtocolVersion:     rhp.ProtocolVersion400,
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

func TestHostsForFunding(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	store := initPostgres(t, log.Named("postgres"))

	// assertNumHostsForFunding asserts that the number of hosts returned by
	// HostsForFunding matches the expected number, and returns the host keys.
	assertNumHostsForFunding := func(expected int) []types.PublicKey {
		t.Helper()
		hks, err := store.HostsForFunding()
		if err != nil {
			t.Fatal(err)
		} else if len(hks) != expected {
			t.Fatalf("expected %d hosts, got %d", expected, len(hks))
		}
		return hks
	}

	// updateContractGood updates the 'good' field of the given contract.
	updateContractGood := func(fcid types.FileContractID, good bool) {
		t.Helper()
		if _, err := store.pool.Exec(t.Context(), `UPDATE contracts SET good = $1 WHERE contract_id = $2`, good, sqlHash256(fcid)); err != nil {
			t.Fatal(err)
		}
	}

	// updateContractState updates the state of the given contract.
	updateContractState := func(fcid types.FileContractID, state contracts.ContractState) {
		t.Helper()
		if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
			return tx.UpdateContractState(fcid, state)
		}); err != nil {
			t.Fatal(err)
		}
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// assert no hosts are returned, they don't have active contracts
	assertNumHostsForFunding(0)

	// add contract for both hosts
	fcid1 := store.addTestContract(t, hk1, types.FileContractID(hk1))
	store.addTestContract(t, hk2, types.FileContractID(hk2))

	// assert both hosts are returned now
	assertNumHostsForFunding(2)

	// block h2
	if err := store.BlockHosts([]types.PublicKey{hk2}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	}

	// assert only h1 is returned
	hks := assertNumHostsForFunding(1)
	if hks[0] != hk1 {
		t.Fatalf("expected host %v, got %v", hk1, hks[0])
	}

	// assert state is taken into account
	updateContractState(fcid1, contracts.ContractStateResolved)
	assertNumHostsForFunding(0)

	// reset
	updateContractState(fcid1, contracts.ContractStateActive)
	assertNumHostsForFunding(1)

	// assert good is taken into account
	updateContractGood(fcid1, false)
	assertNumHostsForFunding(0)
	updateContractGood(fcid1, true)
	assertNumHostsForFunding(1)

	// assert renewed_to is taken into account
	fcid3 := types.FileContractID{3}
	if err := store.AddRenewedContract(fcid1, fcid3, newTestRevision(hk1), types.ZeroCurrency, types.ZeroCurrency, proto4.Usage{}); err != nil {
		t.Fatal(err)
	}
	updateContractGood(fcid3, false)
	assertNumHostsForFunding(0)
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
	db.addTestAccount(t, types.PublicKey(acc))

	// add contract to hosts so can satisfy PinSlab rules on hosts
	db.addTestContract(t, hk1, frand.Entropy256())
	db.addTestContract(t, hk2, frand.Entropy256())

	// pin a slab with sector on both hosts
	r1 := frand.Entropy256()
	if _, err := db.PinSlabs(acc, time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
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

	// remove contracts to avoid interfering with rest of test
	if _, err := db.pool.Exec(t.Context(), `DELETE FROM contract_sectors_map; DELETE FROM contracts;`); err != nil {
		t.Fatal(err)
	}

	// assert no hosts are returned, they don't have active contracts
	hks, err := db.HostsForPinning()
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}

	// add contract for both hosts
	fcid1 := db.addTestContract(t, hk1)
	_ = db.addTestContract(t, hk2)

	// assert both hosts are returned now
	hks, err = db.HostsForPinning()
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hks))
	}

	// pin sectors for h1
	if err := db.PinSectors(fcid1, []types.Hash256{r1}); err != nil {
		t.Fatal(err)
	}

	// assert only h2 is returned
	hks, err = db.HostsForPinning()
	if err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 {
		t.Fatalf("expected one host, got %d", len(hks))
	} else if hks[0] != hk2 {
		t.Fatalf("expected host %v, got %v", hk2, hks[0])
	}

	// block host 2
	if err := db.BlockHosts([]types.PublicKey{hk2}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	}

	// assert no hosts are returned
	hks, err = db.HostsForPinning()
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
	db.addTestAccount(t, types.PublicKey(acc))

	// add contract for both hosts
	fcid1 := db.addTestContract(t, hk1)
	fcid2 := db.addTestContract(t, hk2)

	// assert there's no hosts for pruning yet
	if hks, err := db.HostsForPruning(); err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}

	// update next prune so h2 should be returned for pruning
	if _, err := db.pool.Exec(t.Context(), `UPDATE contracts SET next_prune = NOW() - INTERVAL '1 second' WHERE contract_id = $1`, sqlHash256(fcid2)); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(); err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 || hks[0] != hk2 {
		t.Fatalf("expected h2 for pruning, got %v", hks)
	}

	// block host 2 and assert it is not returned anymore
	if err := db.BlockHosts([]types.PublicKey{hk2}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(); err != nil {
		t.Fatal(err)
	} else if len(hks) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hks))
	}

	// update next prune so h1 should be returned for pruning
	if _, err := db.pool.Exec(t.Context(), `UPDATE contracts SET next_prune = NOW() - INTERVAL '1 second' WHERE contract_id = $1`, sqlHash256(fcid1)); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(); err != nil {
		t.Fatal(err)
	} else if len(hks) != 1 || hks[0] != hk1 {
		t.Fatalf("expected h1 for pruning, got %v", hks)
	}

	// update good status so h1 is not returned anymore
	if _, err := db.pool.Exec(t.Context(), `UPDATE contracts SET good = FALSE WHERE contract_id = $1`, sqlHash256(fcid1)); err != nil {
		t.Fatal(err)
	} else if hks, err := db.HostsForPruning(); err != nil {
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
	store.addTestAccount(b, types.PublicKey(account))

	const (
		nHosts            = 1000
		nContractsPerHost = 100
	)

	// prepare database
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for range nHosts {
			// add host
			hk := types.GeneratePrivateKey().PublicKey()
			var hostID int64
			err := tx.QueryRow(ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}

			// add contracts
			for range nContractsPerHost {
				insertRandomContract(b, tx, hostID, hk)
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	if err := errors.Join(vErr1, vErr2); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		batch, err := store.HostsForPruning()
		if err != nil {
			b.Fatal(err)
		} else if len(batch) < nHosts/2 {
			b.Fatal("unexpected number of hosts for pruning:", len(batch))
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
	db.addTestContract(t, hk1)
	db.addTestContract(t, hk2)

	// add account
	acc := proto4.Account{1}
	db.addTestAccount(t, types.PublicKey(acc))

	// helper to pin sector with a given checkTime
	pinSector := func(hk types.PublicKey, root types.Hash256, nextCheck time.Time) {
		t.Helper()
		_, err := db.PinSlabs(acc, nextCheck, slabs.SlabPinParams{
			EncryptionKey: [32]byte{},
			MinShards:     1,
			Sectors: []slabs.PinnedSector{
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

	hosts, err := db.HostsForIntegrityChecks(oneHFromNow, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hosts))
	} else if hosts[0] != hk1 || hosts[1] != hk2 {
		t.Fatalf("expected hosts %v, got %v", []types.PublicKey{hk1, hk2}, hosts)
	}

	// apply limit
	hosts, err = db.HostsForIntegrityChecks(oneHFromNow, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	} else if hosts[0] != hk1 {
		t.Fatalf("expected host %v, got %v", hk1, hosts[0])
	}

	// using a maxLastCheck time in the past should cause no hosts to be returned
	hosts, err = db.HostsForIntegrityChecks(now, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hosts))
	}

	// unpinning the sector on host 2 which is up for a check should cause host
	// 2 to not be returned anymore
	if err := db.MarkSectorsLost(hk2, []types.Hash256{root2}); err != nil {
		t.Fatal(err)
	}

	hosts, err = db.HostsForIntegrityChecks(oneHFromNow, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	} else if hosts[0] != hk1 {
		t.Fatalf("expected host %v, got %v", hk1, hosts[0])
	}

	// block host 1 so that it's also not returned anymore
	if err := db.BlockHosts([]types.PublicKey{hk1}, []string{t.Name()}); err != nil {
		t.Fatal(err)
	}
	hosts, err = db.HostsForIntegrityChecks(oneHFromNow, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hosts))
	}
}

// BenchmarkHostsForFunding benchmarks HostsForFunding.
func BenchmarkHostsForFunding(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// prepare database
	const nHosts = 1000
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for range nHosts {
			hk := types.GeneratePrivateKey().PublicKey()

			// add host
			var hostID int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO hosts (public_key, last_announcement)
				VALUES ($1, NOW())
				RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID); err != nil {
				return err
			}

			// add contract
			if frand.Intn(10) < 8 {
				fcid := insertRandomContract(b, tx, hostID, hk)

				// make some contracts not good
				if frand.Intn(10) < 2 {
					_, err := tx.Exec(ctx, `UPDATE contracts SET good = FALSE WHERE contract_id = $1`, sqlHash256(fcid))
					if err != nil {
						return err
					}
				}
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	if err := errors.Join(vErr1, vErr2); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		hosts, err := store.HostsForFunding()
		if err != nil {
			b.Fatal(err)
		} else if len(hosts) == 0 {
			b.Fatal("expected some hosts, got none")
		}
	}
}

// BenchmarkHostsForPinning benchmarks HostsForPinning.
func BenchmarkHostsForPinning(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// add account
	account := proto4.Account{1}
	store.addTestAccount(b, types.PublicKey(account))

	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors

		nHosts            = 1000
		nContractsPerHost = 100
		nSectorsPerHost   = dbBaseSize / proto4.SectorSize / nHosts
	)

	// prepare database
	hostToContractIDs := make(map[types.PublicKey][]types.FileContractID, nHosts)
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for range nHosts {
			hk := types.GeneratePrivateKey().PublicKey()

			// add host
			var hostID int64
			err := tx.QueryRow(ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}

			// add contracts
			for range nContractsPerHost {
				hostToContractIDs[hk] = append(hostToContractIDs[hk], insertRandomContract(b, tx, hostID, hk))
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// add sectors
	for hk, fcids := range hostToContractIDs {
		var idx int
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			fcid := fcids[idx%len(fcids)]
			idx++

			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.PinnedSector
			var roots []types.Hash256
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.PinnedSector{
					Root:    root,
					HostKey: hk,
				})
				roots = append(roots, root)
			}
			if _, err := store.PinSlabs(account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}

			// pin 10% of the sectors
			frand.Shuffle(len(roots), func(i, j int) { roots[i], roots[j] = roots[j], roots[i] })
			if err := store.PinSectors(fcid, roots[:len(roots)/10]); err != nil {
				b.Fatal(err, fcid)
			}
		}
	}

	for b.Loop() {
		batch, err := store.HostsForPinning()
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

	store.addTestAccount(b, types.PublicKey(account))

	const (
		nHosts          = 10000
		dbBaseSize      = 1 << 40 // 1TiB of sectors
		nSectorsPerHost = dbBaseSize / proto4.SectorSize / nHosts
	)

	// add hosts
	for range nHosts {
		hk := store.addTestHost(b)
		store.addTestContract(b, hk)

		// add sectors
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.PinnedSector
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.PinnedSector{
					Root:    root,
					HostKey: hk,
				})
			}
			if _, err := store.PinSlabs(account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	// 10% of the sectors have a next check time in the past
	_, err := store.pool.Exec(b.Context(), `UPDATE sectors SET next_integrity_check = NOW() - INTERVAL '1 hour' WHERE id % 10 = 0`)
	if err != nil {
		b.Fatal(err)
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	_, vErr3 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) sectors;`)
	if err := errors.Join(vErr1, vErr2, vErr3); err != nil {
		b.Fatal(err)
	}

	// run benchmark for various batch sizes
	futureTime := time.Now().Add(time.Hour)
	for _, batchSize := range []int{50, 100, 200} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			for b.Loop() {
				batch, err := store.HostsForIntegrityChecks(futureTime, batchSize)
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

	store.addTestAccount(b, types.PublicKey(account))

	const (
		nHosts          = 10000
		dbBaseSize      = 1 << 40 // 1TiB of sectors
		nSectorsPerHost = dbBaseSize / proto4.SectorSize / nHosts
	)

	// add hosts
	for range nHosts {
		hk := store.addTestHost(b)
		store.addTestContract(b, hk)

		// add sectors
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.PinnedSector
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.PinnedSector{
					Root:    root,
					HostKey: hk,
				})
			}
			if _, err := store.PinSlabs(account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	// 10% of hosts have lost sectors
	_, err := store.pool.Exec(b.Context(), `UPDATE hosts SET lost_sectors = floor(random() * 100) + 1 WHERE id % 10 = 0`)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		batch, err := store.HostsWithLostSectors()
		if err != nil {
			b.Fatal(err)
		} else if len(batch) == 0 {
			b.Fatal("expected hosts, got none")
		}
	}
}

func BenchmarkHostsWithUnpinnableSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto4.Account{1}

	store.addTestAccount(b, types.PublicKey(account))

	const (
		nHosts          = 1000
		dbBaseSize      = 1 << 40 // 1TiB of sectors
		nSectorsPerHost = dbBaseSize / proto4.SectorSize / nHosts
	)

	// add hosts
	for i := range nHosts {
		hk := store.addTestHost(b)
		store.addTestContract(b, hk)

		// add sectors
		for remainingSectors := nSectorsPerHost; remainingSectors > 0; {
			batchSize := min(remainingSectors, 10000)
			remainingSectors -= batchSize
			var sectors []slabs.PinnedSector
			for range batchSize {
				root := frand.Entropy256()
				sectors = append(sectors, slabs.PinnedSector{
					Root:    root,
					HostKey: hk,
				})
			}
			if _, err := store.PinSlabs(account, time.Now().Add(time.Hour), slabs.SlabPinParams{
				MinShards:     1,
				EncryptionKey: frand.Entropy256(),
				Sectors:       sectors,
			}); err != nil {
				b.Fatal(err)
			}
		}

		// 10% of hosts have unpinned sectors which results in 100 out of 1000.
		if i%10 == 0 {
			if res, err := store.pool.Exec(b.Context(), `UPDATE contracts SET state = $1 WHERE contract_id = $2`, sqlContractState(contracts.ContractStateResolved), sqlHash256(types.FileContractID(hk))); err != nil {
				b.Fatal(err)
			} else if res.RowsAffected() != 1 {
				b.Fatal("expected to update 1 contract")
			}
		}
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	_, vErr3 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) sectors;`)
	if err := errors.Join(vErr1, vErr2, vErr3); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		batch, err := store.HostsWithUnpinnableSectors()
		if err != nil {
			b.Fatal(err)
		} else if len(batch) != 100 {
			b.Fatal("expected 100 hosts, got", len(batch))
		}
	}
}

// BenchmarkHostStats benchmarks the HostStats method.
func BenchmarkHostStats(b *testing.B) {
	const (
		numHosts            = 10_000
		numContractsPerHost = 10
		limit               = 500
	)

	store := initPostgres(b, zap.NewNop())

	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for i := range numHosts {
			hk := types.GeneratePrivateKey().PublicKey()

			accountUsage := types.NewCurrency64(frand.Uint64n(1e6) + 1)
			totalUsage := accountUsage.Add(types.NewCurrency64(frand.Uint64n(1e6) + 1))

			var hostID int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO hosts (public_key, lost_sectors, usage_account_funding, usage_total_spent, last_announcement)
				VALUES ($1, $2, $3, $4, NOW())
				RETURNING id
			`,
				sqlPublicKey(hk),
				frand.Uint64n(1e3),
				sqlCurrency(accountUsage),
				sqlCurrency(totalUsage),
			).Scan(&hostID); err != nil {
				return err
			}

			for range numContractsPerHost {
				insertRandomContract(b, tx, hostID, hk)
			}

			if i%7 == 0 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO hosts_blocklist (public_key, reasons)
					VALUES ($1, $2)
					ON CONFLICT (public_key) DO UPDATE SET reasons = EXCLUDED.reasons
				`, sqlPublicKey(hk), []string{"benchmark", "reason"}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	if err := errors.Join(vErr1, vErr2); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		offset := frand.Intn(numHosts - limit)
		stats, err := store.HostStats(offset, limit)
		if err != nil {
			b.Fatal(err)
		} else if len(stats) != limit {
			b.Fatalf("expected %d host stats, got %d", limit, len(stats))
		}
	}
}

func BenchmarkStuckHosts(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	const (
		numHosts = 10_000
	)

	// prepare database
	var allHosts []types.PublicKey
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		for i := range numHosts {
			hk := types.GeneratePrivateKey().PublicKey()
			allHosts = append(allHosts, hk)

			// add host
			var hostID int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO hosts (public_key, last_announcement)
				VALUES ($1, NOW())
				RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID); err != nil {
				return err
			}

			// 10% of hosts are stuck (stuck_since > 24 hours ago)
			if i%10 == 0 {
				_, err := tx.Exec(ctx, `UPDATE hosts SET stuck_since = NOW() - INTERVAL '48 hours' WHERE id = $1`, hostID)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) hosts;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contracts;`)
	if err := errors.Join(vErr1, vErr2); err != nil {
		b.Fatal(err)
	}

	b.Run("StuckHosts", func(b *testing.B) {
		for b.Loop() {
			hosts, err := store.StuckHosts()
			if err != nil {
				b.Fatal(err)
			} else if len(hosts) == 0 {
				b.Fatal("expected some hosts")
			}
		}
	})

	b.Run("UpdateStuckHosts", func(b *testing.B) {
		// use 10% of hosts for stuck/unstuck
		stuckHosts := allHosts[:numHosts/10]
		unstuckHosts := allHosts[numHosts/10 : numHosts/5]
		for b.Loop() {
			if err := store.UpdateStuckHosts(stuckHosts, unstuckHosts); err != nil {
				b.Fatal(err)
			}
		}
	})
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
	if err := s.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{ha}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	return hk
}
