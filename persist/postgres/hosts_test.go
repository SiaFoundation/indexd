package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	_, err := db.rawHost(context.Background(), hk)
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
	h, err := db.rawHost(context.Background(), hk)
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
	h, err = db.rawHost(context.Background(), hk)
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

func TestUpdateHost(t *testing.T) {
	// create database
	log := zaptest.NewLogger(t)
	db := initPostgres(t, log.Named("postgres"))

	// assert [ErrHostNotFound] is returned
	hk := types.PublicKey{1}
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
	} else if h, err := db.rawHost(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.Settings != (proto4.HostSettings{}) {
		t.Fatal("expected no settings")
	}

	// assert consecutive failures get updated
	err = db.UpdateHost(context.Background(), hk, nil, hs, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.rawHost(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if h.consecutiveFailedScans != 2 {
		t.Fatal("unexpected", h.consecutiveFailedScans)
	}

	now := time.Now().Round(time.Minute)
	nextScan := now.Add(time.Hour)
	networks := []string{"192.168.1.0/24"}

	// assert host is properly updated on successful scan
	err = db.UpdateHost(context.Background(), hk, networks, hs, true, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.rawHost(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(h.Settings, hs) {
		t.Fatal("expected settings to match")
	} else if h.totalScans != 3 {
		t.Fatal("unexpected", h.totalScans)
	} else if !h.nextScan.Equal(nextScan) {
		t.Fatal("unexpected next scan", h.nextScan)
	} else if h.failedScans != 2 {
		t.Fatal("unexpected failed scans", h.failedScans)
	} else if h.consecutiveFailedScans != 0 {
		t.Fatal("unexpected consecutive failed scans", h.consecutiveFailedScans)
	} else if len(h.networks) != 1 {
		t.Fatal("unexpected networks", h.networks)
	} else if h.networks[0] != networks[0] {
		t.Fatal("unexpected network", h.networks[0])
	}

	// assert networks are overwritten
	networks = []string{"2001:db8::/32"}
	err = db.UpdateHost(context.Background(), hk, networks, hs, true, nextScan)
	if err != nil {
		t.Fatal(err)
	} else if h, err := db.rawHost(context.Background(), hk); err != nil {
		t.Fatal(err)
	} else if len(h.networks) != 1 {
		t.Fatal("unexpected networks", h.networks)
	} else if h.networks[0] != networks[0] {
		t.Fatal("unexpected network", h.networks[0])
	}
}

// rawHost returns the host for given public key
func (s *Store) rawHost(ctx context.Context, hk types.PublicKey) (dbHost, error) {
	var h dbHost
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `SELECT id, public_key, last_announcement, total_scans, failed_scans, consecutive_failed_scans, next_scan FROM hosts WHERE public_key = $1`, sqlPublicKey(hk)).Scan(
			&h.id,
			(*sqlPublicKey)(&h.PublicKey),
			&h.LastAnnouncement,
			&h.totalScans,
			&h.failedScans,
			&h.consecutiveFailedScans,
			&h.nextScan,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hk, ErrHostNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to query host: %w", err)
		}
		h.Addresses, err = queryHostAddresses(ctx, tx, h.id)
		if err != nil {
			return err
		}
		h.Settings, err = queryHostSettings(ctx, tx, h.id)
		if err != nil {
			return err
		}

		h.networks, err = queryHostNetworks(ctx, tx, h.id)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return dbHost{}, err
	}
	return h, nil
}

func queryHostNetworks(ctx context.Context, tx *txn, hostID int64) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT cidr FROM host_resolved_cidrs WHERE host_id = $1`, hostID)
	if err != nil {
		return nil, fmt.Errorf("failed to query host resolved CIDRs: %w", err)
	}
	defer rows.Close()

	var networks []string
	for rows.Next() {
		var cidr net.IPNet
		if err := rows.Scan(&cidr); err != nil {
			return nil, fmt.Errorf("failed to scan host address: %w", err)
		}
		networks = append(networks, cidr.String())
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return networks, nil
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
