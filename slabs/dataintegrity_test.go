package slabs_test

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

func TestPerformIntegrityChecksForHost(t *testing.T) {
	oneSC := types.Siacoins(1)

	// prepare host
	client := newMockHostClient()
	hostKey := types.GeneratePrivateKey()
	host := client.addTestHost(hostKey)

	// prepare managers
	store := newMockStore(t)
	chain := newMockChainManager()
	am := newMockAccountManager()
	cm := newMockContractManager()
	hm := newMockHostManager()
	host.Usability = hosts.GoodUsability
	hm.hosts[host.PublicKey] = host // make host scannable

	// prepare account
	sk := types.GeneratePrivateKey()
	acc := proto.Account(sk.PublicKey())

	// prepare slab manager
	sm := slabs.NewSlabManager(chain, am, cm, hm, store, client, nil, sk, sk, slabs.WithIntegrityCheckIntervals(time.Millisecond, time.Millisecond))

	// prepare helper to reset balance to 3SC to avoid running out of funds
	resetBalance := func() {
		t.Helper()
		err := am.UpdateServiceAccountBalance(context.Background(), hostKey.PublicKey(), acc, oneSC.Mul64(3))
		if err != nil {
			t.Fatal(err)
		}
	}

	// prepare sectors
	roots := make([]types.Hash256, 3)
	for i := range roots {
		root, err := client.WriteSector(context.Background(), types.GeneratePrivateKey(), host.PublicKey, []byte{byte(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		roots[i] = root.Root
	}
	store.setSectorsForCheck(t, host.PublicKey, roots)

	assertLostAndFailed := func(failed, lost []types.Hash256) {
		t.Helper()

		lostSectors := store.lostSectors(t)
		lostMap := make(map[types.Hash256]struct{})
		for _, r := range lost {
			lostMap[r] = struct{}{}
		}
		failedMap := make(map[types.Hash256]struct{})
		for _, r := range failed {
			failedMap[r] = struct{}{}
		}
		if len(lostSectors) != len(lost) {
			t.Errorf("expected %d lost sectors, got %d", len(lost), len(lostSectors))
		}
		for r := range lostSectors {
			if _, exists := lostMap[r]; !exists {
				t.Fatalf("unexpected lost sector %v", r)
			}
			delete(lostMap, r)
		}
		failedChecks := store.failedChecks(t, host.PublicKey)
		if len(failedChecks) != len(failed) {
			t.Fatalf("expected %d failed checks, got %d", len(failed), len(failedChecks))
		}
		for r, n := range failedChecks {
			if _, exists := failedMap[r]; n == 0 && exists {
				t.Fatalf("unexpected successful check for %v", r)
			} else if !exists && n > 0 {
				t.Fatalf("unexpected failed check for %v", r)
			}
			delete(failedMap, r)
		}
	}

	// perform the checks once
	resetBalance()
	client.integrityErrors[roots[1]] = proto.ErrSectorNotFound // simulate lost sector
	client.integrityErrors[roots[2]] = proto.ErrNotEnoughFunds // simulate bad sector
	sm.PerformIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	assertLostAndFailed(roots[2:3], roots[1:2])

	// perform the checks a few more time to reach the maximum number of failed
	// checks before a bad sector gets removed
	for i := uint(1); i < sm.MaxFailedIntegrityChecks(); i++ {
		resetBalance()
		sm.PerformIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	}
	assertLostAndFailed(nil, roots[1:3])

	// empty the service account to trigger a "not enough funds" error which
	// causes triggering a refill.
	if cm.triggeredRefills[acc] != 0 {
		t.Fatalf("expected 0 triggered refill, got %d", cm.triggeredRefills[acc])
	}
	if err := am.UpdateServiceAccountBalance(context.Background(), hostKey.PublicKey(), acc, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}
	sm.PerformIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	if cm.triggeredRefills[acc] != 1 {
		t.Fatalf("expected 1 triggered refill, got %d", cm.triggeredRefills[acc])
	}
}

func TestIntegrityChecksAlert(t *testing.T) {
	store := newMockStore(t)
	alerter := alerts.NewManager()
	sm := slabs.NewSlabManager(newMockChainManager(), newMockAccountManager(), nil, nil, store, nil, alerter, types.GeneratePrivateKey(), types.GeneratePrivateKey())

	// assert there are no alerts
	if alerts, err := alerter.Alerts(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}

	// mock a lost sector
	hk := types.PublicKey{1}
	store.AddTestHost(t, hosts.Host{PublicKey: hk})
	_, err := store.Exec(context.Background(), "UPDATE hosts SET lost_sectors = 1 WHERE public_key = $1", hk[:])
	if err != nil {
		t.Fatal(err)
	}

	// perform integrity checks
	if err := sm.PerformIntegrityChecks(context.Background()); err != nil {
		t.Fatal(err)
	}

	// assert alert was generated
	var got alerts.Alert
	if alerts, err := alerter.Alerts(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	} else {
		got = alerts[0]
	}

	expected := slabs.NewLostSectorsAlert([]types.PublicKey{hk})
	got.Timestamp = expected.Timestamp // ignore timestamp
	if !reflect.DeepEqual(expected, got) {
		t.Fatal("unexpected alert", expected, got)
	}

	// remove lostSectors
	_, err = store.Exec(context.Background(), "UPDATE hosts SET lost_sectors = 0 WHERE public_key = $1", hk[:])
	if err != nil {
		t.Fatal(err)
	}

	// perform integrity checks
	if err := sm.PerformIntegrityChecks(context.Background()); err != nil {
		t.Fatal(err)
	}

	// alert should be dismissed
	if alerts, err := alerter.Alerts(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}
}
