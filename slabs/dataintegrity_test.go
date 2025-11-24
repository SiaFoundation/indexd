package slabs

import (
	"context"
	"math"
	"reflect"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/alerts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func TestPerformIntegrityChecksForHost(t *testing.T) {
	oneSC := types.Siacoins(1)

	// prepare host
	client := newMockHostClient()
	hostKey := types.GeneratePrivateKey()
	host := client.addTestHost(hostKey)

	// prepare managers
	store := newMockStore()
	chain := newMockChainManager()
	am := newMockAccountManager(store)
	cm := newMockContractManager()
	hm := newMockHostManager()
	host.Usability = hosts.GoodUsability
	hm.hosts[host.PublicKey] = host // make host scannable

	// prepare account
	sk := types.GeneratePrivateKey()
	acc := proto.Account(sk.PublicKey())

	// prepare slab manager
	sm, err := newSlabManager(chain, am, cm, hm, store, client, nil, sk, sk)
	if err != nil {
		t.Fatal(err)
	}

	// prepare helper to reset balance to 3SC to avoid running out of funds
	resetBalance := func() {
		t.Helper()
		err = am.UpdateServiceAccountBalance(context.Background(), hostKey.PublicKey(), acc, oneSC.Mul64(3))
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
	store.sectorsForCheck = roots

	assertLostAndFailed := func(failed, lost []types.Hash256) {
		t.Helper()

		lostMap := make(map[types.Hash256]struct{})
		for _, r := range lost {
			lostMap[r] = struct{}{}
		}
		failedMap := make(map[types.Hash256]struct{})
		for _, r := range failed {
			failedMap[r] = struct{}{}
		}
		if len(store.lostSectors[host.PublicKey]) != len(lost) {
			t.Fatalf("expected %d lost sectors, got %d", len(lost), len(store.lostSectors[host.PublicKey]))
		}
		for r := range store.lostSectors[host.PublicKey] {
			if _, exists := lostMap[r]; !exists {
				t.Fatalf("unexpected lost sector %v", r)
			}
			delete(lostMap, r)
		}
		if len(store.failedChecks[host.PublicKey]) != len(failed) {
			t.Fatalf("expected %d failed checks, got %d", len(failed), len(store.failedChecks[host.PublicKey]))
		}
		for r, n := range store.failedChecks[host.PublicKey] {
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
	sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	assertLostAndFailed(roots[1:3], roots[1:2])

	// perform the checks a few more time to reach the maximum number of failed
	// checks before a bad sector gets removed
	for i := uint(1); i < sm.maxFailedIntegrityChecks; i++ {
		resetBalance()
		sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	}
	assertLostAndFailed(roots[1:3], roots[1:3])

	// empty the service account to trigger a "not enough funds" error which
	// causes triggering a refill.
	if cm.triggeredRefills[acc] != 0 {
		t.Fatalf("expected 0 triggered refill, got %d", cm.triggeredRefills[acc])
	}
	_ = am.UpdateServiceAccountBalance(context.Background(), hostKey.PublicKey(), acc, types.ZeroCurrency)
	sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	if cm.triggeredRefills[acc] != 1 {
		t.Fatalf("expected 1 triggered refill, got %d", cm.triggeredRefills[acc])
	}
}

func TestIntegrityChecksAlert(t *testing.T) {
	store := newMockStore()
	alerter := alerts.NewManager()
	sm, err := newSlabManager(newMockChainManager(), newMockAccountManager(store), nil, nil, store, nil, alerter, types.GeneratePrivateKey(), types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert there are no alerts
	if alerts, err := alerter.Alerts(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}

	// mock a lost sector
	hk := types.PublicKey{1}
	store.hosts[hk] = hosts.Host{PublicKey: hk}
	store.lostSectors[hk] = make(map[types.Hash256]struct{})
	store.lostSectors[hk][types.Hash256{1}] = struct{}{}

	// perform integrity checks
	sm.performIntegrityChecks(context.Background())

	// assert alert was generated
	var got alerts.Alert
	if alerts, err := alerter.Alerts(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	} else {
		got = alerts[0]
	}

	expected := newLostSectorsAlert([]types.PublicKey{hk})
	got.Timestamp = expected.Timestamp // ignore timestamp
	if !reflect.DeepEqual(expected, got) {
		t.Fatal("unexpected alert", expected, got)
	}

	// remove lostSectors
	delete(store.lostSectors, hk)

	// perform integrity checks
	sm.performIntegrityChecks(context.Background())

	// alert should be dismissed
	if alerts, err := alerter.Alerts(0, math.MaxInt64); err != nil {
		t.Fatal(err)
	} else if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}
}
