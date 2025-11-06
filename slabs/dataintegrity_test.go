package slabs

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
	"go.uber.org/zap"
)

func TestPerformIntegrityChecksForHost(t *testing.T) {
	oneSC := types.Siacoins(1)

	// prepare host
	hk := types.PublicKey{1}
	host := hosts.Host{
		PublicKey: hk,
		Settings: proto.HostSettings{
			Prices: proto.HostPrices{
				EgressPrice: types.Siacoins(1).Div64(proto.SectorSize), // 1SC per sector
				ValidUntil:  time.Now().Add(time.Hour),
			},
		},
	}
	dialer := newMockDialer([]hosts.Host{host})

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
	sm, err := newSlabManager(chain, am, cm, hm, store, dialer, nil, sk, sk)
	if err != nil {
		t.Fatal(err)
	}

	// prepare helper to reset balance to 3SC to avoid running out of funds
	resetBalance := func() {
		t.Helper()
		err = am.UpdateServiceAccountBalance(context.Background(), hk, acc, oneSC.Mul64(3))
		if err != nil {
			t.Fatal(err)
		}
	}

	// prepare roots
	r1 := types.Hash256{1} // good
	r2 := types.Hash256{2} // lost
	r3 := types.Hash256{3} // bad
	dialer.clients[host.PublicKey].integrity[r1] = nil
	dialer.clients[host.PublicKey].integrity[r2] = proto.ErrSectorNotFound
	dialer.clients[host.PublicKey].integrity[r3] = proto.ErrNotEnoughFunds
	store.sectorsForCheck = []types.Hash256{r1, r2, r3}

	// perform the checks once
	resetBalance()
	sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())

	// the lost sector should be marked as lost
	if len(store.lostSectors[host.PublicKey]) != 1 {
		t.Fatalf("expected 1 lost sector, got %d", len(store.lostSectors))
	} else if _, exists := store.lostSectors[host.PublicKey][r2]; !exists {
		t.Fatalf("expected lost sector %v, got %v", r2, store.lostSectors)
	}

	// the failed sector should be marked as failed
	if len(store.failedChecks) != 1 {
		t.Fatalf("expected 1 failed checks, got %d", len(store.failedChecks))
	} else if n, exists := store.failedChecks[host.PublicKey][r3]; !exists || n != 1 {
		t.Fatalf("expected failed check %v, got %v", r3, store.failedChecks[host.PublicKey])
	}

	// perform the checks a few more time to reach the maximum number of failed
	// checks before a bad sector gets removed
	for i := uint(1); i < sm.maxFailedIntegrityChecks; i++ {
		resetBalance()
		sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	}

	// the failed sector should be marked as failed 5 times
	if len(store.failedChecks) != 1 {
		t.Fatalf("expected 1 failed checks, got %d", len(store.failedChecks))
	} else if n, exists := store.failedChecks[host.PublicKey][r3]; !exists || n != 5 {
		t.Fatalf("expected failed check %v, got %v", r3, store.failedChecks[host.PublicKey])
	}

	// should have 2 lost sectors now
	if len(store.lostSectors[host.PublicKey]) != 2 {
		t.Fatalf("expected 2 lost sectors, got %d", len(store.lostSectors))
	} else if _, exists := store.lostSectors[host.PublicKey][r2]; !exists {
		t.Fatalf("expected lost sector %v, got %v", r2, store.lostSectors)
	} else if _, exists := store.lostSectors[host.PublicKey][r3]; !exists {
		t.Fatalf("expected lost sector %v, got %v", r3, store.lostSectors)
	}

	// empty the service account to trigger a "not enough funds" error which
	// causes triggering a refill.
	if cm.triggeredRefills[acc] != 0 {
		t.Fatalf("expected 0 triggered refill, got %d", cm.triggeredRefills[acc])
	}
	_ = am.UpdateServiceAccountBalance(context.Background(), hk, acc, types.ZeroCurrency)
	sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())
	if cm.triggeredRefills[acc] != 1 {
		t.Fatalf("expected 1 triggered refill, got %d", cm.triggeredRefills[acc])
	}
}

func TestPerformIntegrityChecksForHostExpiredPrices(t *testing.T) {
	// prepare host
	hk := types.PublicKey{1}
	host := hosts.Host{
		PublicKey: hk,
		Settings: proto.HostSettings{
			Prices: proto.HostPrices{
				EgressPrice: types.Siacoins(1).Div64(proto.SectorSize), // 1SC per sector
				ValidUntil:  time.Time{},                               // prices are initially expired
			},
		},
	}
	dialer := newMockDialer([]hosts.Host{host})

	// prepare managers
	store := newMockStore()
	chain := newMockChainManager()
	am := newMockAccountManager(store)
	cm := newMockContractManager()
	hm := newMockHostManager()
	hm.refreshPrices = true // refresh prices after first scan
	host.Usability = hosts.GoodUsability
	hm.hosts[host.PublicKey] = host // make host scannable to get valid prices

	// prepare account
	sk := types.GeneratePrivateKey()
	acc := proto.Account(sk.PublicKey())

	// prepare slab manager
	sm, err := newSlabManager(chain, am, cm, hm, store, dialer, nil, sk, sk)
	if err != nil {
		t.Fatal(err)
	}

	// prepare 2 roots for checking
	r1 := types.Hash256{1}
	r2 := types.Hash256{2}
	dialer.clients[host.PublicKey].integrity[r1] = nil
	dialer.clients[host.PublicKey].integrity[r2] = nil
	store.sectorsForCheck = []types.Hash256{r1, r2}

	// fund service account
	err = am.UpdateServiceAccountBalance(context.Background(), hk, acc, types.MaxCurrency)
	if err != nil {
		t.Fatal(err)
	}

	// perform the checks once
	sm.performIntegrityChecksForHost(context.Background(), host.PublicKey, zap.NewNop())

	// no checks should have failed
	if store.failedChecks[host.PublicKey][r1] != 0 || store.failedChecks[host.PublicKey][r2] != 0 {
		t.Fatal("expected 0 failed checks")
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
