package contracts_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
)

func TestBlockBadHosts(t *testing.T) {
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	goodHost := hosts.Host{PublicKey: types.PublicKey{1}, Usability: hosts.GoodUsability, Settings: goodSettings}
	badHost := hosts.Host{PublicKey: types.PublicKey{2}, Usability: hosts.Usability{}, Settings: goodSettings}
	unusedBadHost := hosts.Host{PublicKey: types.PublicKey{3}, Usability: hosts.Usability{}, Settings: goodSettings}

	// add hosts to the store
	store.addTestHost(t, goodHost)
	store.addTestHost(t, badHost)
	store.addTestHost(t, unusedBadHost)

	// add settings to host manager
	hmMock.settings[goodHost.PublicKey] = goodSettings
	hmMock.settings[badHost.PublicKey] = goodSettings
	hmMock.settings[unusedBadHost.PublicKey] = goodSettings

	// form a contract with each host except the unused one
	for _, host := range []hosts.Host{goodHost, badHost} {
		store.addTestContract(t, host.PublicKey, true, types.FileContractID(host.PublicKey))
	}

	// get expected reasons before blocking (usability is computed from settings in DB)
	storedBadHost, err := store.Host(badHost.PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	// block the bad hosts
	if err := cm.BlockBadHosts(context.Background()); err != nil {
		t.Fatal(err)
	}

	// helper to assert a blocked host has a blocked contract and vice versa
	assertHostAndContract := func(hk types.PublicKey, blocked bool, reasons []string) {
		t.Helper()

		assertHostBlocked(t, store, hk, blocked, reasons)
		contract, err := store.Contract(types.FileContractID(hk))
		if errors.Is(err, contracts.ErrNotFound) && hk == unusedBadHost.PublicKey {
			return // unused host doesn't have a contract
		} else if err != nil {
			t.Fatal(err)
		} else if contract.Good != !blocked {
			t.Fatalf("expected contract %v to be good=%v, got good=%v", types.FileContractID(hk), !blocked, contract.Good)
		}
	}

	// a good host shouldn't be blocked
	assertHostAndContract(goodHost.PublicKey, false, nil)

	// a bad host and its contract should be blocked
	assertHostAndContract(badHost.PublicKey, true, storedBadHost.Usability.FailedChecks())

	// an unused host shouldn't be blocked
	assertHostAndContract(unusedBadHost.PublicKey, false, nil)
}

func TestUnblockUsableHosts(t *testing.T) {
	store := newTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, store, nil, nil, nil, contracts.NewContractLocker(), hmMock, nil, nil)

	// usable again, no active contracts -> should be unblocked
	recovered := hosts.Host{PublicKey: types.PublicKey{1}, Usability: hosts.GoodUsability, Settings: goodSettings}
	// no contracts but still unusable -> stays blocked
	stillUnusable := hosts.Host{PublicKey: types.PublicKey{2}, Usability: hosts.Usability{}, Settings: goodSettings}
	// usable but still has an active contract -> stays blocked
	usableWithContract := hosts.Host{PublicKey: types.PublicKey{3}, Usability: hosts.GoodUsability, Settings: goodSettings}
	// usable, no contracts, but blocked for both a usability and a non-usability reason
	usabilityAndOther := hosts.Host{PublicKey: types.PublicKey{4}, Usability: hosts.GoodUsability, Settings: goodSettings}
	// usable, no contracts, but blocked for a non-usability reason only -> untouched
	otherReasonOnly := hosts.Host{PublicKey: types.PublicKey{5}, Usability: hosts.GoodUsability, Settings: goodSettings}

	for _, h := range []hosts.Host{recovered, stillUnusable, usableWithContract, usabilityAndOther, otherReasonOnly} {
		store.addTestHost(t, h)
		hmMock.settings[h.PublicKey] = goodSettings
	}
	fcid := store.addTestContract(t, usableWithContract.PublicKey, true, types.FileContractID(usableWithContract.PublicKey))

	// block hosts for a usability reason, as the maintenance loop would
	if err := store.BlockHosts([]types.PublicKey{recovered.PublicKey, stillUnusable.PublicKey, usableWithContract.PublicKey}, []string{"EgressPrice"}); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockHosts([]types.PublicKey{usabilityAndOther.PublicKey}, []string{"EgressPrice", "foo"}); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockHosts([]types.PublicKey{otherReasonOnly.PublicKey}, []string{"foo"}); err != nil {
		t.Fatal(err)
	}

	if err := cm.UnblockUsableHosts(context.Background()); err != nil {
		t.Fatal(err)
	}

	// recovered host is fully unblocked
	assertHostBlocked(t, store, recovered.PublicKey, false, nil)
	// still unusable host stays blocked (not usable)
	assertHostBlocked(t, store, stillUnusable.PublicKey, true, []string{"EgressPrice"})
	// usable host with an active contract stays blocked
	assertHostBlocked(t, store, usableWithContract.PublicKey, true, []string{"EgressPrice"})
	// non-usability reason is preserved, only the usability reason is removed
	assertHostBlocked(t, store, usabilityAndOther.PublicKey, true, []string{"foo"})
	// host blocked only for a non-usability reason is left untouched
	assertHostBlocked(t, store, otherReasonOnly.PublicKey, true, []string{"foo"})

	// the still-blocked contract must remain bad
	if c, err := store.Contract(fcid); err != nil {
		t.Fatal(err)
	} else if c.Good {
		t.Fatal("expected contract of still-blocked host to remain bad")
	}
}

// assertHostBlocked asserts a host's blocked state and, when blocked, its
// blocklist reasons.
func assertHostBlocked(t *testing.T, store testStore, hk types.PublicKey, blocked bool, reasons []string) {
	t.Helper()

	host, err := store.Host(hk)
	if err != nil {
		t.Fatal(err)
	} else if host.Blocked != blocked {
		t.Fatalf("expected host %v to be blocked=%v, got blocked=%v", hk, blocked, host.Blocked)
	} else if host.Blocked && !slices.Equal(slices.Sorted(slices.Values(host.BlockedReasons)), slices.Sorted(slices.Values(reasons))) {
		t.Fatalf("expected host %v to be blocked due to %v, got blocked due to %v", hk, reasons, host.BlockedReasons)
	}
}
