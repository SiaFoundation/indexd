package contracts_test

import (
	"context"
	"errors"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
)

func TestBlockBadHosts(t *testing.T) {
	store := newContractTestStore(t)
	hmMock := newHostManagerMock(store)
	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, nil, store, nil, hmMock, nil, nil)

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

	// block the bad hosts
	if err := cm.BlockBadHosts(context.Background()); err != nil {
		t.Fatal(err)
	}

	// helper to assert a blocked host has a blocked contract and vice versa
	assertHostAndContract := func(hk types.PublicKey, blocked bool, expectReasons bool) {
		t.Helper()

		host, err := store.Host(hk)
		if err != nil {
			t.Fatal(err)
		} else if host.Blocked != blocked {
			t.Fatalf("expected host %v to be blocked=%v, got blocked=%v", hk, blocked, host.Blocked)
		} else if host.Blocked && expectReasons && len(host.BlockedReasons) == 0 {
			t.Fatalf("expected host %v to have block reasons, got none", hk)
		}
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
	assertHostAndContract(goodHost.PublicKey, false, false)

	// a bad host and its contract should be blocked
	assertHostAndContract(badHost.PublicKey, true, true)

	// an unused host shouldn't be blocked
	assertHostAndContract(unusedBadHost.PublicKey, false, false)
}
