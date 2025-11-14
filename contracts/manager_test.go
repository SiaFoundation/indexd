package contracts

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

func TestBlockBadHosts(t *testing.T) {
	store := &storeMock{}
	hmMock := &hostManagerMock{store: store, settings: make(map[types.PublicKey]rhp.HostSettings)}
	contracts := newContractManager(types.PublicKey{}, nil, nil, store, nil, hmMock, nil, nil)

	goodHost := hosts.Host{PublicKey: types.PublicKey{1}, Usability: hosts.GoodUsability, Blocked: false}
	badHost := hosts.Host{PublicKey: types.PublicKey{2}, Usability: hosts.Usability{}, Blocked: false}
	unusedBadHost := hosts.Host{PublicKey: types.PublicKey{3}, Usability: hosts.Usability{}, Blocked: false}

	store.hosts = map[types.PublicKey]hosts.Host{
		goodHost.PublicKey:      goodHost,
		badHost.PublicKey:       badHost,
		unusedBadHost.PublicKey: unusedBadHost,
	}

	// form a contract with each host except the unused one
	for _, host := range []hosts.Host{goodHost, badHost} {
		store.addTestContract(t, host.PublicKey, true)
	}

	// block the bad hosts
	if err := contracts.blockBadHosts(context.Background()); err != nil {
		t.Fatal(err)
	}

	// helper to assert a blocked host has a blocked contract and vice versa
	assertHostAndContract := func(hk types.PublicKey, blocked bool, reasons []string) {
		t.Helper()

		host, err := store.Host(hk)
		if err != nil {
			t.Fatal(err)
		} else if host.Blocked != blocked {
			t.Fatalf("expected host %v to be blocked=%v, got blocked=%v", hk, blocked, host.Blocked)
		} else if host.Blocked && !reflect.DeepEqual(host.BlockedReasons, reasons) {
			t.Fatalf("expected host %v to be blocked due to %v, got blocked due to %v", hk, reasons, host.BlockedReasons)
		}
		contract, err := store.Contract(types.FileContractID(hk))
		if errors.Is(err, ErrNotFound) && hk == unusedBadHost.PublicKey {
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
	assertHostAndContract(badHost.PublicKey, true, badHost.Usability.FailedChecks())

	// an unused host shouldn't be blocked
	assertHostAndContract(unusedBadHost.PublicKey, false, nil)
}
