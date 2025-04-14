package contracts

import (
	"context"
	"errors"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

func TestBlockBadHosts(t *testing.T) {
	store := &storeMock{}
	contracts := newContractManager(types.PublicKey{}, nil, nil, nil, nil, store, nil, nil)

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
		err := store.AddFormedContract(context.Background(), types.FileContractID(host.PublicKey), host.PublicKey, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4))
		if err != nil {
			t.Fatal(err)
		}
	}

	// block the bad hosts
	if err := contracts.blockBadHosts(context.Background()); err != nil {
		t.Fatal(err)
	}

	// helper to assert a blocked host has a blocked contract and vice versa
	assertHostAndContract := func(hk types.PublicKey, blocked bool, reason string) {
		t.Helper()

		host, err := store.Host(context.Background(), hk)
		if err != nil {
			t.Fatal(err)
		} else if host.Blocked != blocked {
			t.Fatalf("expected host %v to be blocked=%v, got blocked=%v", hk, blocked, host.Blocked)
		} else if host.Blocked && host.BlockedReason != reason {
			t.Fatalf("expected host %v to be blocked due to %s, got blocked due to %v", hk, reason, host.BlockedReason)
		}
		contract, err := store.Contract(context.Background(), types.FileContractID(hk))
		if errors.Is(err, ErrNotFound) && hk == unusedBadHost.PublicKey {
			return // unused host doesn't have a contract
		} else if err != nil {
			t.Fatal(err)
		} else if contract.Good != !blocked {
			t.Fatalf("expected contract %v to be good=%v, got good=%v", types.FileContractID(hk), !blocked, contract.Good)
		}
	}

	// a good host shouldn't be blocked
	assertHostAndContract(goodHost.PublicKey, false, "")

	// a bad host and its contract should be blocked
	assertHostAndContract(badHost.PublicKey, true, blockingReasonUsability)

	// an unused host shouldn't be blocked
	assertHostAndContract(unusedBadHost.PublicKey, false, "")
}
