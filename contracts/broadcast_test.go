package contracts

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func TestBroadcastContractRevisions(t *testing.T) {
	cmMock := newChainManagerMock()
	dialer := newDialerMock()
	syncerMock := &syncerMock{}
	walletMock := &walletMock{}
	store := &storeMock{}

	contracts := newContractManager(types.PublicKey{}, nil, cmMock, store, dialer, nil, syncerMock, walletMock)
	contracts.revisionBroadcastInterval = time.Minute

	// add host
	hk := types.PublicKey{1}
	store.hosts = map[types.PublicKey]hosts.Host{
		hk: {
			PublicKey: hk,
			Networks:  []string{"127.0.0.1/24"},
			Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host.com"}},
			Settings:  goodSettings,
			Usability: hosts.GoodUsability,
		},
	}

	// c1 is not broadcasted because it's not revisble
	store.contracts = append(store.contracts, Contract{
		ID:        types.FileContractID{1},
		HostKey:   hk,
		Formation: time.Now(),
		State:     ContractStateExpired,
		Good:      true,
	})
	store.revisions = append(store.revisions, rhp.ContractRevision{ID: types.FileContractID{1}, Revision: newTestRevision(hk)})

	// c2 is not broadcasted because it's renewed
	store.contracts = append(store.contracts, Contract{
		ID:        types.FileContractID{2},
		HostKey:   hk,
		Formation: time.Now(),
		State:     ContractStateActive,
		Good:      true,
		RenewedTo: types.FileContractID{3},
	})
	store.revisions = append(store.revisions, rhp.ContractRevision{ID: types.FileContractID{2}, Revision: newTestRevision(hk)})

	// c3 is not broadcasted because it was broadcasted recently
	store.contracts = append(store.contracts, Contract{
		ID:                   types.FileContractID{3},
		HostKey:              hk,
		Formation:            time.Now(),
		State:                ContractStateActive,
		Good:                 true,
		LastBroadcastAttempt: time.Now(),
	})
	store.revisions = append(store.revisions, rhp.ContractRevision{ID: types.FileContractID{3}, Revision: newTestRevision(hk)})

	// c4 is broadcasted
	store.contracts = append(store.contracts, Contract{
		ID:        types.FileContractID{4},
		HostKey:   hk,
		Formation: time.Now(),
		State:     ContractStateActive,
		Good:      true,
	})
	rev := newTestRevision(hk)
	store.revisions = append(store.revisions, rhp.ContractRevision{ID: types.FileContractID{4}, Revision: rev})

	// assert revision was broadcasted and contract was marked as such
	if err := contracts.performBroadcastContractRevisions(context.Background(), zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(walletMock.broadcasted) != 1 {
		t.Fatal("expected 1 broadcasted contract, got", len(walletMock.broadcasted))
	} else if walletMock.broadcasted[0].FileContractRevisions[0].Revision != rev {
		t.Fatal("unexpected revision", walletMock.broadcasted[0].FileContractRevisions[0].Revision, rev)
	} else if contract, err := store.Contract(context.Background(), types.FileContractID{4}); err != nil {
		t.Fatal(err)
	} else if contract.LastBroadcastAttempt.IsZero() {
		t.Fatal("expected last successful broadcast to be set")
	}
}
