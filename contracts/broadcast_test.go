package contracts

import (
	"context"
	"net"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
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

	contracts := newContractManager(types.PublicKey{}, nil, cmMock, dialer, nil, store, syncerMock, walletMock)
	contracts.revisionBroadcastInterval = time.Minute

	// add host
	hk := types.PublicKey{1}
	store.hosts = map[types.PublicKey]hosts.Host{
		hk: {
			PublicKey: hk,
			Networks:  []net.IPNet{{IP: net.IP{127, 0, 0, 1}, Mask: net.CIDRMask(24, 32)}},
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

	// c2 is not broadcasted because it's renewed
	store.contracts = append(store.contracts, Contract{
		ID:        types.FileContractID{2},
		HostKey:   hk,
		Formation: time.Now(),
		State:     ContractStateActive,
		Good:      true,
		RenewedTo: types.FileContractID{3},
	})

	// c3 is not broadcasted because it was broadcasted recently
	store.contracts = append(store.contracts, Contract{
		ID:                   types.FileContractID{3},
		HostKey:              hk,
		Formation:            time.Now(),
		State:                ContractStateActive,
		Good:                 true,
		LastBroadcastAttempt: time.Now(),
	})

	// c4 is broadcasted
	store.contracts = append(store.contracts, Contract{
		ID:        types.FileContractID{4},
		HostKey:   hk,
		Formation: time.Now(),
		State:     ContractStateActive,
		Good:      true,
	})

	// mock a latest revision
	rev := types.V2FileContract{RevisionNumber: 1}
	hc := newHostClientMock()
	dialer.clients[hk] = hc
	hc.latestRevisions[types.FileContractID{4}] = proto.RPCLatestRevisionResponse{Contract: rev}

	// assert revision was broadcasted and contract was marked as such
	if err := contracts.performBroadcastContractRevisions(context.Background(), zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(syncerMock.broadcasted) != 1 {
		t.Fatal("expected 1 broadcasted contract, got", len(syncerMock.broadcasted))
	} else if syncerMock.broadcasted[0].FileContractRevisions[0].Revision != rev {
		t.Fatal("unexpected revision", syncerMock.broadcasted[0].FileContractRevisions[0].Revision)
	} else if contract, err := store.Contract(context.Background(), types.FileContractID{4}); err != nil {
		t.Fatal(err)
	} else if contract.LastBroadcastAttempt.IsZero() {
		t.Fatal("expected last successful broadcast to be set")
	}
}
