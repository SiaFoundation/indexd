package contracts_test

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func TestBroadcastContractRevisions(t *testing.T) {
	cmMock := newChainManagerMock()
	syncerMock := &syncerMock{}
	walletMock := &walletMock{}
	store := newTestStore(t)

	cm := contracts.NewTestContractManager(types.PublicKey{}, nil, nil, cmMock, store, nil, nil, nil, contracts.NewContractLocker(), nil, syncerMock, walletMock)
	cm.SetRevisionBroadcastInterval(time.Minute)

	// add host
	hk := types.PublicKey{1}
	h := hosts.Host{
		PublicKey: hk,
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host.com"}},
		Settings:  goodSettings,
		Usability: hosts.GoodUsability,
	}
	store.addTestHost(t, h)

	// c1 is not broadcasted because it's not revisable
	c1 := store.addTestContract(t, hk, true, types.FileContractID{1})
	store.setContractState(t, c1, contracts.ContractStateExpired)

	// c3 is not broadcasted because it was broadcasted recently
	// (created before c2 because c2.renewed_to references c3)
	c3 := store.addTestContract(t, hk, true, types.FileContractID{3})
	store.setContractLastBroadcastAttempt(t, c3, time.Now())

	// c2 is not broadcasted because it's renewed
	c2 := store.addTestContract(t, hk, true, types.FileContractID{2})
	store.setContractRenewedTo(t, c2, c3)

	// c4 is broadcasted (active, not renewed, not recently broadcasted)
	c4 := store.addTestContract(t, hk, true, types.FileContractID{4})
	store.setContractState(t, c4, contracts.ContractStateActive)
	// ensure it wasn't broadcasted recently by setting a time far in the past
	store.setContractLastBroadcastAttempt(t, c4, time.Time{})

	// get the revision we expect to be broadcasted
	rev, _, err := store.ContractRevision(c4)
	if err != nil {
		t.Fatal(err)
	}

	// add contract element for c4 so it can be broadcasted
	store.addContractElement(t, types.V2FileContractElement{
		ID: c4,
		StateElement: types.StateElement{
			LeafIndex:   1,
			MerkleProof: []types.Hash256{{1}},
		},
		V2FileContract: rev.Revision,
	})

	// refetch the revision after adding the element
	rev, _, err = store.ContractRevision(c4)
	if err != nil {
		t.Fatal(err)
	}

	// assert revision was broadcasted and contract was marked as such
	if err := cm.PerformBroadcastContractRevisions(t.Context(), zap.NewNop()); err != nil {
		t.Fatal(err)
	} else if len(walletMock.broadcasted) != 1 {
		t.Fatal("expected 1 broadcasted contract, got", len(walletMock.broadcasted))
	} else if walletMock.broadcasted[0].FileContractRevisions[0].Revision != rev.Revision {
		t.Fatal("unexpected revision", walletMock.broadcasted[0].FileContractRevisions[0].Revision, rev.Revision)
	} else if contract, err := store.Contract(types.FileContractID{4}); err != nil {
		t.Fatal(err)
	} else if contract.LastBroadcastAttempt.IsZero() {
		t.Fatal("expected last successful broadcast to be set")
	}
}
