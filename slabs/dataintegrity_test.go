package slabs

import (
	"context"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
)

type mockSectorVerifier struct {
}

func newMockSectorVerifier() *mockSectorVerifier {
	return &mockSectorVerifier{}
}

func (ht *mockSectorVerifier) VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	panic("not implemented")
}

func TestVerifySectors(t *testing.T) {
	am := newMockAccountManager()
	store := newMockStore()
	account := types.GeneratePrivateKey()
	sm, err := newSlabManager(am, store, account)
	if err != nil {
		t.Fatal(err)
	}

	ht := newMockSectorVerifier()
	_, err = sm.verifySectors(context.Background(), ht, hosts.Host{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: implement
}
