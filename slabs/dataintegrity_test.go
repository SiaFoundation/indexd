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
	prices  proto.HostPrices
	sectors map[types.Hash256]error
}

func newMockSectorVerifier(prices proto.HostPrices) *mockSectorVerifier {
	return &mockSectorVerifier{
		prices: prices,
	}
}

func (ht *mockSectorVerifier) VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	if err, ok := ht.sectors[root]; ok {
		if err == nil {
			return rhp.RPCVerifySectorResult{
				Usage: prices.RPCVerifySectorCost(),
			}, nil
		}
		return rhp.RPCVerifySectorResult{}, err
	}
	return rhp.RPCVerifySectorResult{}, proto.ErrNotEnoughFunds
}

func TestVerifySectors(t *testing.T) {
	am := newMockAccountManager()
	store := newMockStore()
	account := types.GeneratePrivateKey()
	sm, err := newSlabManager(am, store, account)
	if err != nil {
		t.Fatal(err)
	}

	host := hosts.Host{
		PublicKey: types.PublicKey{1},
		Settings: proto.HostSettings{
			Prices: proto.HostPrices{
				EgressPrice: types.Siacoins(1).Div64(proto.SectorSize), // 1SC per sector
			},
		},
	}

	ht := newMockSectorVerifier(host.Settings.Prices)
	_, err = sm.verifySectors(context.Background(), ht, host, nil)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: case 1: successfully verify a bad and good sector

	// TODO: case 2: verify that running out of funds returns results up until the interruption

	// TODO: case 3: same but with context.Canceled
}
