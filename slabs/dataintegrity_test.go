package slabs

import (
	"context"
	"errors"
	"reflect"
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
	panic("unknown sector")
}

func TestVerifySectors(t *testing.T) {
	store := newMockStore()
	am := newMockAccountManager(store)
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

	// helper to call verify sectors
	verifySectors := func(hostSectors map[types.Hash256]error, toVerify []types.Hash256, expectedResults []CheckSectorsResult) error {
		verifier := newMockSectorVerifier(host.Settings.Prices)
		verifier.sectors = hostSectors
		results, err := sm.verifySectors(context.Background(), verifier, host, toVerify)
		if !reflect.DeepEqual(results, expectedResults) {
			t.Fatalf("expected %v, got %v", expectedResults, results)
		}
		return err
	}

	// helper to assert balance of service account
	assertBalance := func(expected types.Currency) {
		t.Helper()
		balance, err := am.ServiceAccountBalance(context.Background(), host.PublicKey, proto.Account(account.PublicKey()))
		if err != nil {
			t.Fatal(err)
		} else if !balance.Equals(expected) {
			t.Fatalf("expected balance %v, got %v", expected, balance)
		}
	}

	// helper to set balance of service account
	updateBalance := func(amount types.Currency) {
		t.Helper()
		err := am.UpdateServiceAccountBalance(context.Background(), host.PublicKey, proto.Account(account.PublicKey()), amount)
		if err != nil {
			t.Fatal(err)
		}
	}

	// verifying sector before funding the service account fails.
	err = verifySectors(map[types.Hash256]error{}, []types.Hash256{
		{1},
	}, nil)
	if !errors.Is(err, errInsufficientServiceAccountBalance) {
		t.Fatalf("expected insufficient balance error, got %v", err)
	}

	// add 3SC to the account
	updateBalance(types.Siacoins(3))

	// case 1: successfully verify a lost and a good sector
	err = verifySectors(map[types.Hash256]error{
		{1}: proto.ErrSectorNotFound, // lost
		{2}: nil,                     // good
	}, []types.Hash256{
		{1},
		{2},
	}, []CheckSectorsResult{SectorLost, SectorSuccess})
	if err != nil {
		t.Fatal(err)
	}

	// assert withdrawal: 3SC-2SC = 1SC
	assertBalance(types.Siacoins(1))

	// case 2: running out of funds unexpectedly (malicious host) should reset the balance but
	// should continue to verify sectors
	updateBalance(types.Siacoins(10))
	err = verifySectors(map[types.Hash256]error{
		{1}: proto.ErrNotEnoughFunds, // unexpected OOF
		{2}: nil,                     // good sector
	}, []types.Hash256{
		{1},
		{2},
	}, []CheckSectorsResult{SectorFailed, SectorSuccess})
	if err != nil {
		t.Fatal(err)
	}
	assertBalance(types.ZeroCurrency)

	// case 3: running out of funds expectedly
	updateBalance(types.Siacoins(2))
	err = verifySectors(map[types.Hash256]error{
		{1}: nil, // good sector
		{2}: nil, // good sector
		{3}: nil, // good sector
	}, []types.Hash256{
		{1},
		{2},
		{3},
	}, []CheckSectorsResult{SectorSuccess, SectorSuccess})
	if !errors.Is(err, errInsufficientServiceAccountBalance) {
		t.Fatalf("expected insufficient balance error, got %v", err)
	}

	// case 4: interruption via context
	updateBalance(types.Siacoins(10))
	err = verifySectors(map[types.Hash256]error{
		{1}: nil,              // good sector
		{2}: context.Canceled, // verification interrupted
	}, []types.Hash256{
		{1},
		{2},
	}, []CheckSectorsResult{SectorSuccess})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}
}
