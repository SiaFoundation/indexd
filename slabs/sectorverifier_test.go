package slabs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap/zaptest"
)

func TestSectorVerifier(t *testing.T) {
	log := zaptest.NewLogger(t)
	oneSC := types.Siacoins(1)

	// prepare manager
	store := newMockStore()
	am := newMockAccountManager(store)

	// prepare account
	sk := types.GeneratePrivateKey()
	acc := proto.Account(sk.PublicKey())
	am.RegisterServiceAccount(acc)

	// prepare host
	client := newMockHostClient()
	hostKey := types.GeneratePrivateKey()
	client.addTestHost(hostKey)
	client.hostSettings[hostKey.PublicKey()] = proto.HostSettings{
		Prices: proto.HostPrices{
			EgressPrice: types.Siacoins(1).Div64(4096), // 1SC per 4KiB too align with minimum egress charge
		},
	}

	// prepare roots
	roots := make([]types.Hash256, 3)
	for i := range roots {
		result, err := client.WriteSector(t.Context(), sk, hostKey.PublicKey(), []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		roots[i] = result.Root
	}

	// prepare verifier
	verifier := NewSectorVerifier(am, client, sk, log)

	// prepare helper to assert account balance
	assertBalance := func(want types.Currency) {
		t.Helper()
		got, err := am.ServiceAccountBalance(context.Background(), hostKey.PublicKey(), acc)
		if err != nil {
			t.Fatal(err)
		} else if !got.Equals(want) {
			t.Fatalf("expected balance %v, got %v", want, got)
		}
	}

	// prepare helper to assert verify sector results
	assertResults := func(roots []types.Hash256, want []CheckSectorsResult, expectedErr error) {
		t.Helper()
		got, err := verifier.VerifySectors(context.Background(), hostKey.PublicKey(), roots)
		if err != nil && expectedErr == nil {
			t.Fatal(err)
		}
		if (len(want) != 0 || len(got) != 0) && !reflect.DeepEqual(got, want) {
			t.Fatalf("expected results %v, got %v", want, got)
		}
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
	}

	// prepare helper to update account balance
	updateBalance := func(amount types.Currency) {
		t.Helper()
		err := am.UpdateServiceAccountBalance(context.Background(), hostKey.PublicKey(), acc, amount)
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert [errInsufficientServiceAccountBalance] is returned
	_, err := verifier.VerifySectors(context.Background(), hostKey.PublicKey(), roots[:1])
	if !errors.Is(err, errInsufficientServiceAccountBalance) {
		t.Fatal("unexpected err", err)
	}

	// add 10SC to the account
	updateBalance(oneSC.Mul64(10))

	// case 1: successfully verify a lost and a good sector, should debit the
	// account balance
	client.integrityErrors[roots[0]] = proto.ErrSectorNotFound // lost
	client.integrityErrors[roots[1]] = nil                     // good
	assertResults(roots[:2], []CheckSectorsResult{SectorLost, SectorSuccess}, nil)
	assertBalance(oneSC.Mul64(8))

	// case 2: running out of funds unexpectedly (malicious host) should reset the balance but
	// should continue to verify sectors
	client.integrityErrors[roots[0]] = proto.ErrNotEnoughFunds // unexpected OOF
	client.integrityErrors[roots[1]] = nil                     // good
	assertResults(roots[:2], []CheckSectorsResult{SectorFailed, SectorSuccess}, nil)
	assertBalance(types.ZeroCurrency)

	// case 3: running out of funds expectedly
	updateBalance(types.Siacoins(2))
	client.integrityErrors[roots[0]] = nil // good
	client.integrityErrors[roots[1]] = nil // good
	client.integrityErrors[roots[2]] = nil // good
	assertResults(roots, []CheckSectorsResult{SectorSuccess, SectorSuccess}, errInsufficientServiceAccountBalance)

	// case 4: interruption via context
	updateBalance(types.Siacoins(10))
	client.integrityErrors[roots[0]] = nil              // good sector
	client.integrityErrors[roots[1]] = context.Canceled // verification interrupted
	assertResults(roots[:2], []CheckSectorsResult{SectorSuccess}, context.Canceled)

	// case 5: interruption via gracefully closed stream
	updateBalance(types.Siacoins(10))
	client.integrityErrors[roots[0]] = nil                 // good sector
	client.integrityErrors[roots[1]] = mux.ErrClosedStream // verification interrupted
	assertResults(roots[:2], []CheckSectorsResult{SectorSuccess}, mux.ErrClosedStream)
}
