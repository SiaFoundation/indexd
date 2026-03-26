package slabs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap/zaptest"
)

func TestSectorVerifier(t *testing.T) {
	log := zaptest.NewLogger(t)
	oneSC := types.Siacoins(1)

	// prepare manager
	am := newMockAccountManager()

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
	verifier, err := slabs.NewSectorVerifier(am, client, sk, log)
	if err != nil {
		t.Fatal(err)
	}

	// prepare helper to assert account balance
	assertBalance := func(want types.Currency) {
		t.Helper()
		got, err := am.ServiceAccountBalance(hostKey.PublicKey(), acc)
		if err != nil {
			t.Fatal(err)
		} else if !got.Equals(want) {
			t.Fatalf("expected balance %v, got %v", want, got)
		}
	}

	// prepare helper to assert verify sector results
	assertResults := func(ctx context.Context, roots []types.Hash256, want []slabs.CheckSectorsResult, expectedErr error) {
		t.Helper()
		got, err := verifier.VerifySectors(ctx, hostKey.PublicKey(), roots)
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
		err := am.UpdateServiceAccountBalance(hostKey.PublicKey(), acc, amount)
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert [errInsufficientServiceAccountBalance] is returned
	_, err = verifier.VerifySectors(t.Context(), hostKey.PublicKey(), roots[:1])
	if !errors.Is(err, slabs.ErrInsufficientServiceAccountBalance) {
		t.Fatal("unexpected err", err)
	}

	// add 10SC to the account
	updateBalance(oneSC.Mul64(10))

	// case 1: successfully verify a lost and a good sector, should debit the
	// account balance
	client.integrityErrors[roots[0]] = wrapRPCErr(proto.ErrSectorNotFound) // lost
	client.integrityErrors[roots[1]] = nil                                 // good
	assertResults(t.Context(), roots[:2], []slabs.CheckSectorsResult{slabs.SectorLost, slabs.SectorSuccess}, nil)
	assertBalance(oneSC.Mul64(8))

	// case 2: running out of funds unexpectedly (malicious host) should reset the balance but
	// should continue to verify sectors
	client.integrityErrors[roots[0]] = wrapRPCErr(proto.ErrNotEnoughFunds) // unexpected OOF
	client.integrityErrors[roots[1]] = nil                                 // good
	assertResults(t.Context(), roots[:2], []slabs.CheckSectorsResult{slabs.SectorFailed, slabs.SectorSuccess}, nil)
	assertBalance(types.ZeroCurrency)

	// case 3: running out of funds expectedly
	updateBalance(types.Siacoins(2))
	client.integrityErrors[roots[0]] = nil // good
	client.integrityErrors[roots[1]] = nil // good
	client.integrityErrors[roots[2]] = nil // good
	assertResults(t.Context(), roots, []slabs.CheckSectorsResult{slabs.SectorSuccess, slabs.SectorSuccess}, slabs.ErrInsufficientServiceAccountBalance)

	// case 4: interruption via context
	updateBalance(types.Siacoins(10))
	client.integrityErrors[roots[0]] = nil              // good sector
	client.integrityErrors[roots[1]] = context.Canceled // verification interrupted
	assertResults(t.Context(), roots[:2], []slabs.CheckSectorsResult{slabs.SectorSuccess}, context.Canceled)

	// case 5: interruption via gracefully closed stream
	updateBalance(types.Siacoins(10))
	client.integrityErrors[roots[0]] = nil                 // good sector
	client.integrityErrors[roots[1]] = mux.ErrClosedStream // verification interrupted
	assertResults(t.Context(), roots[:2], []slabs.CheckSectorsResult{slabs.SectorSuccess}, mux.ErrClosedStream)

	// case 6: interruption via deadline exceeded on second sector
	updateBalance(types.Siacoins(10))
	client.integrityErrors[roots[0]] = nil                      // good sector
	client.integrityErrors[roots[1]] = context.DeadlineExceeded // deadline fires during ReadSector
	assertResults(t.Context(), roots[:2], []slabs.CheckSectorsResult{slabs.SectorSuccess}, context.DeadlineExceeded)

	// case 7: interruption via deadline exceeded on first sector
	updateBalance(types.Siacoins(10))
	client.integrityErrors[roots[0]] = context.DeadlineExceeded // deadline fires immediately
	assertResults(t.Context(), roots[:1], nil, context.DeadlineExceeded)

	// case 8: verify timeout fires on a slow host
	updateBalance(types.Siacoins(10))
	client.integrityErrors = make(map[types.Hash256]error)
	client.setSlowHost(hostKey.PublicKey(), 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	assertResults(ctx, roots[:1], nil, context.DeadlineExceeded)
	cancel()

	// case 9: fast host completes within verify timeout
	updateBalance(types.Siacoins(10))
	client.setSlowHost(hostKey.PublicKey(), 0)
	ctx, cancel = context.WithTimeout(t.Context(), 500*time.Millisecond)
	assertResults(ctx, roots, []slabs.CheckSectorsResult{slabs.SectorSuccess, slabs.SectorSuccess, slabs.SectorSuccess}, nil)
	cancel()
}
