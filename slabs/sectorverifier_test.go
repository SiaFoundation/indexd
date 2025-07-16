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

func (c *mockHostClient) Close() error {
	return nil
}

func (c *mockHostClient) VerifySector(ctx context.Context, hostPrices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	err, ok := c.integrity[root]
	if !ok {
		panic("unknown sector: " + root.String())
	}
	return rhp.RPCVerifySectorResult{Usage: hostPrices.RPCVerifySectorCost()}, err
}

func TestSectorVerifier(t *testing.T) {
	oneSC := types.Siacoins(1)

	// prepare manager
	store := newMockStore()
	am := newMockAccountManager(store)

	// prepare account
	sk := types.GeneratePrivateKey()
	acc := proto.Account(sk.PublicKey())
	am.RegisterServiceAccount(acc)

	// prepare host
	hk := types.PublicKey{1}
	host := hosts.Host{
		PublicKey: hk,
		Settings: proto.HostSettings{
			Prices: proto.HostPrices{
				EgressPrice: oneSC.Div64(proto.SectorSize), // 1SC per sector
			},
		},
	}

	// prepare roots
	r1 := types.Hash256{1}
	r2 := types.Hash256{2}
	r3 := types.Hash256{3}

	// prepare verifier
	dialer := newMockDialer([]hosts.Host{host})
	verifier := NewSectorVerifier(am, dialer, sk)

	// prepare helper to assert account balance
	assertBalance := func(want types.Currency) {
		t.Helper()
		got, err := am.ServiceAccountBalance(context.Background(), hk, acc)
		if err != nil {
			t.Fatal(err)
		} else if !got.Equals(want) {
			t.Fatalf("expected balance %v, got %v", want, got)
		}
	}

	// prepare helper to assert verify sector results
	assertResults := func(roots []types.Hash256, want []CheckSectorsResult, expectedErr error) {
		t.Helper()
		got, err := verifier.VerifySectors(context.Background(), host, roots)
		if err != nil && expectedErr == nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("expected results %v, got %v", want, got)
		}
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
	}

	// prepare helper to update account balance
	updateBalance := func(amount types.Currency) {
		t.Helper()
		err := am.UpdateServiceAccountBalance(context.Background(), hk, acc, amount)
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert [errInsufficientServiceAccountBalance] is returned
	_, err := verifier.VerifySectors(context.Background(), host, []types.Hash256{r1})
	if !errors.Is(err, errInsufficientServiceAccountBalance) {
		t.Fatal("unexpected err", err)
	}

	// add 10SC to the account
	updateBalance(oneSC.Mul64(10))

	// case 1: successfully verify a lost and a good sector, should debit the
	// account balance
	dialer.clients[host.PublicKey].integrity[r1] = proto.ErrSectorNotFound // lost
	dialer.clients[host.PublicKey].integrity[r2] = nil                     // good
	assertResults([]types.Hash256{r1, r2}, []CheckSectorsResult{SectorLost, SectorSuccess}, nil)
	assertBalance(oneSC.Mul64(8))

	// case 2: running out of funds unexpectedly (malicious host) should reset the balance but
	// should continue to verify sectors
	dialer.clients[host.PublicKey].integrity[r1] = proto.ErrNotEnoughFunds // unexpected OOF
	dialer.clients[host.PublicKey].integrity[r2] = nil                     // good
	assertResults([]types.Hash256{r1, r2}, []CheckSectorsResult{SectorFailed, SectorSuccess}, nil)
	assertBalance(types.ZeroCurrency)

	// case 3: running out of funds expectedly
	updateBalance(types.Siacoins(2))
	dialer.clients[host.PublicKey].integrity[r1] = nil // good
	dialer.clients[host.PublicKey].integrity[r2] = nil // good
	dialer.clients[host.PublicKey].integrity[r3] = nil // good
	assertResults([]types.Hash256{r1, r2, r3}, []CheckSectorsResult{SectorSuccess, SectorSuccess}, errInsufficientServiceAccountBalance)

	// case 4: interruption via context
	updateBalance(types.Siacoins(10))
	dialer.clients[host.PublicKey].integrity[r1] = nil              // good sector
	dialer.clients[host.PublicKey].integrity[r2] = context.Canceled // verification interrupted
	assertResults([]types.Hash256{r1, r2}, []CheckSectorsResult{SectorSuccess}, context.Canceled)
}
