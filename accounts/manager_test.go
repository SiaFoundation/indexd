package accounts_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

var (
	// goodUsability is the usability of a host that passes all checks
	goodUsability = hosts.Usability{
		Uptime:              true,
		MaxContractDuration: true,
		MaxCollateral:       true,
		ProtocolVersion:     true,
		PriceValidity:       true,
		AcceptingContracts:  true,

		ContractPrice:   true,
		Collateral:      true,
		StoragePrice:    true,
		IngressPrice:    true,
		EgressPrice:     true,
		FreeSectorPrice: true,
	}
)

type fundAccountCall struct {
	host        hosts.Host
	accounts    []accounts.HostAccount
	contractIDs []types.FileContractID
	target      types.Currency
}

type mockFunder struct {
	calls []fundAccountCall
	fail  bool
}

func (f *mockFunder) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accounts []accounts.HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, _ error) {
	f.calls = append(f.calls, fundAccountCall{
		host:        host,
		accounts:    accounts,
		contractIDs: contractIDs,
		target:      target,
	})
	if f.fail {
		return 0, 0, nil
	}
	return len(accounts), 1, nil
}

type testStore struct {
	testutils.TestStore
}

func (s testStore) resetNextFund(t testing.TB) {
	t.Helper()
	if _, err := s.Exec(t.Context(), `UPDATE account_hosts SET next_fund = NOW()`); err != nil {
		t.Fatal(err)
	}
}

func (s testStore) hostAccounts(t testing.TB) (result []accounts.HostAccount) {
	t.Helper()

	rows, err := s.Query(t.Context(), `SELECT next_fund FROM account_hosts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var account accounts.HostAccount
		if err := rows.Scan(&account.NextFund); err != nil {
			t.Fatal(err)
		}
		result = append(result, account)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return
}

func newTestStore(t testing.TB) testStore {
	s := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() {
		s.Close()
	})

	return testStore{s}
}

// TestAccountManager is a unit test that covers the functionality of the
// account manager. It asserts FundAccounts fetches and updates accounts for
// funding and the returned usage is calculated correctly.
func TestAccountManager(t *testing.T) {
	s := newTestStore(t)
	f := &mockFunder{}

	am := accounts.NewManager(s, f)
	defer am.Close()

	hs := proto.HostSettings{
		Prices: proto.HostPrices{
			EgressPrice:  types.Siacoins(1),
			IngressPrice: types.Siacoins(1),
			StoragePrice: types.Siacoins(1),
		},
	}

	host := hosts.Host{
		Settings:  hs,
		PublicKey: types.GeneratePrivateKey().PublicKey(),
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "foo"}},
		Usability: goodUsability,
	}

	contractIDs := []types.FileContractID{{1}}
	err := am.FundAccounts(context.Background(), host, contractIDs, false, zap.NewNop())
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected host not found error")
	}

	// add a host and two accounts
	s.AddTestHost(t, host)

	pk1 := types.GeneratePrivateKey().PublicKey()
	s.AddTestAccount(t, pk1)

	pk2 := types.GeneratePrivateKey().PublicKey()
	s.AddTestAccount(t, pk2)

	// fund accounts
	err = am.FundAccounts(context.Background(), host, contractIDs, false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert the call params
	if len(f.calls) != 1 {
		t.Fatal("expected one call to fund accounts")
	} else if !reflect.DeepEqual(f.calls[0].host, host) {
		t.Fatal("expected host key to match")
	} else if len(f.calls[0].accounts) != 2 {
		t.Fatal("expected two accounts to be funded")
	}

	// assert the accounts were updated
	eas := s.hostAccounts(t)
	if len(eas) != 2 {
		t.Fatal("expected two accounts to be updated")
	}
	expected := time.Now().Add(accounts.AccountFundInterval)
	for _, ea := range eas {
		if !approxEqual(ea.NextFund, expected) {
			t.Fatal("expected next fund to be updated to the next fund interval", ea.NextFund)
		}
	}

	// simulate a couple of failed fund attempts
	f.fail = true
	for range 3 {
		s.resetNextFund(t)
		err = am.FundAccounts(context.Background(), host, contractIDs, false, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert the exponential backoff was applied
	expected = time.Now().Add(8 * time.Minute)
	for _, ea := range s.hostAccounts(t) {
		if !approxEqual(ea.NextFund, expected) {
			t.Fatal("expected next fund to be updated to the exponential backoff", ea.NextFund)
		}
	}

	// reset state
	f.fail = false
	f.calls = f.calls[:0]
	s.resetNextFund(t)

	// add another 1000 accounts
	for range 1000 {
		pk := types.GeneratePrivateKey().PublicKey()
		s.AddTestAccount(t, pk)
	}

	// fund accounts
	contractIDs = append(contractIDs, types.FileContractID{2})
	err = am.FundAccounts(context.Background(), host, contractIDs, false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert batches were applied correctly
	target := accounts.HostFundTarget(host)
	if len(f.calls) != 2 {
		t.Fatal("expected two calls to fund accounts")
	} else if len(f.calls[0].accounts) != accounts.AccountFundBatch {
		t.Fatal("expected first call to fund 1000 accounts")
	} else if len(f.calls[1].accounts) != 2 {
		t.Fatal("expected second call to fund 2 accounts")
	} else if len(f.calls[0].contractIDs) != 2 {
		t.Fatal("expected first call to have two contract IDs")
	} else if len(f.calls[1].contractIDs) != 1 {
		t.Fatal("expected second call to have one contract ID")
	} else if !f.calls[0].target.Equals(target) {
		t.Fatalf("expected target to be %v, got %v", target, f.calls[0].target)
	} else if !f.calls[1].target.Equals(target) {
		t.Fatalf("expected target to be %v, got %v", target, f.calls[1].target)
	}

	// assert all accounts next fund was updated and consecutive failed funds was reset
	expected = time.Now().Add(time.Hour)
	for _, ea := range s.hostAccounts(t) {
		if !approxEqual(ea.NextFund, expected) {
			t.Fatal("expected next fund to be updated to the next fund interval", ea.NextFund)
		}
	}

	// assert there's no accounts to fund
	err = am.FundAccounts(context.Background(), host, contractIDs, false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(f.calls) != 2 {
		t.Fatal("expected two calls to fund accounts")
	}

	// assert we can force a refill on all accounts
	err = am.FundAccounts(context.Background(), host, contractIDs, true, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(f.calls) != 4 {
		t.Fatal("expected four calls to fund accounts")
	}
}

// TestUpdateFundedAccounts is a unit test that covers the functionality of
// updating the funded accounts. It asserts that the consecutive failed funds
// and next fund time are updated correctly based on the number of funded
// accounts.
func TestUpdateFundedAccounts(t *testing.T) {
	tests := []struct {
		name   string
		accs   []accounts.HostAccount
		funded int
		panic  bool
	}{
		{
			name: "all funded",
			accs: []accounts.HostAccount{
				{ConsecutiveFailedFunds: 3},
				{ConsecutiveFailedFunds: 5},
			},
			funded: 2,
		},
		{
			name: "none funded",
			accs: []accounts.HostAccount{
				{ConsecutiveFailedFunds: 0},
				{ConsecutiveFailedFunds: 1},
			},
			funded: 0,
		},
		{
			name: "partially funded",
			accs: []accounts.HostAccount{
				{ConsecutiveFailedFunds: 2},
				{ConsecutiveFailedFunds: 4},
				{ConsecutiveFailedFunds: 0},
			},
			funded: 2,
		},
		{
			name: "sanity check",
			accs: []accounts.HostAccount{
				{ConsecutiveFailedFunds: 1},
			},
			funded: 2,
			panic:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// handle panic case
			if tc.panic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("expected panic but function did not panic")
					}
				}()
				accounts.UpdateFundedAccounts(tc.accs, tc.funded)
				return
			}

			updated := slices.Clone(tc.accs)
			accounts.UpdateFundedAccounts(updated, tc.funded)

			for i, acc := range updated {
				// calculate expected values
				var wantConsecFailures int
				var wantNextFund time.Time
				if i < tc.funded {
					wantConsecFailures = 0
					wantNextFund = time.Now().Add(accounts.AccountFundInterval)
				} else {
					wantConsecFailures = tc.accs[i].ConsecutiveFailedFunds + 1
					wantNextFund = time.Now().Add(time.Duration(min(math.Pow(2, float64(wantConsecFailures)), accounts.AccountExpBackoffMaxMinutes)) * time.Minute)
				}

				// assert updates
				if acc.ConsecutiveFailedFunds != wantConsecFailures {
					t.Fatal("unexpected consecutive failed funds", acc.ConsecutiveFailedFunds, wantConsecFailures)
				} else if !approxEqual(acc.NextFund, wantNextFund) {
					t.Fatal("unexpected next fund", acc.NextFund, wantNextFund)
				}
			}
		})
	}
}

// approxEqual checks if two time.Time values are within a second of each
// other.
func approxEqual(t1, t2 time.Time) bool {
	const tol = time.Second

	diff := t1.Sub(t2)
	if diff < 0 {
		diff = -diff
	}
	return diff <= tol
}
