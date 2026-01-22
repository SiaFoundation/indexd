package accounts_test

import (
	"math"
	"slices"
	"testing"
	"time"

	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap/zaptest"
)

type testStore struct {
	testutils.TestStore
}

func newTestStore(t testing.TB) testStore {
	s := testutils.NewDB(t, contracts.DefaultMaintenanceSettings, zaptest.NewLogger(t))
	t.Cleanup(func() {
		s.Close()
	})

	return testStore{s}
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
