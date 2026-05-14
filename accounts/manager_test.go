package accounts_test

import (
	"math"
	"slices"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
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
	maxBackoff := 128 * time.Minute
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
				accounts.UpdateFundedAccounts(tc.accs, tc.funded, maxBackoff)
				return
			}

			updated := slices.Clone(tc.accs)
			accounts.UpdateFundedAccounts(updated, tc.funded, maxBackoff)

			for i, acc := range updated {
				// calculate expected values
				var wantConsecFailures int
				var wantNextFund time.Time
				if i < tc.funded {
					wantConsecFailures = 0
					wantNextFund = time.Now().Add(accounts.AccountFundInterval)
				} else {
					wantConsecFailures = tc.accs[i].ConsecutiveFailedFunds + 1
					wantNextFund = time.Now().Add(min(time.Duration(math.Pow(2, float64(wantConsecFailures)))*time.Minute, maxBackoff))
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

func TestUpdateFundedPools(t *testing.T) {
	maxBackoff := 128 * time.Minute
	tests := []struct {
		name   string
		pools  []accounts.HostPool
		funded int
		panic  bool
	}{
		{
			name: "all funded",
			pools: []accounts.HostPool{
				{ConsecutiveFailedFunds: 3},
				{ConsecutiveFailedFunds: 5},
			},
			funded: 2,
		},
		{
			name: "none funded",
			pools: []accounts.HostPool{
				{ConsecutiveFailedFunds: 0},
				{ConsecutiveFailedFunds: 1},
			},
			funded: 0,
		},
		{
			name: "partially funded",
			pools: []accounts.HostPool{
				{ConsecutiveFailedFunds: 2},
				{ConsecutiveFailedFunds: 4},
				{ConsecutiveFailedFunds: 0},
			},
			funded: 2,
		},
		{
			name: "sanity check",
			pools: []accounts.HostPool{
				{ConsecutiveFailedFunds: 1},
			},
			funded: 2,
			panic:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.panic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("expected panic but function did not panic")
					}
				}()
				accounts.UpdateFundedPools(tc.pools, tc.funded, maxBackoff)
				return
			}

			updated := slices.Clone(tc.pools)
			accounts.UpdateFundedPools(updated, tc.funded, maxBackoff)

			for i, pool := range updated {
				var wantConsecFailures int
				var wantNextFund time.Time
				if i < tc.funded {
					wantConsecFailures = 0
					wantNextFund = time.Now().Add(accounts.PoolFundInterval)
				} else {
					wantConsecFailures = tc.pools[i].ConsecutiveFailedFunds + 1
					wantNextFund = time.Now().Add(min(time.Duration(math.Pow(2, float64(wantConsecFailures)))*time.Minute, maxBackoff))
				}

				if pool.ConsecutiveFailedFunds != wantConsecFailures {
					t.Fatal("unexpected consecutive failed funds", pool.ConsecutiveFailedFunds, wantConsecFailures)
				} else if !approxEqual(pool.NextFund, wantNextFund) {
					t.Fatal("unexpected next fund", pool.NextFund, wantNextFund)
				}
			}
		})
	}
}

func TestPoolFundTarget(t *testing.T) {
	host := hosts.Host{
		Settings: proto.HostSettings{
			Prices: proto.HostPrices{
				StoragePrice: types.NewCurrency64(100),
				IngressPrice: types.NewCurrency64(100),
				EgressPrice:  types.NewCurrency64(100),
				Collateral:   types.NewCurrency64(200),
			},
		},
	}

	fundTargetBytes := uint64(4 << 20) // 1 sector
	base := accounts.HostFundTarget(host, fundTargetBytes)
	if base.IsZero() {
		t.Fatal("base fund target should not be zero")
	}

	tests := []struct {
		activeAccounts uint64
		wantMultiple   uint64
	}{
		{0, 3}, // clamped to minimum 3
		{1, 3}, // clamped to minimum 3
		{2, 3}, // clamped to minimum 3
		{3, 3}, // exact multiple of 3
		{4, 6}, // rounded up to 6
		{5, 6}, // rounded up to 6
		{6, 6}, // exact multiple of 3
		{7, 9}, // rounded up to 9
		{9, 9}, // exact multiple of 3
		{10, 12},
		{100, 102},
	}

	for _, tc := range tests {
		result := accounts.PoolFundTarget(host, fundTargetBytes, tc.activeAccounts)
		expected := base.Mul64(tc.wantMultiple)
		if result != expected {
			t.Fatalf("PoolFundTarget(%d accounts): got %v, want %v (base * %d)", tc.activeAccounts, result, expected, tc.wantMultiple)
		}
	}

	// zero fund target bytes should always return zero
	result := accounts.PoolFundTarget(host, 0, 10)
	if !result.IsZero() {
		t.Fatal("expected zero for zero fund target bytes")
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
