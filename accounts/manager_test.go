package accounts_test

import (
	"errors"
	"math"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"go.sia.tech/core/types"
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

// TestUpdateFundedPools is a unit test that covers the functionality of
// updating the funded pool. It asserts that the consecutive failed funds
// and next fund time are updated correctly based on the number of funded
// pools.
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
			// handle panic case
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
				// calculate expected values
				var wantConsecFailures int
				var wantNextFund time.Time
				if i < tc.funded {
					wantConsecFailures = 0
					wantNextFund = time.Now().Add(accounts.PoolFundInterval)
				} else {
					wantConsecFailures = tc.pools[i].ConsecutiveFailedFunds + 1
					wantNextFund = time.Now().Add(min(time.Duration(math.Pow(2, float64(wantConsecFailures)))*time.Minute, maxBackoff))
				}

				// assert updates
				if pool.ConsecutiveFailedFunds != wantConsecFailures {
					t.Fatal("unexpected consecutive failed funds", pool.ConsecutiveFailedFunds, wantConsecFailures)
				} else if !approxEqual(pool.NextFund, wantNextFund) {
					t.Fatal("unexpected next fund", pool.NextFund, wantNextFund)
				}
			}
		})
	}
}

func TestPruneExpiredPreAuthorizedKeys(t *testing.T) {
	store := newTestStore(t)
	synctest.Test(t, func(t *testing.T) {
		const maintenanceInterval = time.Hour
		manager, err := accounts.NewManager(store, accounts.WithPruneAccountsInterval(maintenanceInterval))
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()

		fundTarget := uint64(1)
		if err := manager.PutQuota(t.Context(), testutils.TestQuotaName, accounts.PutQuotaRequest{
			MaxPinnedData:   1,
			TotalUses:       1,
			FundTargetBytes: &fundTarget,
		}); err != nil {
			t.Fatal(err)
		}
		connectKey, err := manager.AddAppConnectKey(t.Context(), accounts.AppConnectKeyRequest{Key: "connect-key", Quota: testutils.TestQuotaName})
		if err != nil {
			t.Fatal(err)
		}
		preAuthorizedPrivateKey := types.GeneratePrivateKey()
		preAuthorizedKeyRequest := accounts.PreAuthorizedKeyRequest{
			ConnectKey: connectKey.Key,
			Expiration: time.Now().Add(maintenanceInterval / 2),
			TotalUses:  1,
		}
		preAuthorizedKeyRequest.Sign(preAuthorizedPrivateKey)
		preAuthorizedKey, err := manager.AddPreAuthorizedKey(t.Context(), preAuthorizedKeyRequest)
		if err != nil {
			t.Fatal(err)
		}

		synctest.Wait()
		if _, err := manager.PreAuthorizedKey(t.Context(), preAuthorizedKey.PublicKey); err != nil {
			t.Fatalf("expected key before maintenance: %v", err)
		}

		time.Sleep(maintenanceInterval)
		synctest.Wait()
		if _, err := manager.PreAuthorizedKey(t.Context(), preAuthorizedKey.PublicKey); !errors.Is(err, accounts.ErrKeyNotFound) {
			t.Fatalf("expected expired key to be pruned, got %v", err)
		}
	})
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
