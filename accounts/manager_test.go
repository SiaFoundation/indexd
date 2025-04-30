package accounts

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
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
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

var _ Store = (*mockStore)(nil)

type mockStore struct {
	accounts map[types.PublicKey]struct{}
	hosts    map[types.PublicKey]hosts.Host
	eas      map[types.PublicKey]map[types.PublicKey]*HostAccount
}

func newMockStore() *mockStore {
	return &mockStore{
		accounts: make(map[types.PublicKey]struct{}),
		hosts:    make(map[types.PublicKey]hosts.Host),
		eas:      make(map[types.PublicKey]map[types.PublicKey]*HostAccount),
	}
}

func (s *mockStore) Host(ctx context.Context, hostKey types.PublicKey) (hosts.Host, error) {
	host, ok := s.hosts[hostKey]
	if !ok {
		return hosts.Host{}, hosts.ErrNotFound
	}
	return host, nil
}

func (s *mockStore) HostAccountsForFunding(ctx context.Context, hk types.PublicKey, limit int) (out []HostAccount, _ error) {
	_, err := s.Host(ctx, hk)
	if err != nil {
		return nil, err
	}

	existing, ok := s.eas[hk]
	if !ok {
		for acc := range s.accounts {
			out = append(out, HostAccount{
				HostKey:    hk,
				AccountKey: proto.Account(acc),
				NextFund:   time.Now(),
			})
		}
	} else {
		for acc := range s.accounts {
			ea, ok := existing[acc]
			if !ok {
				out = append(out, HostAccount{
					HostKey:    hk,
					AccountKey: proto.Account(acc),
					NextFund:   time.Now(),
				})
			} else if time.Now().After(ea.NextFund) {
				out = append(out, *ea)
			}
		}
	}

	if len(out) > limit {
		out = out[:limit]
	}
	return
}

func (s *mockStore) UpdateHostAccounts(ctx context.Context, accounts []HostAccount) error {
	for _, acc := range accounts {
		_, ok := s.eas[acc.HostKey]
		if !ok {
			s.eas[acc.HostKey] = make(map[types.PublicKey]*HostAccount)
		}
		s.eas[acc.HostKey][types.PublicKey(acc.AccountKey)] = &acc
	}
	return nil
}

// UpdateServiceAccountBalance updates the balance of a service account.
func (s *mockStore) UpdateServiceAccountBalance(ctx context.Context, account proto.Account, balance types.Currency) error {
	return nil // not implemented
}

// ServiceAccountBalance returns the balance of a service account.
func (s *mockStore) ServiceAccountBalance(ctx context.Context, account proto.Account) (types.Currency, error) {
	return types.Currency{}, nil // not implemented
}

func (s *mockStore) resetNextFund() {
	for _, eas := range s.eas {
		for _, ea := range eas {
			ea.NextFund = time.Now()
		}
	}
}

type fundAccountCall struct {
	host        hosts.Host
	accounts    []HostAccount
	contractIDs []types.FileContractID
	target      types.Currency
}

type mockFunder struct {
	calls []fundAccountCall
	fail  bool
}

func (f *mockFunder) FundAccounts(ctx context.Context, host hosts.Host, accounts []HostAccount, contractIDs []types.FileContractID, target types.Currency, log *zap.Logger) (funded int, drained int, _ error) {
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

// TestAccountManager is a unit test that covers the functionality of the
// account manager. It asserts FundAccounts fetches and updates accounts for
// funding and the returned usage is calculated correctly.
func TestAccountManager(t *testing.T) {
	s := newMockStore()
	f := &mockFunder{}

	am := NewManager(s, f)
	defer am.Close()

	host := hosts.Host{
		PublicKey: types.GeneratePrivateKey().PublicKey(),
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "foo"}},
		Usability: goodUsability,
	}

	contractIDs := []types.FileContractID{{1}}
	err := am.FundAccounts(context.Background(), host, contractIDs, zap.NewNop())
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected host not found error")
	}

	// add a host and two accounts
	s.hosts[host.PublicKey] = host
	s.accounts[types.GeneratePrivateKey().PublicKey()] = struct{}{}
	s.accounts[types.GeneratePrivateKey().PublicKey()] = struct{}{}

	// fund accounts
	err = am.FundAccounts(context.Background(), host, contractIDs, zap.NewNop())
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
	if len(s.eas[host.PublicKey]) != 2 {
		t.Fatal("expected two accounts to be updated")
	}
	expected := time.Now().Add(accountFundInterval)
	for _, ea := range s.eas[host.PublicKey] {
		if !ea.NextFund.After(expected.Add(-time.Second)) || !ea.NextFund.Before(expected.Add(time.Second)) {
			t.Fatal("expected next fund to be updated to the next fund interval", ea.NextFund)
		}
	}

	// simulate a couple of failed fund attempts
	f.fail = true
	for range 3 {
		s.resetNextFund()
		err = am.FundAccounts(context.Background(), host, contractIDs, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert the exponential backoff was applied
	expected = time.Now().Add(8 * time.Minute)
	for _, ea := range s.eas[host.PublicKey] {
		if !approxEqual(ea.NextFund, expected, time.Second) {
			t.Fatal("expected next fund to be updated to the exponential backoff", ea.NextFund)
		}
	}

	// reset state
	f.fail = false
	f.calls = f.calls[:0]
	s.resetNextFund()

	// add another 1000 accounts
	for range 1000 {
		s.accounts[types.GeneratePrivateKey().PublicKey()] = struct{}{}
	}

	// fund accounts
	contractIDs = append(contractIDs, types.FileContractID{2})
	err = am.FundAccounts(context.Background(), host, contractIDs, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert batches were applied correctly
	if len(f.calls) != 2 {
		t.Fatal("expected two calls to fund accounts")
	} else if len(f.calls[0].accounts) != accountFundBatch {
		t.Fatal("expected first call to fund 1000 accounts")
	} else if len(f.calls[1].accounts) != 2 {
		t.Fatal("expected second call to fund 2 accounts")
	} else if len(f.calls[0].contractIDs) != 2 {
		t.Fatal("expected first call to have two contract IDs")
	} else if len(f.calls[1].contractIDs) != 1 {
		t.Fatal("expected second call to have one contract ID")
	} else if !f.calls[0].target.Equals(types.Siacoins(1)) {
		t.Fatalf("expected target to be %v, got %v", types.Siacoins(1), f.calls[0].target)
	} else if !f.calls[1].target.Equals(types.Siacoins(1)) {
		t.Fatalf("expected target to be %v, got %v", types.Siacoins(1), f.calls[0].target)
	}

	// assert all accounts next fund was updated and consecutive failed funds was reset
	expected = time.Now().Add(accountFundInterval)
	for _, ea := range s.eas[host.PublicKey] {
		if !approxEqual(ea.NextFund, expected, time.Second) {
			t.Fatal("expected next fund to be updated to the next fund interval", ea.NextFund)
		}
	}

	// assert there's no accounts to fund
	err = am.FundAccounts(context.Background(), host, contractIDs, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(f.calls) != 2 {
		t.Fatal("expected two calls to fund accounts")
	}
}

// TestUpdateFundedAccounts is a unit test that covers the functionality of
// updating the funded accounts. It asserts that the consecutive failed funds
// and next fund time are updated correctly based on the number of funded
// accounts.
func TestUpdateFundedAccounts(t *testing.T) {
	tests := []struct {
		name   string
		accs   []HostAccount
		funded int
		panic  bool
	}{
		{
			name: "all funded",
			accs: []HostAccount{
				{ConsecutiveFailedFunds: 3},
				{ConsecutiveFailedFunds: 5},
			},
			funded: 2,
		},
		{
			name: "none funded",
			accs: []HostAccount{
				{ConsecutiveFailedFunds: 0},
				{ConsecutiveFailedFunds: 1},
			},
			funded: 0,
		},
		{
			name: "partially funded",
			accs: []HostAccount{
				{ConsecutiveFailedFunds: 2},
				{ConsecutiveFailedFunds: 4},
				{ConsecutiveFailedFunds: 0},
			},
			funded: 2,
		},
		{
			name: "sanity check",
			accs: []HostAccount{
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
				updateFundedAccounts(tc.accs, tc.funded)
				return
			}

			updated := slices.Clone(tc.accs)
			updateFundedAccounts(updated, tc.funded)

			for i, acc := range updated {
				// calculate expected values
				var wantConsecFailures int
				var wantNextFund time.Time
				if i < tc.funded {
					wantConsecFailures = 0
					wantNextFund = time.Now().Add(accountFundInterval)
				} else {
					wantConsecFailures = tc.accs[i].ConsecutiveFailedFunds + 1
					wantNextFund = time.Now().Add(time.Duration(min(math.Pow(2, float64(wantConsecFailures)), accountExpBackoffMaxMinutes)) * time.Minute)
				}

				// assert updates
				if acc.ConsecutiveFailedFunds != wantConsecFailures {
					t.Fatal("unexpected consecutive failed funds", acc.ConsecutiveFailedFunds, wantConsecFailures)
				} else if !approxEqual(acc.NextFund, wantNextFund, time.Second) {
					t.Fatal("unexpected next fund", acc.NextFund, wantNextFund)
				}
			}
		})
	}
}

// approxEqual checks if two time.Time values are within a given tolerance
func approxEqual(t1, t2 time.Time, tol time.Duration) bool {
	diff := t1.Sub(t2)
	if diff < 0 {
		diff = -diff
	}
	return diff <= tol
}
