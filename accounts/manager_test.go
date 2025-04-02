package accounts

import (
	"context"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
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

func (s *mockStore) resetNextFund() {
	for _, eas := range s.eas {
		for _, ea := range eas {
			ea.NextFund = time.Now()
		}
	}
}

type fundAccountCall struct {
	hk          types.PublicKey
	addr        string
	accounts    []HostAccount
	contractIDs []types.FileContractID
}

type mockFunder struct {
	calls []fundAccountCall
	fail  bool
}

func (f *mockFunder) FundAccounts(ctx context.Context, hk types.PublicKey, addr string, accounts []HostAccount, contractIDs []types.FileContractID, log *zap.Logger) (FundResult, error) {
	f.calls = append(f.calls, fundAccountCall{
		hk:          hk,
		addr:        addr,
		accounts:    accounts,
		contractIDs: contractIDs,
	})
	result := newFundResult(accounts)
	for i := range accounts {
		result.Funded[i] = !f.fail
	}
	if !f.fail {
		result.Usage = proto.Usage{AccountFunding: types.NewCurrency64(uint64(len(accounts)))}
	}
	return result, nil
}

// TestAccountManager is a unit test that covers the functionality of the
// account manager. It asserts FundAccounts fetches and updates accounts for
// funding and the returned usage is calculated correctly.
func TestAccountManager(t *testing.T) {
	s := newMockStore()
	f := &mockFunder{}

	am := NewManager(s, f)
	defer am.Close()

	hk := types.GeneratePrivateKey().PublicKey()
	_, err := am.FundAccounts(context.Background(), hk, nil, zap.NewNop())
	if !errors.Is(err, hosts.ErrNotFound) {
		t.Fatal("expected host not found error")
	}

	// assertInRange is a helper function that asserts the time x is within the
	// range of y ± d
	assertInRange := func(x, y time.Time, d time.Duration) {
		t.Helper()
		if !x.After(y.Add(-d)) || !x.Before(y.Add(d)) {
			t.Fatal("expected time to be in range", x, y)
		}
	}

	// add a host and two accounts
	s.hosts[hk] = hosts.Host{PublicKey: hk, Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "foo"}}}
	s.accounts[types.GeneratePrivateKey().PublicKey()] = struct{}{}
	s.accounts[types.GeneratePrivateKey().PublicKey()] = struct{}{}

	// fund accounts
	_, err = am.FundAccounts(context.Background(), hk, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert the call params
	if len(f.calls) != 1 {
		t.Fatal("expected one call to fund accounts")
	} else if f.calls[0].hk != hk {
		t.Fatal("expected host key to match")
	} else if f.calls[0].addr != "foo" {
		t.Fatal("expected address to match")
	} else if len(f.calls[0].accounts) != 2 {
		t.Fatal("expected two accounts to be funded")
	}

	// assert the accounts were updated
	if len(s.eas[hk]) != 2 {
		t.Fatal("expected two accounts to be updated")
	}
	expected := time.Now().Add(accountFundInterval)
	for _, ea := range s.eas[hk] {
		if !ea.NextFund.After(expected.Add(-time.Millisecond)) || !ea.NextFund.Before(expected.Add(time.Millisecond)) {
			t.Fatal("expected next fund to be updated to the next fund interval", ea.NextFund)
		}
	}

	// simulate a couple of failed fund attempts
	f.fail = true
	for range 3 {
		s.resetNextFund()
		_, err = am.FundAccounts(context.Background(), hk, nil, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert the exponential backoff was applied
	expected = time.Now().Add(8 * time.Minute)
	for _, ea := range s.eas[hk] {
		assertInRange(ea.NextFund, expected, time.Second)
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
	usage, err := am.FundAccounts(context.Background(), hk, nil, zap.NewNop())
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
	}

	// assert usage was aggregated correctly
	if usage.AccountFunding.Cmp(types.NewCurrency64(1002)) != 0 {
		t.Fatal("expected usage to be 1002")
	}

	// assert all accounts next fund was updated and consecutive failed funds was reset
	expected = time.Now().Add(accountFundInterval)
	for _, ea := range s.eas[hk] {
		assertInRange(ea.NextFund, expected, time.Second)
	}

	// assert there's no accounts to fund
	_, err = am.FundAccounts(context.Background(), hk, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if len(f.calls) != 2 {
		t.Fatal("expected two calls to fund accounts")
	}
}
