package contracts

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type fundAccountsCall struct {
	host        hosts.Host
	contractIDs []types.FileContractID
}

type accountsManagerMock struct {
	mu             sync.Mutex
	activeAccounts uint64
	accountsToFund []accounts.HostAccount
}

func (am *accountsManagerMock) AccountsForFunding(hk types.PublicKey, threshold time.Time, limit int) ([]accounts.HostAccount, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	cpy := make([]accounts.HostAccount, len(am.accountsToFund))
	copy(cpy, am.accountsToFund)
	return cpy, nil
}

func (am *accountsManagerMock) ActiveAccounts(threshold time.Time) (uint64, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.activeAccounts, nil
}

func (am *accountsManagerMock) ScheduleAccountsForFunding(hostKey types.PublicKey) error {
	return nil
}

func (am *accountsManagerMock) ServiceAccounts(hk types.PublicKey) []accounts.HostAccount {
	return nil
}

func (am *accountsManagerMock) UpdateHostAccounts(accs []accounts.HostAccount) error {
	return nil
}

func (am *accountsManagerMock) UpdateServiceAccounts(ctx context.Context, accs []accounts.HostAccount, balance types.Currency) error {
	return nil
}

type accountFunderMock struct {
	mu    sync.Mutex
	calls []fundAccountsCall
}

func (f *accountFunderMock) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accs []accounts.HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fundAccountsCall{
		host:        host,
		contractIDs: contractIDs,
	})
	return len(accs), 0, nil
}

func (s *storeMock) ContractsForBroadcasting(minBroadcast time.Time, limit int) ([]types.FileContractID, error) {
	var contracts []Contract
	for _, c := range s.contracts {
		if c.RenewedTo == (types.FileContractID{}) &&
			(c.State == ContractStatePending || c.State == ContractStateActive) &&
			c.LastBroadcastAttempt.Before(minBroadcast) {
			contracts = append(contracts, c)
		}
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].LastBroadcastAttempt.Before(contracts[j].LastBroadcastAttempt)
	})

	out := make([]types.FileContractID, len(contracts))
	for i, c := range contracts {
		out[i] = c.ID
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *storeMock) ContractsForFunding(hk types.PublicKey, limit int) ([]types.FileContractID, error) {
	var contracts []Contract
	for _, c := range s.contracts {
		if c.HostKey == hk && !c.RemainingAllowance.IsZero() {
			contracts = append(contracts, c)
		}
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].RemainingAllowance.Cmp(contracts[j].RemainingAllowance) > 0
	})

	out := make([]types.FileContractID, len(contracts))
	for i, c := range contracts {
		out[i] = c.ID
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *storeMock) HostsForFunding() ([]types.PublicKey, error) {
	hasContract := make(map[types.PublicKey]struct{})
	for _, c := range s.contracts {
		hasContract[c.HostKey] = struct{}{}
	}

	var hks []types.PublicKey
	for hk, host := range s.hosts {
		if host.Usability == hosts.GoodUsability && !host.Blocked {
			if _, ok := hasContract[hk]; ok {
				hks = append(hks, hk)
			}
		}
	}

	return hks, nil
}

func TestPerformAccountFunding(t *testing.T) {
	amMock := &accountsManagerMock{
		accountsToFund: []accounts.HostAccount{{AccountKey: [32]byte{1}}},
	}
	funderMock := &accountFunderMock{}
	store := newStoreMock()
	hmMock := &hostManagerMock{settings: make(map[types.PublicKey]rhp.HostSettings), store: store}
	cm := newContractManager(types.PublicKey{}, amMock, funderMock, nil, store, nil, hmMock, nil, nil)

	// fund accounts
	err := cm.performAccountFunding(context.Background(), false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were no calls, as there are no contracts
	if len(funderMock.calls) != 0 {
		t.Fatal("unexpected")
	}

	// add h1 with two contracts, c2 has more allowance
	hk1 := types.PublicKey{1}
	store.hosts[hk1] = hosts.Host{
		PublicKey: hk1,
		Usability: hosts.GoodUsability,
	}
	store.contracts = append(store.contracts, Contract{
		ID:                 types.FileContractID{1},
		HostKey:            hk1,
		RemainingAllowance: types.Siacoins(1),
	})
	store.contracts = append(store.contracts, Contract{
		ID:                 types.FileContractID{2},
		HostKey:            hk1,
		RemainingAllowance: types.Siacoins(2),
	})

	// add h2 with one contract
	hk2 := types.PublicKey{2}
	store.hosts[hk2] = hosts.Host{
		PublicKey: hk2,
		Usability: hosts.GoodUsability,
	}
	store.contracts = append(store.contracts, Contract{
		ID:                 types.FileContractID{3},
		HostKey:            hk2,
		RemainingAllowance: types.Siacoins(1),
	})

	// add h3, which is unusable
	hk3 := types.PublicKey{3}
	store.hosts[hk3] = hosts.Host{PublicKey: hk3}
	store.contracts = append(store.contracts, Contract{
		ID:                 types.FileContractID{4},
		HostKey:            hk3,
		RemainingAllowance: types.Siacoins(1),
	})

	// add h4, which is blocked
	hk4 := types.PublicKey{4}
	store.hosts[hk4] = hosts.Host{PublicKey: hk4, Blocked: true}
	store.contracts = append(store.contracts, Contract{
		ID:                 types.FileContractID{5},
		HostKey:            hk4,
		RemainingAllowance: types.Siacoins(1),
	})

	// give all hosts good settings
	hmMock.settings[hk1] = goodSettings
	hmMock.settings[hk2] = goodSettings
	hmMock.settings[hk3] = goodSettings
	hmMock.settings[hk4] = goodSettings

	// fund accounts
	err = cm.performAccountFunding(context.Background(), false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were two calls, one for each usable host
	if len(funderMock.calls) != 2 {
		t.Fatal("unexpected")
	}
	call1 := funderMock.calls[0]
	call2 := funderMock.calls[1]
	if call1.host.PublicKey != hk1 {
		call1, call2 = call2, call1
	}
	if call1.host.PublicKey != hk1 {
		t.Fatal("unexpected host key")
	} else if call1.contractIDs[0] != (types.FileContractID{2}) {
		t.Fatal("unexpected contract ID")
	} else if call1.contractIDs[1] != (types.FileContractID{1}) {
		t.Fatal("unexpected contract ID")
	}
	if call2.host.PublicKey != hk2 {
		t.Fatal("unexpected host key")
	} else if call2.contractIDs[0] != (types.FileContractID{3}) {
		t.Fatal("unexpected contract ID")
	}
}
