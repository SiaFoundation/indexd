package contracts

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type fundAccountsCall struct {
	host        hosts.Host
	contractIDs []types.FileContractID
}

type accountsManagerMock struct {
	mu    sync.Mutex
	calls []fundAccountsCall
}

func (am *accountsManagerMock) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, log *zap.Logger) error {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.calls = append(am.calls, fundAccountsCall{
		host:        host,
		contractIDs: contractIDs,
	})
	return nil
}

func (s *storeMock) ContractsForBroadcasting(_ context.Context, minBroadcast time.Time, limit int) ([]types.FileContractID, error) {
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

func (s *storeMock) ContractsForFunding(_ context.Context, hk types.PublicKey, limit int) ([]types.FileContractID, error) {
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

func TestPerformAccountFunding(t *testing.T) {
	amMock := &accountsManagerMock{}
	store := newStoreMock()
	cm := newContractManager(types.PublicKey{}, amMock, nil, store, nil, nil, nil, nil)

	// fund accounts
	err := cm.performAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were no calls, as there are no contracts
	if len(amMock.calls) != 0 {
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

	// fund accounts
	err = cm.performAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were two calls, one for each usable host
	if len(amMock.calls) != 2 {
		t.Fatal("unexpected")
	}
	call1 := amMock.calls[0]
	call2 := amMock.calls[1]
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
