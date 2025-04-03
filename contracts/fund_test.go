package contracts

import (
	"context"
	"sync"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

type fundAccountsCall struct {
	hk          types.PublicKey
	contractIDs []types.FileContractID
}

type accountsManagerMock struct {
	mu    sync.Mutex
	calls []fundAccountsCall
}

func (am *accountsManagerMock) FundAccounts(ctx context.Context, hk types.PublicKey, contractIDs []types.FileContractID, log *zap.Logger) (proto.Usage, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.calls = append(am.calls, fundAccountsCall{
		hk:          hk,
		contractIDs: contractIDs,
	})
	return proto.Usage{}, nil
}

func TestPerformAccountFunding(t *testing.T) {
	amMock := &accountsManagerMock{}
	store := &storeMock{}
	cm := newContractManager(types.PublicKey{}, amMock, nil, nil, nil, store, nil, nil)

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

	// add h1 with one contract
	hk2 := types.PublicKey{2}
	store.contracts = append(store.contracts, Contract{
		ID:                 types.FileContractID{3},
		HostKey:            hk2,
		RemainingAllowance: types.Siacoins(1),
	})

	// fund accounts
	err = cm.performAccountFunding(context.Background(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// assert there were two calls, one for each host
	if len(amMock.calls) != 2 {
		t.Fatal("unexpected")
	}
	call1 := amMock.calls[0]
	call2 := amMock.calls[1]
	if call1.hk != hk1 {
		call1, call2 = call2, call1
	}
	if call1.hk != hk1 {
		t.Fatal("unexpected host key")
	} else if call1.contractIDs[0] != (types.FileContractID{2}) {
		t.Fatal("unexpected contract ID")
	} else if call1.contractIDs[1] != (types.FileContractID{1}) {
		t.Fatal("unexpected contract ID")
	}
	if call2.hk != hk2 {
		t.Fatal("unexpected host key")
	} else if call2.contractIDs[0] != (types.FileContractID{3}) {
		t.Fatal("unexpected contract ID")
	}
}
