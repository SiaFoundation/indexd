package contracts

import (
	"context"
	"errors"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type funderDialerMock struct {
	clients map[types.PublicKey]FunderHostClient
}

func (d *funderDialerMock) DialHost(ctx context.Context, hk types.PublicKey, addrs []chain.NetAddress) (FunderHostClient, error) {
	if client, ok := d.clients[hk]; ok {
		return client, nil
	}
	return &funderHostClientMock{}, nil
}

type (
	funderHostClientMock struct {
		results map[types.FileContractID]funderRpcResult
	}

	funderRpcResult struct {
		res    rhp.RPCReplenishAccountsResult
		funded int
		err    error
	}
)

func (*funderHostClientMock) Close() error { return nil }

func (h *funderHostClientMock) ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (rhp.RPCReplenishAccountsResult, int, error) {
	res, ok := h.results[contractID]
	if !ok {
		panic("unexpected contract ID in mock")
	}
	return res.res, res.funded, res.err
}

// TestFunder is a unit test that checks the various edge cases in FundAccounts
func TestFunder(t *testing.T) {
	// prepare funder
	dialer := &funderDialerMock{clients: make(map[types.PublicKey]FunderHostClient)}
	f := &Funder{dialer: dialer}

	// prepare accounts
	accs := []accounts.HostAccount{
		{AccountKey: proto.Account{1}},
		{AccountKey: proto.Account{2}},
		{AccountKey: proto.Account{3}},
	}

	host := hosts.Host{PublicKey: types.PublicKey{1}}
	target := types.Siacoins(1)

	// prepare results to cover all possible branches in FundAccounts
	hc := &funderHostClientMock{results: make(map[types.FileContractID]funderRpcResult)}
	hc.results[types.FileContractID{1}] = funderRpcResult{err: client.ErrContractInsufficientFunds}
	hc.results[types.FileContractID{2}] = funderRpcResult{err: client.ErrContractNotRevisable}
	hc.results[types.FileContractID{3}] = funderRpcResult{err: errors.New("failed to replenish accounts")}
	hc.results[types.FileContractID{4}] = funderRpcResult{
		res:    rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}}},
		funded: 1,
	}
	hc.results[types.FileContractID{5}] = funderRpcResult{
		res:    rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}},
		funded: 1,
	}
	hc.results[types.FileContractID{6}] = hc.results[types.FileContractID{5}]
	hc.results[types.FileContractID{7}] = funderRpcResult{
		res:    rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}},
		funded: len(accs) - 1,
	}
	dialer.clients[host.PublicKey] = hc

	// assert contract is marked as drained if it is out of funds
	funded, drained, err := f.FundAccounts(context.Background(), host, []types.FileContractID{{1}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained)
	}

	// assert contract is marked as drained if it is not revisable
	funded, drained, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{2}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained)
	}

	// assert contract is not marked as drained if replenish RPC fails
	funded, drained, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{3}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 0 {
		t.Fatal("expected no drained contracts, got", drained)
	}

	// assert contract is marked as drained if replenish RPC succeeds but leaves the contract with insufficient funds afterwards
	funded, drained, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{4}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 1 {
		t.Fatal("expected 1 funded account, got", funded)
	} else if drained != 1 {
		t.Fatal("expected drained 1 contract, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of contracts
	funded, drained, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{5}, {6}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 2 {
		t.Fatal("expected 2 funded accounts, got", funded)
	} else if drained != 0 {
		t.Fatal("expected no drained contracts, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of accounts
	funded, drained, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{7}, {1}, {5}, {4}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 3 {
		t.Fatal("expected 3 funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained) // both 1 and 4 would be drained, were it not we ran out of accounts to replenish
	}
}
