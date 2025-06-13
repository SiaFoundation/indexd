package accounts

import (
	"context"
	"errors"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type dialerMock struct{}

func (*dialerMock) DialHost(ctx context.Context, hk types.PublicKey, addr string) (HostClient, error) {
	return &hostClientMock{}, nil
}

type hostClientMock struct{}

func (*hostClientMock) Close() error { return nil }

func (*hostClientMock) LatestRevision(context.Context, types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return proto.RPCLatestRevisionResponse{}, nil
}

func (*hostClientMock) ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (rhp.RPCReplenishAccountsResult, int, error) {
	// use contract ID to cover all possible branches
	switch contractID {
	case types.FileContractID{1}:
		return rhp.RPCReplenishAccountsResult{}, 0, client.ErrContractInsufficientFunds
	case types.FileContractID{2}:
		return rhp.RPCReplenishAccountsResult{}, 0, client.ErrContractNotRevisable
	case types.FileContractID{3}:
		return rhp.RPCReplenishAccountsResult{}, 0, errors.New("failed to replenish accounts")
	case types.FileContractID{4}:
		return rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}}}, 1, nil
	case types.FileContractID{5}, types.FileContractID{6}:
		return rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}}, 1, nil
	case types.FileContractID{7}:
		return rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}}, len(accounts) - 1, nil
	default:
		panic("unexpected contract ID in mock")
	}
}

// TestFunder is a unit test that checks the various edge cases in FundAccounts
func TestFunder(t *testing.T) {
	// prepare funder
	f := &Funder{dialer: &dialerMock{}}

	// prepare accounts
	accounts := []HostAccount{
		{AccountKey: proto.Account{1}},
		{AccountKey: proto.Account{2}},
		{AccountKey: proto.Account{3}},
	}

	// assert contract is marked as drained if it is out of funds
	funded, drained, err := f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{1}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained)
	}

	// assert contract is marked as drained if it is not revisable
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{2}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained)
	}

	// assert contract is not marked as drained if replenish RPC fails
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{3}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 0 {
		t.Fatal("expected no drained contracts, got", drained)
	}

	// assert contract is marked as drained if replenish RPC succeeds but leaves the contract with insufficient funds afterwards
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{4}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 1 {
		t.Fatal("expected 1 funded account, got", funded)
	} else if drained != 1 {
		t.Fatal("expected drained 1 contract, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of contracts
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{5}, {6}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 2 {
		t.Fatal("expected 2 funded accounts, got", funded)
	} else if drained != 0 {
		t.Fatal("expected no drained contracts, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of accounts
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{7}, {1}, {5}, {4}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 3 {
		t.Fatal("expected 3 funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained) // both 1 and 4 would be drained, were it not we ran out of accounts to replenish
	}
}
