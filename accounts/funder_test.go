package accounts

import (
	"context"
	"errors"
	"net"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type clientMock struct{}

func (c *clientMock) DialStream() (net.Conn, error) { return nil, nil }

func (c *clientMock) FrameSize() int           { return 0 }
func (c *clientMock) PeerKey() types.PublicKey { return types.PublicKey{} }

func (c *clientMock) Close() error { return nil }

type (
	hostClientMock struct {
		results map[types.FileContractID]rpcResult
	}

	rpcResult struct {
		res    rhp4.RPCReplenishAccountsResult
		funded int
		err    error
	}
)

func (*hostClientMock) Close() error { return nil }

func (*hostClientMock) Dial(ctx context.Context, addr string, hk types.PublicKey) (rhp4.TransportClient, error) {
	return &clientMock{}, nil
}

func (*hostClientMock) RPCLatestRevision(ctx context.Context, tc rhp4.TransportClient, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return proto.RPCLatestRevisionResponse{}, nil
}

func (h *hostClientMock) RPCReplenishAccounts(ctx context.Context, tc rhp4.TransportClient, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (rhp4.RPCReplenishAccountsResult, int, error) {
	res, ok := h.results[contractID]
	if !ok {
		panic("unexpected contract ID in mock")
	}
	return res.res, res.funded, res.err
}

// TestFunder is a unit test that checks the various edge cases in FundAccounts
func TestFunder(t *testing.T) {
	// prepare funder
	client := &hostClientMock{results: make(map[types.FileContractID]rpcResult)}
	f := &Funder{client: client}

	// prepare accounts
	accounts := []HostAccount{
		{AccountKey: proto.Account{1}},
		{AccountKey: proto.Account{2}},
		{AccountKey: proto.Account{3}},
	}

	// prepare results to cover all possible branches in FundAccounts
	target := types.Siacoins(1)
	client.results[types.FileContractID{1}] = rpcResult{err: errContractInsufficientFunds}
	client.results[types.FileContractID{2}] = rpcResult{err: errContractNotRevisable}
	client.results[types.FileContractID{3}] = rpcResult{err: errors.New("failed to replenish accounts")}
	client.results[types.FileContractID{4}] = rpcResult{
		res:    rhp4.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}}},
		funded: 1,
	}
	client.results[types.FileContractID{5}] = rpcResult{
		res:    rhp4.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}},
		funded: 1,
	}
	client.results[types.FileContractID{6}] = client.results[types.FileContractID{5}]
	client.results[types.FileContractID{7}] = rpcResult{
		res:    rhp4.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}},
		funded: len(accounts) - 1,
	}

	// assert contract is marked as drained if it is out of funds
	funded, drained, err := f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{1}}, accounts, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained)
	}

	// assert contract is marked as drained if it is not revisable
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{2}}, accounts, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained)
	}

	// assert contract is not marked as drained if replenish RPC fails
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{3}}, accounts, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected no funded accounts, got", funded)
	} else if drained != 0 {
		t.Fatal("expected no drained contracts, got", drained)
	}

	// assert contract is marked as drained if replenish RPC succeeds but leaves the contract with insufficient funds afterwards
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{4}}, accounts, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 1 {
		t.Fatal("expected 1 funded account, got", funded)
	} else if drained != 1 {
		t.Fatal("expected drained 1 contract, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of contracts
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{5}, {6}}, accounts, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 2 {
		t.Fatal("expected 2 funded accounts, got", funded)
	} else if drained != 0 {
		t.Fatal("expected no drained contracts, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of accounts
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{7}, {1}, {5}, {4}}, accounts, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 3 {
		t.Fatal("expected 3 funded accounts, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 drained contract, got", drained) // both 1 and 4 would be drained, were it not we ran out of accounts to replenish
	}
}
