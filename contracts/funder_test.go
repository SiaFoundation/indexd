package contracts

import (
	"context"
	"errors"
	"testing"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

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

func (h *funderHostClientMock) LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return proto.RPCLatestRevisionResponse{}, nil
}
func (h *funderHostClientMock) ReplenishAccounts(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, params rhp.RPCReplenishAccountsParams) (rhp.RPCReplenishAccountsResult, error) {
	res, ok := h.results[params.Contract.ID]
	if !ok {
		panic("unexpected contract ID in mock")
	}
	return res.res, res.err
}

type mockChainManager struct{}

func (cm *mockChainManager) TipState() consensus.State {
	return consensus.State{}
}

func (cm *mockChainManager) V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error) {
	return types.ChainIndex{}, nil, nil
}

type mockStore struct {
	target types.Currency
}

func (s *mockStore) ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error) {
	// contract 7 has enough funds for 2 accounts (len(accs) - 1)
	value := s.target
	if contractID == (types.FileContractID{7}) {
		value = s.target.Mul64(2)
	}
	return rhp.ContractRevision{
		ID: contractID,
		Revision: types.V2FileContract{
			ProofHeight:  10,
			RenterOutput: types.SiacoinOutput{Value: value},
		},
	}, false, nil
}

func (s *mockStore) UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error {
	return nil
}

func (s *mockStore) MarkContractBad(contractID types.FileContractID) error {
	return nil
}

// TestFunder is a unit test that checks the various edge cases in FundAccounts
func TestFunder(t *testing.T) {
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
	hc.results[types.FileContractID{1}] = funderRpcResult{err: ErrContractInsufficientFunds}
	hc.results[types.FileContractID{2}] = funderRpcResult{err: ErrContractNotRevisable}
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

	// prepare funder
	f := NewFunder(hc, nil, &mockChainManager{}, &mockStore{target: target}, zap.NewNop(), WithRevisionSubmissionBuffer(1))

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
