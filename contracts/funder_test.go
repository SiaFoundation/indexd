package contracts

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

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
		revision types.V2FileContract
		funded   int
		err      error
	}
)

func (h *funderHostClientMock) LatestRevision(_ context.Context, _ client.ChainManager, _, _ types.PublicKey, _ types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return proto.RPCLatestRevisionResponse{}, nil
}
func (h *funderHostClientMock) ReplenishAccounts(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, params rhp.RPCReplenishAccountsParams) (rhp.RPCReplenishAccountsResult, error) {
	res, ok := h.results[params.Contract.ID]
	if !ok {
		panic("unexpected contract ID in mock")
	}
	return rhp.RPCReplenishAccountsResult{Revision: res.revision}, res.err
}

func (h *funderHostClientMock) AttachPools(_ context.Context, _ types.PublicKey, _ []rhp.PoolAttachInput, _ time.Duration) error {
	return nil
}

func (h *funderHostClientMock) ReplenishPools(_ context.Context, _ rhp.ContractSigner, _ client.ChainManager, params rhp.RPCReplenishPoolsParams) (rhp.RPCReplenishPoolsResult, error) {
	res, ok := h.results[params.Contract.ID]
	if !ok {
		panic("unexpected contract ID in mock")
	}
	return rhp.RPCReplenishPoolsResult{Revision: res.revision}, res.err
}

type mockChainManager struct{}

func (cm *mockChainManager) Block(id types.BlockID) (types.Block, bool) {
	return types.Block{}, false
}

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
		revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}},
		funded:   1,
	}
	hc.results[types.FileContractID{5}] = funderRpcResult{
		revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}},
		funded:   1,
	}
	hc.results[types.FileContractID{6}] = hc.results[types.FileContractID{5}]
	hc.results[types.FileContractID{7}] = funderRpcResult{
		revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}},
		funded:   len(accs) - 1,
	}

	// prepare funder
	rev := NewRevisionManager(hc, &mockChainManager{}, &mockStore{target: target}, 1, zap.NewNop())
	cl := NewContractLocker()
	f := NewFunder(hc, cl, rev, nil, &mockChainManager{}, zap.NewNop())

	// assert contract is marked as drained if it is out of funds
	res, err := f.FundAccounts(context.Background(), host, []types.FileContractID{{1}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if res.Funded != 0 {
		t.Fatal("expected no funded accounts, got", res.Funded)
	} else if !slices.Equal(res.Drained, []types.FileContractID{{1}}) {
		t.Fatal("expected contract 1 to be drained, got", res.Drained)
	}

	// assert contract is marked as drained if it is not revisable
	res, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{2}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if res.Funded != 0 {
		t.Fatal("expected no funded accounts, got", res.Funded)
	} else if !slices.Equal(res.Drained, []types.FileContractID{{2}}) {
		t.Fatal("expected contract 2 to be drained, got", res.Drained)
	}

	// assert contract is not marked as drained if replenish RPC fails
	res, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{3}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if res.Funded != 0 {
		t.Fatal("expected no funded accounts, got", res.Funded)
	} else if len(res.Drained) != 0 {
		t.Fatal("expected no drained contracts, got", res.Drained)
	}

	// assert contract is marked as drained if replenish RPC succeeds but leaves the contract with insufficient funds afterwards
	res, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{4}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if res.Funded != 1 {
		t.Fatal("expected 1 funded account, got", res.Funded)
	} else if !slices.Equal(res.Drained, []types.FileContractID{{4}}) {
		t.Fatal("expected contract 4 to be drained, got", res.Drained)
	}

	// assert contracts are iterated and funded is updated until we run out of contracts
	res, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{5}, {6}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if res.Funded != 2 {
		t.Fatal("expected 2 funded accounts, got", res.Funded)
	} else if len(res.Drained) != 0 {
		t.Fatal("expected no drained contracts, got", res.Drained)
	}

	// assert contracts are iterated and funded is updated until we run out of accounts
	res, err = f.FundAccounts(context.Background(), host, []types.FileContractID{{7}, {1}, {5}, {4}}, accs, target, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if res.Funded != 3 {
		t.Fatal("expected 3 funded accounts, got", res.Funded)
	} else if !slices.Equal(res.Drained, []types.FileContractID{{1}}) {
		t.Fatal("expected contract 1 to be drained, got", res.Drained) // both 1 and 4 would be drained, were it not we ran out of accounts to replenish
	}
}

// TestFunderLockedContracts asserts that FundAccounts and FundPools only report
// the unfunded accounts or pools as skipped when locked contracts are the only
// reason they were left unfunded.
func TestFunderLockedContracts(t *testing.T) {
	host := hosts.Host{PublicKey: types.PublicKey{1}}
	target := types.Siacoins(1)
	accs := []accounts.HostAccount{
		{AccountKey: proto.Account{1}},
		{AccountKey: proto.Account{2}},
	}
	pools := []accounts.HostPool{
		{PoolKey: types.GeneratePrivateKey()},
		{PoolKey: types.GeneratePrivateKey()},
	}

	// contract 1 is locked, contract 3 fails, contract 4 funds one account
	// and is drained, contract 5 funds one account and contract 7 funds both
	hc := &funderHostClientMock{results: make(map[types.FileContractID]funderRpcResult)}
	hc.results[types.FileContractID{3}] = funderRpcResult{err: errors.New("failed to replenish")}
	hc.results[types.FileContractID{4}] = funderRpcResult{revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}}}
	hc.results[types.FileContractID{5}] = funderRpcResult{revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}}
	hc.results[types.FileContractID{7}] = hc.results[types.FileContractID{5}]

	// prepare funder
	rev := NewRevisionManager(hc, &mockChainManager{}, &mockStore{target: target}, 1, zap.NewNop())
	cl := NewContractLocker()
	_, unlock := cl.LockContract(types.FileContractID{1})
	defer unlock()
	f := NewFunder(hc, cl, rev, nil, &mockChainManager{}, zap.NewNop())

	for _, tc := range []struct {
		name        string
		contractIDs []types.FileContractID
		funded      int
		drained     []types.FileContractID
		skipped     bool
	}{
		{
			name:        "funded then locked",
			contractIDs: []types.FileContractID{{5}, {1}},
			funded:      1,
			skipped:     true,
		},
		{
			name:        "locked then funded",
			contractIDs: []types.FileContractID{{1}, {7}},
			funded:      2,
		},
		{
			name:        "locked and drained",
			contractIDs: []types.FileContractID{{1}, {4}},
			funded:      1,
			drained:     []types.FileContractID{{4}},
			skipped:     true,
		},
		{
			name:        "locked and replenish failure",
			contractIDs: []types.FileContractID{{1}, {3}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := func(t *testing.T, kind string, res FundResult) {
				t.Helper()
				if res.Funded != tc.funded {
					t.Fatalf("expected %d funded %s, got %d", tc.funded, kind, res.Funded)
				} else if !slices.Equal(res.Drained, tc.drained) {
					t.Fatalf("expected drained contracts %v when funding %s, got %v", tc.drained, kind, res.Drained)
				} else if res.Skipped != tc.skipped {
					t.Fatalf("expected skipped to be %v when funding %s, got %v", tc.skipped, kind, res.Skipped)
				}
			}

			res, err := f.FundAccounts(context.Background(), host, tc.contractIDs, accs, target, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			check(t, "accounts", res)

			res, err = f.FundPools(context.Background(), host, tc.contractIDs, pools, target, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			check(t, "pools", res)
		})
	}
}
