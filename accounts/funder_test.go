package accounts

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

var badContractId = types.FileContractID{1, 1, 1}

type tcMock struct{}

func (tcMock) DialStream() (net.Conn, error) { return nil, nil }
func (tcMock) FrameSize() int                { return 0 }
func (tcMock) PeerKey() types.PublicKey      { return types.PublicKey{} }
func (tcMock) Close() error                  { return nil }

type cmMock struct{}

func (cmMock) TipState() consensus.State { return consensus.State{} }

type hostMock struct {
	revisions map[types.FileContractID]proto.RPCLatestRevisionResponse
	calls     []rhp.RPCReplenishAccountsParams
}

func (hostMock) Dial(ctx context.Context, addr string, pk types.PublicKey) (rhp.TransportClient, error) {
	return &tcMock{}, nil
}

func (h *hostMock) RPCLatestRevision(ctx context.Context, t rhp.TransportClient, fcid types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	if rev, ok := h.revisions[fcid]; ok {
		return rev, nil
	}
	return proto.RPCLatestRevisionResponse{}, errors.New("unknown contract")
}

func (h *hostMock) RPCReplenishAccounts(_ context.Context, _ rhp.TransportClient, params rhp.RPCReplenishAccountsParams, _ consensus.State, _ rhp.ContractSigner) (rhp.RPCReplenishAccountsResult, error) {
	if params.Contract.ID == badContractId {
		return rhp.RPCReplenishAccountsResult{}, errors.New("failed to replenish")
	} else if _, ok := h.revisions[params.Contract.ID]; !ok {
		return rhp.RPCReplenishAccountsResult{}, errors.New("unknown contract")
	}

	h.calls = append(h.calls, params)

	var underflow bool
	rev := params.Contract.Revision
	rev.RenterOutput.Value, underflow = rev.RenterOutput.Value.SubWithUnderflow(params.Target.Mul64(uint64(len(params.Accounts))))
	if underflow {
		return rhp.RPCReplenishAccountsResult{}, errors.New("insufficient funds")
	}

	return rhp.RPCReplenishAccountsResult{Revision: rev}, nil
}

// TestFunder is a unit test that checks the various edge cases in FundAccounts
func TestFunder(t *testing.T) {
	// prepare mock host
	target := types.Siacoins(1)
	h := &hostMock{revisions: map[types.FileContractID]proto.RPCLatestRevisionResponse{
		{1}: {Revisable: false},                                                                                                              // not revisable
		{2}: {Revisable: true, Contract: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}}}, // insufficient funds
	}}

	// prepare funder
	f := NewFunder(&cmMock{}, nil, target)
	f.host = h

	// assert contract checks
	core, logs := observer.New(zapcore.DebugLevel)
	contractIDs := []types.FileContractID{{}, {1}, {2}}
	funded, drained, err := f.FundAccounts(context.Background(), hosts.Host{}, nil, contractIDs, zap.New(core))
	if err != nil {
		t.Fatal("unexpected", err)
	} else if funded != 0 {
		t.Fatal("expected 0 accounts funded, got", funded)
	} else if drained != 2 {
		t.Fatal("expected 2 contracts drained, got", drained)
	} else if entries := logs.TakeAll(); len(entries) != 3 {
		t.Fatal("expected 3 log entries, got", len(entries))
	} else if !strings.Contains(entries[0].Message, "latest revision") {
		t.Fatalf("expected 'latest revision', got %q", entries[0].Message)
	} else if !strings.Contains(entries[1].Message, "not revisable") {
		t.Fatalf("expected 'not revisable', got %q", entries[1].Message)
	} else if !strings.Contains(entries[2].Message, "insufficient funds") {
		t.Fatalf("expected 'insufficient funds', got %q", entries[2].Message)
	}

	// add a good contract, capable of funding two accounts
	h.revisions[types.FileContractID{3}] = proto.RPCLatestRevisionResponse{
		Revisable: true,
		Contract:  types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Mul64(2)}},
	}

	// add a bad contract, that fails RPC replenish (to assert we don't increment fundIdx)
	h.revisions[badContractId] = proto.RPCLatestRevisionResponse{
		Revisable: true,
		Contract:  types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}},
	}

	// add a good contract, capable of funding 1 account
	h.revisions[types.FileContractID{4}] = proto.RPCLatestRevisionResponse{
		Revisable: true,
		Contract:  types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Mul64(1)}},
	}

	contractIDs = []types.FileContractID{{3}, badContractId, {4}}
	accounts := []HostAccount{{AccountKey: proto.Account{1}}, {AccountKey: proto.Account{2}}, {AccountKey: proto.Account{3}}, {AccountKey: proto.Account{4}}}
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, accounts, contractIDs, zap.NewNop())
	if err != nil {
		t.Fatal("unexpected", err)
	} else if funded != 3 {
		t.Fatal("expected 3 accounts funded, got", funded)
	} else if drained != 2 {
		t.Fatal("expected 2 contracts drained, got", drained)
	} else if len(h.calls) != 2 {
		t.Fatal("expected 2 replenish calls, got", len(h.calls))
	} else if len(h.calls[0].Accounts) != 2 {
		t.Fatal("expected first batch to contain 2 accounts, got", len(h.calls[0].Accounts))
	} else if h.calls[0].Accounts[0] != accounts[0].AccountKey {
		t.Fatal("expected first account to be funded, got", h.calls[0].Accounts[0])
	} else if h.calls[0].Accounts[1] != accounts[1].AccountKey {
		t.Fatal("expected second account to be funded, got", h.calls[0].Accounts[1])
	} else if len(h.calls[1].Accounts) != 1 {
		t.Fatal("expected second batch to contain 1 account, got", len(h.calls[1].Accounts))
	} else if h.calls[1].Accounts[0] != accounts[2].AccountKey {
		t.Fatal("expected third account to be funded, got", h.calls[1].Accounts[0])
	}
}
