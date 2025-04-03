package accounts

import (
	"context"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const dialTimeout = 10 * time.Second

type (
	// ChainManager is the minimal interface of ChainManager functionality the
	// Funder requires.
	ChainManager interface {
		TipState() consensus.State
	}

	// HostClient defines the interface for the funder to interact with the
	// host.
	HostClient interface {
		Dial(context.Context, string, types.PublicKey) (rhp.TransportClient, error)
		RPCLatestRevision(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error)
		RPCReplenishAccounts(context.Context, rhp.TransportClient, rhp.RPCReplenishAccountsParams, consensus.State, rhp.ContractSigner) (rhp.RPCReplenishAccountsResult, error)
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		cm     ChainManager
		host   HostClient
		signer rhp.ContractSigner
		target types.Currency
	}
)

type hostClient struct{}

func (c *hostClient) Dial(ctx context.Context, addr string, peerKey types.PublicKey) (rhp.TransportClient, error) {
	return siamux.Dial(ctx, addr, peerKey)
}

func (c *hostClient) RPCLatestRevision(ctx context.Context, t rhp.TransportClient, fcid types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return rhp.RPCLatestRevision(ctx, t, fcid)
}

func (c *hostClient) RPCReplenishAccounts(ctx context.Context, t rhp.TransportClient, params rhp.RPCReplenishAccountsParams, state consensus.State, signer rhp.ContractSigner) (rhp.RPCReplenishAccountsResult, error) {
	return rhp.RPCReplenishAccounts(ctx, t, params, state, signer)
}

// NewFunder creates a new Funder.
func NewFunder(cm ChainManager, signer rhp.ContractSigner, target types.Currency) *Funder {
	return &Funder{
		cm:     cm,
		signer: signer,
		target: target,
		host:   &hostClient{},
	}
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. The number returned is the amount of accounts
// that got funded, the accounts are funded in order.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, accounts []HostAccount, contractIDs []types.FileContractID, log *zap.Logger) (int, error) {
	// dial host
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	t, err := f.host.Dial(dialCtx, host.SiamuxAddr(), host.PublicKey)
	cancel()
	if err != nil {
		log.Debug("failed to dial host", zap.Error(err))
		return 0, nil
	}
	defer t.Close()

	// iterate over contracts
	var fundedIdx int
	for _, fcid := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", fcid))

		// fetch the latest revision, check it's revisable and has money
		rev, err := f.host.RPCLatestRevision(ctx, t, fcid)
		if err != nil {
			contractLog.Debug("failed to fetch latest revision", zap.Error(err))
			continue
		} else if !rev.Revisable {
			contractLog.Debug("contract is not revisable") // sanity check
			continue
		} else if rev.Contract.RenterOutput.Value.IsZero() {
			contractLog.Debug("contract is out of funds")
			continue
		} else if rev.Contract.RenterOutput.Value.Cmp(f.target) < 0 {
			contractLog.Debug("contract has insufficient funds")
			continue
		}
		balance := rev.Contract.RenterOutput.Value

		// prepare accounts batch
		var batch []proto.Account
		for i := fundedIdx; i < len(accounts); i++ {
			var underflow bool
			balance, underflow = balance.SubWithUnderflow(f.target)
			if underflow {
				break
			}

			batch = append(batch, accounts[i].AccountKey)
		}
		if len(batch) == 0 {
			continue
		}

		// prepare replenish RPC params
		revision := rhp.ContractRevision{ID: fcid, Revision: rev.Contract}
		params := rhp.RPCReplenishAccountsParams{
			Accounts: batch,
			Target:   f.target,
			Contract: revision,
		}

		// execute replenish RPC
		_, err = f.host.RPCReplenishAccounts(ctx, t, params, f.cm.TipState(), f.signer)
		if err != nil {
			log.Debug("failed to replenish accounts", zap.Error(err))
			continue
		}

		// update funded ix
		fundedIdx += len(batch)
	}

	return fundedIdx, nil
}
