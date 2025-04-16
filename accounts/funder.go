package accounts

import (
	"context"
	"errors"
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
// specified contracts in order. The given accounts should not exceed the batch
// size used in the replenish RPC. This method returns two numbers, the first
// one indicates the number of accounts that were funded, the second indicates
// the number of contracts that were drained. Consecutive calls for the same
// host should take this into account and adjust the contract IDs that are being
// passed in.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, accounts []HostAccount, contractIDs []types.FileContractID, log *zap.Logger) (funded int, drained int, _ error) {
	// sanity check
	if len(accounts) > proto.MaxAccountBatchSize {
		return 0, 0, errors.New("too many accounts") // developer error
	}

	// dial host
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	t, err := f.host.Dial(dialCtx, host.SiamuxAddr(), host.PublicKey)
	cancel()
	if err != nil {
		log.Debug("failed to dial host", zap.Error(err))
		return 0, 0, nil
	}
	defer t.Close()

	// prepare account keys
	accountKeys := make([]proto.Account, len(accounts))
	for i, account := range accounts {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, fcid := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", fcid))

		// fetch the latest revision, check it's revisable and has money
		rev, err := f.host.RPCLatestRevision(ctx, t, fcid)
		if err != nil {
			contractLog.Debug("failed to fetch latest revision", zap.Error(err))
			continue
		} else if !rev.Revisable {
			contractLog.Debug("contract is not revisable") // sanity check
			drained++
			continue
		} else if rev.Contract.RenterOutput.Value.Cmp(f.target) < 0 {
			contractLog.Debug("contract has insufficient funds")
			drained++
			continue
		}

		// prepare batch
		batchSize := int(min(rev.Contract.RenterOutput.Value.Div(f.target).Big().Uint64(), proto.MaxAccountBatchSize))
		batchEndIdx := min(batchSize+funded, len(accounts))

		// prepare replenish RPC params
		revision := rhp.ContractRevision{ID: fcid, Revision: rev.Contract}
		params := rhp.RPCReplenishAccountsParams{
			Accounts: accountKeys[funded:batchEndIdx],
			Target:   f.target,
			Contract: revision,
		}

		// execute replenish RPC
		res, err := f.host.RPCReplenishAccounts(ctx, t, params, f.cm.TipState(), f.signer)
		if err != nil {
			contractLog.Debug("failed to replenish accounts", zap.Error(err))
			continue
		} else if res.Revision.RenterOutput.Value.Cmp(f.target) < 0 {
			contractLog.Debug("contract was drained by replenish RPC")
			drained++
		}

		// update funded ix
		funded = batchEndIdx
		if funded == len(accounts) {
			break
		}
	}

	return funded, drained, nil
}
