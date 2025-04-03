package accounts

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const dialTimeout = 10 * time.Second

type (
	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		target types.Currency
		cm     *chain.Manager
		signer rhp.ContractSigner
	}
)

// NewFunder creates a new Funder.
func NewFunder(cm *chain.Manager, signer rhp.ContractSigner, target types.Currency) *Funder {
	return &Funder{
		cm:     cm,
		signer: signer,
		target: target,
	}
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. The number returned is the amount of accounts
// that got funded, the accounts are funded in order.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, accounts []HostAccount, contractIDs []types.FileContractID, log *zap.Logger) (int, error) {
	// dial host
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	t, err := siamux.Dial(dialCtx, host.SiamuxAddr(), host.PublicKey)
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
		rev, err := rhp.RPCLatestRevision(ctx, t, fcid)
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
		_, err = rhp.RPCReplenishAccounts(ctx, t, params, f.cm.TipState(), f.signer)
		if err != nil {
			log.Debug("failed to replenish accounts", zap.Error(err))
			continue
		}

		// update funded ix
		fundedIdx += len(batch)
	}

	return fundedIdx, nil
}
