package accounts

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
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

	// FundResult is the result of a funding operation.
	FundResult struct {
		Funded []bool
		Usage  proto.Usage
	}
)

func newFundResult(accounts []HostAccount) FundResult {
	return FundResult{
		Funded: make([]bool, len(accounts)),
		Usage:  proto.Usage{},
	}
}

// NewFunder creates a new Funder.
func NewFunder(cm *chain.Manager, signer rhp.ContractSigner, target types.Currency) *Funder {
	return &Funder{
		cm:     cm,
		signer: signer,
		target: target,
	}
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. It returns a result object that contains a bool
// to indicate if the account was funded as well as the total usage.
func (f *Funder) FundAccounts(ctx context.Context, hk types.PublicKey, addr string, accounts []HostAccount, contractIDs []types.FileContractID, log *zap.Logger) (FundResult, error) {
	result := newFundResult(accounts)

	// sanity check input
	if len(accounts) == 0 {
		return result, nil
	} else if len(contractIDs) == 0 {
		log.Debug("no contracts provided")
		return result, nil
	}

	// dial host
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	t, err := siamux.Dial(dialCtx, addr, hk)
	if err != nil {
		log.Debug("failed to dial host", zap.Error(err))
		return result, nil
	}
	defer t.Close()

	// prepare unfunded indices
	unfunded := make([]int, 0, len(accounts))
	for i := range accounts {
		unfunded = append(unfunded, i)
	}

	// iterate over contracts
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
		}
		balance := rev.Contract.RenterOutput.Value

		// prepare accounts batch
		var batch []proto.Account
		var batchIndices []int
		for _, i := range unfunded {
			var underflow bool
			balance, underflow = balance.SubWithUnderflow(f.target)
			if underflow {
				break
			}

			batch = append(batch, accounts[i].AccountKey)
			batchIndices = append(batchIndices, i)
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
		res, err := rhp.RPCReplenishAccounts(ctx, t, params, f.cm.TipState(), f.signer)
		if err != nil {
			log.Debug("failed to replenish accounts", zap.Error(err))
			continue
		}

		// process response
		result.Usage = result.Usage.Add(res.Usage)
		for _, i := range batchIndices {
			result.Funded[i] = true
		}

		// update unfunded accounts
		unfunded = unfunded[len(batch):]
		if len(unfunded) == 0 {
			break
		}
	}

	return result, nil
}
