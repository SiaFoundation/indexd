package contracts

import (
	"context"
	"errors"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// FunderHostClient defines the interface for the funder to interact with the
	// host.
	FunderHostClient interface {
		AttachPools(ctx context.Context, hostKey types.PublicKey, inputs []rhp.PoolAttachInput, validity time.Duration) error
		ReplenishAccounts(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, params rhp.RPCReplenishAccountsParams) (rhp.RPCReplenishAccountsResult, error)
		ReplenishPools(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, params rhp.RPCReplenishPoolsParams) (rhp.RPCReplenishPoolsResult, error)
	}

	// FundResult is the outcome of funding a batch of accounts or pools.
	FundResult struct {
		// Funded is the number of accounts or pools that were funded. They
		// are funded in order, so these are always the first Funded accounts
		// or pools of the batch.
		Funded int

		// Drained contains the contracts that have insufficient funds, are no
		// longer revisable or have a remaining allowance below the target
		// after funding. They should be excluded when funding subsequent
		// batches on the same host.
		Drained []types.FileContractID

		// Skipped indicates that the accounts or pools that were not funded
		// should be skipped rather than counted as failed. It is true when
		// funding was interrupted, or when contracts were locked by another
		// operation and replenishing did not fail on any contract.
		Skipped bool
	}

	// Funder dials a host and replenishes ephemeral accounts and balance pools.
	Funder struct {
		client FunderHostClient
		locker *ContractLocker
		signer rhp.ContractSigner
		chain  ChainManager
		rev    *RevisionManager

		log *zap.Logger
	}
)

// NewFunder creates a new Funder.
func NewFunder(client FunderHostClient, cl *ContractLocker, rev *RevisionManager, signer rhp.ContractSigner, chain ChainManager, log *zap.Logger) *Funder {
	return &Funder{
		client: client,
		locker: cl,
		signer: signer,
		chain:  chain,
		rev:    rev,
		log:    log,
	}
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order, skipping contracts that are locked by another
// operation. Funding stops early if it is interrupted. The given accounts
// should not exceed the batch size used in the replenish RPC.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accs []accounts.HostAccount, target types.Currency, log *zap.Logger) (FundResult, error) {
	// sanity check the input
	if len(accs) > proto.MaxAccountBatchSize {
		return FundResult{}, errors.New("too many accounts")
	} else if len(contractIDs) == 0 {
		return FundResult{}, errors.New("no contract provided")
	} else if len(accs) == 0 {
		return FundResult{}, nil
	}

	// prepare account keys
	accountKeys := make([]proto.Account, len(accs))
	for i, account := range accs {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	var funded int
	var drained []types.FileContractID
	var contractsLocked, interrupted, failed bool
	for _, contractID := range contractIDs {
		done, err := func() (bool, error) {
			contractLog := log.With(zap.Stringer("contractID", contractID))

			lc, unlock := f.locker.TryLockContract(contractID)
			if lc == nil {
				contractsLocked = true
				contractLog.Debug("ignoring locked contract for funding")
				return false, nil
			}
			defer unlock()

			var res rhp.RPCReplenishAccountsResult
			var err error
			err = f.rev.WithRevision(ctx, lc, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
				if contract.Revision.RenterOutput.Value.Cmp(target) < 0 {
					return rhp.ContractRevision{}, proto.Usage{}, ErrContractInsufficientFunds
				}

				batchSize := int(max(1, min(contract.Revision.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize)))
				maxEnd := min(len(accountKeys), funded+batchSize)
				// execute replenish RPC
				res, err = f.client.ReplenishAccounts(ctx, f.signer, f.chain, rhp.RPCReplenishAccountsParams{
					Accounts: accountKeys[funded:maxEnd],
					Target:   target,
					Contract: contract,
				})
				if err != nil {
					return rhp.ContractRevision{}, proto.Usage{}, err
				}
				funded = maxEnd
				return rhp.ContractRevision{
					ID:       contractID,
					Revision: res.Revision,
				}, res.Usage, nil
			})
			if errors.Is(err, ErrContractInsufficientFunds) {
				contractLog.Debug("contract has insufficient funds", zap.Error(err))
				drained = append(drained, contractID)
				return false, nil
			} else if errors.Is(err, ErrContractNotRevisable) {
				contractLog.Debug("contract is not revisable", zap.Error(err)) // sanity check
				drained = append(drained, contractID)
				return false, nil
			} else if err != nil && ctx.Err() != nil {
				interrupted = true
				contractLog.Debug("replenishing accounts was interrupted", zap.Error(err))
				return true, nil
			} else if err != nil {
				failed = true
				contractLog.Debug("failed to replenish accounts", zap.Error(err))
				return false, nil
			} else if res.Revision.RemainingAllowance().Cmp(target) < 0 {
				contractLog.Debug("contract was drained by replenish RPC",
					zap.Stringer("remainingAllowance", res.Revision.RemainingAllowance()),
					zap.Stringer("target", target))
				drained = append(drained, contractID)
			}

			return funded == len(accountKeys), nil
		}()
		if err != nil {
			return FundResult{}, err
		} else if done {
			break
		}
	}

	return FundResult{
		Funded:  funded,
		Drained: drained,
		Skipped: funded < len(accs) && (interrupted || (contractsLocked && !failed)),
	}, nil
}

// AttachPools attaches pools to accounts on the specified host.
func (f *Funder) AttachPools(ctx context.Context, hostKey types.PublicKey, inputs []rhp.PoolAttachInput, validity time.Duration) error {
	return f.client.AttachPools(ctx, hostKey, inputs, validity)
}

// FundPools tops up the provided pools to the target balance using the
// specified contracts in order, skipping contracts that are locked by another
// operation. Funding stops early if it is interrupted. The given pools should
// not exceed the batch size used in the replenish RPC.
func (f *Funder) FundPools(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, pools []accounts.HostPool, target types.Currency, log *zap.Logger) (FundResult, error) {
	if len(pools) > proto.MaxAccountBatchSize {
		return FundResult{}, errors.New("too many pools")
	} else if len(contractIDs) == 0 {
		return FundResult{}, errors.New("no contract provided")
	} else if len(pools) == 0 {
		return FundResult{}, nil
	}

	poolKeys := make([]proto.Account, len(pools))
	for i, pool := range pools {
		poolKeys[i] = proto.Account(pool.PoolKey.PublicKey())
	}

	var funded int
	var drained []types.FileContractID
	var contractsLocked, interrupted, failed bool
	for _, contractID := range contractIDs {
		done, err := func() (bool, error) {
			contractLog := log.With(zap.Stringer("contractID", contractID))

			lc, unlock := f.locker.TryLockContract(contractID)
			if lc == nil {
				contractsLocked = true
				contractLog.Debug("ignoring locked contract for funding")
				return false, nil
			}
			defer unlock()

			var res rhp.RPCReplenishPoolsResult
			var err error
			err = f.rev.WithRevision(ctx, lc, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
				if contract.Revision.RenterOutput.Value.Cmp(target) < 0 {
					return rhp.ContractRevision{}, proto.Usage{}, ErrContractInsufficientFunds
				}

				batchSize := int(max(1, min(contract.Revision.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize)))
				maxEnd := min(len(poolKeys), funded+batchSize)
				res, err = f.client.ReplenishPools(ctx, f.signer, f.chain, rhp.RPCReplenishPoolsParams{
					Pools:    poolKeys[funded:maxEnd],
					Target:   target,
					Contract: contract,
				})
				if err != nil {
					return rhp.ContractRevision{}, proto.Usage{}, err
				}
				funded = maxEnd
				return rhp.ContractRevision{
					ID:       contractID,
					Revision: res.Revision,
				}, res.Usage, nil
			})
			if errors.Is(err, ErrContractInsufficientFunds) {
				contractLog.Debug("contract has insufficient funds", zap.Error(err))
				drained = append(drained, contractID)
				return false, nil
			} else if errors.Is(err, ErrContractNotRevisable) {
				contractLog.Debug("contract is not revisable", zap.Error(err))
				drained = append(drained, contractID)
				return false, nil
			} else if err != nil && ctx.Err() != nil {
				interrupted = true
				contractLog.Debug("replenishing pools was interrupted", zap.Error(err))
				return true, nil
			} else if err != nil {
				failed = true
				contractLog.Debug("failed to replenish pools", zap.Error(err))
				return false, nil
			} else if res.Revision.RemainingAllowance().Cmp(target) < 0 {
				contractLog.Debug("contract was drained by replenish pools RPC",
					zap.Stringer("remainingAllowance", res.Revision.RemainingAllowance()),
					zap.Stringer("target", target))
				drained = append(drained, contractID)
			}

			return funded == len(poolKeys), nil
		}()
		if err != nil {
			return FundResult{}, err
		} else if done {
			break
		}
	}

	return FundResult{
		Funded:  funded,
		Drained: drained,
		Skipped: funded < len(pools) && (interrupted || (contractsLocked && !failed)),
	}, nil
}
