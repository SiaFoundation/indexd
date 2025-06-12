package accounts

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// ChainManager defines an interface to get the current chain state.
	ChainManager interface {
		TipState() consensus.State
	}

	// HostManager defines an interface to dial a host and get a client.
	HostManager interface {
		DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (any, error)
	}

	// HostClient defines an interface for replenishing accounts on a host.
	HostClient interface {
		io.Closer
		ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (rhp.RPCReplenishAccountsResult, int, error)
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		cm ChainManager
		hm HostManager
	}
)

// NewFunder creates a new Funder.
func NewFunder(cm ChainManager, hm HostManager) *Funder {
	return &Funder{
		cm: cm,
		hm: hm,
	}
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. The given accounts should not exceed the batch
// size used in the replenish RPC. This method returns two numbers, the first
// one indicates the number of accounts that were funded, the second indicates
// the number of contracts that were drained. Consecutive calls for the same
// host should take this into account and adjust the contract IDs that are being
// passed in.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accounts []HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, _ error) {
	// sanity check the input
	if len(accounts) > proto.MaxAccountBatchSize {
		return 0, 0, errors.New("too many accounts")
	} else if len(contractIDs) == 0 {
		return 0, 0, errors.New("no contract provided")
	} else if len(accounts) == 0 {
		return 0, 0, nil
	}

	// dial the host
	raw, err := f.hm.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to dial host %s: %w", host.PublicKey, err)
	}
	client, ok := raw.(HostClient)
	if !ok {
		return 0, 0, fmt.Errorf("host client does not implement HostClient interface: %T", raw)
	}
	defer client.Close()

	// prepare account keys
	accountKeys := make([]proto.Account, len(accounts))
	for i, account := range accounts {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		// execute replenish RPC
		res, n, err := client.ReplenishAccounts(ctx, contractID, accountKeys[funded:], target)
		if errors.Is(err, hosts.ErrContractInsufficientFunds) {
			contractLog.Debug("contract has insufficient funds")
			drained++
			continue
		} else if errors.Is(err, hosts.ErrContractNotRevisable) {
			contractLog.Debug("contract is not revisable") // sanity check
			drained++
			continue
		} else if err != nil {
			contractLog.Debug("failed to replenish accounts", zap.Error(err))
			continue
		} else if res.Revision.RenterOutput.Value.Cmp(target) < 0 {
			contractLog.Debug("contract was drained by replenish RPC")
			drained++
		}

		// update funded ix
		funded += n
		if funded == len(accountKeys) {
			break
		}
	}

	return funded, drained, nil
}
