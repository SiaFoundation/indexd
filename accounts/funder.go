package accounts

import (
	"context"
	"errors"
	"fmt"
	"io"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/client"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// HostClient defines the interface for the funder to interact with the
	// host.
	HostClient interface {
		io.Closer
		LatestRevision(context.Context, types.FileContractID) (proto.RPCLatestRevisionResponse, error)
		ReplenishAccounts(context.Context, types.FileContractID, []proto.Account, types.Currency) (rhp.RPCReplenishAccountsResult, int, error)
	}

	// Dialer defines an interface for dialing the host and returning a host client. This client can be used to
	// interact with the host using the RHP methods. The client is expected to be closed when no longer needed.
	Dialer interface {
		DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error)
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		dialer Dialer
	}
)

type wrapper struct {
	d *client.SiamuxDialer
}

// DialHost dials the host and returns a HostClient.
func (w *wrapper) DialHost(ctx context.Context, hostKey types.PublicKey, addr string) (HostClient, error) {
	client, err := w.d.DialHost(ctx, hostKey, addr)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// NewFunder creates a new Funder.
func NewFunder(dialer *client.SiamuxDialer) *Funder {
	return &Funder{dialer: &wrapper{d: dialer}}
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
	hc, err := f.dialer.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to dial host %s: %w", host.PublicKey, err)
	}
	defer hc.Close()

	// prepare account keys
	accountKeys := make([]proto.Account, len(accounts))
	for i, account := range accounts {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		// execute replenish RPC
		maxEnd := min(len(accountKeys), funded+proto.MaxAccountBatchSize)
		res, n, err := hc.ReplenishAccounts(ctx, contractID, accountKeys[funded:maxEnd], target)
		if errors.Is(err, client.ErrContractInsufficientFunds) {
			contractLog.Debug("contract has insufficient funds")
			drained++
			continue
		} else if errors.Is(err, client.ErrContractNotRevisable) {
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
