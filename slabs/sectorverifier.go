package slabs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
)

const (
	// SectorLost indicates that the sector was lost. This is returned if the
	// host admits to losing the sector.
	SectorLost = CheckSectorsResult(iota)

	// SectorFailed indicates that the sector failed verification for some
	// reason. Since we can't trust a host to not use all means necessary for us
	// to not track a failure, we consider anything that is not a lost sector or
	// successfully verification a failure.
	SectorFailed

	// SectorSuccess indicates that the sector was successfully verified and is
	// still in the host's possession.
	SectorSuccess
)

var (
	errInsufficientServiceAccountBalance = errors.New("insufficient service account balance")
	errHostUnreachable                   = errors.New("host unreachable")
)

type (
	// SectorVerifier is responsible for verifying sectors on hosts. It uses a
	// service account to pay for the RPCs and updates the account balance
	// accordingly. It returns a list of CheckSectorsResult for each sector that
	// gets verified.
	SectorVerifier struct {
		am             AccountManager
		dialer         Dialer
		serviceAccount types.PrivateKey
	}

	// CheckSectorsResult is the result of a sector verification. It indicates
	// whether:
	// - the sector was lost
	// - the sector failed verification for any reason
	// - the sector was successfully verified
	CheckSectorsResult int
)

// NewSectorVerifier creates a new SectorVerifier.
func NewSectorVerifier(am AccountManager, dialer Dialer, serviceAccount types.PrivateKey) *SectorVerifier {
	return &SectorVerifier{am: am, dialer: dialer, serviceAccount: serviceAccount}
}

// CheckBalance ensures the service account balance is sufficient to cover the
// cost of the verify sector RPC.
func (v *SectorVerifier) CheckBalance(ctx context.Context, host hosts.Host) (types.Currency, error) {
	balance, err := v.am.ServiceAccountBalance(ctx, host.PublicKey, v.account())
	if err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to get service account balance: %w", err)
	} else if balance.Cmp(host.Settings.Prices.RPCVerifySectorCost().RenterCost()) < 0 {
		return types.ZeroCurrency, errInsufficientServiceAccountBalance
	}
	return balance, nil
}

// UpdateBalance debits the service account for the cost of the verify sector RPC.
func (v *SectorVerifier) UpdateBalance(ctx context.Context, host hosts.Host, usage proto.Usage) error {
	if err := v.am.DebitServiceAccount(ctx, host.PublicKey, v.account(), usage.RenterCost()); err != nil {
		return fmt.Errorf("failed to update service account balance: %w", err)
	}
	return nil
}

// ResetBalance resets the service account balance for the host.
func (v *SectorVerifier) ResetBalance(ctx context.Context, host hosts.Host) error {
	if err := v.am.ResetAccountBalance(ctx, host.PublicKey, v.account()); err != nil {
		return fmt.Errorf("failed to reset service account balance: %w", err)
	}
	return nil
}

// VerifySectors verifies a list of sectors on a host. If verifySectors returns
// either errInsufficientServiceAccountBalance or context.Canceled, the caller
// should handle any remaining results and then interrupt the integrity checks
// for the host.
func (v *SectorVerifier) VerifySectors(ctx context.Context, host hosts.Host, roots []types.Hash256) ([]CheckSectorsResult, error) {
	hc, err := v.dialer.DialHost(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		return nil, fmt.Errorf("%w: failed to dial host %s: %w", errHostUnreachable, host.PublicKey, err)
	}
	defer hc.Close()

	// check budget before verifying the sector
	budget, err := v.CheckBalance(ctx, host)
	if err != nil {
		return nil, err
	}

	cost := host.Settings.Prices.RPCVerifySectorCost().RenterCost()
	var results []CheckSectorsResult
	var resetOnce sync.Once
	for _, root := range roots {
		select {
		case <-ctx.Done():
			return results, nil
		default:
		}

		// check our budget before verifying the sector
		if budget.Cmp(cost) < 0 {
			return results, errInsufficientServiceAccountBalance
		}

		// verify the sector
		res, err := hc.VerifySector(ctx, host.Settings.Prices, v.token(host.PublicKey), root)
		if errors.Is(err, context.Canceled) {
			return results, err // interrupted
		}

		// adjust the budget
		budget = budget.Sub(res.Usage.RenterCost())

		// check results - we need to be careful here since we can't trust the
		// host and need to assume that anything that isn't a success is a
		// potentially lost sector
		if err == nil {
			results = append(results, SectorSuccess)
		} else if strings.Contains(err.Error(), proto.ErrSectorNotFound.Error()) {
			results = append(results, SectorLost)
		} else {
			results = append(results, SectorFailed)
		}

		// if the host returned an insufficient balance error, reset the account
		if err != nil && strings.Contains(err.Error(), proto.ErrNotEnoughFunds.Error()) {
			resetOnce.Do(func() { err = v.ResetBalance(ctx, host) })

			// NOTE: when this happens we don't interrupt on purpose and
			// continue as if our internal balance was ok. So if we still
			// expected to have enough balance to check 100 sectors, the next
			// 100 sectors will probably also have a failure recorded. This
			// means the host can't be clever about using an out-of-funds error
			// to avoid penalties but we are also not harsher than necessary.
			continue
		}

		// update the service account balance
		err = v.UpdateBalance(ctx, host, res.Usage)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (v *SectorVerifier) account() proto.Account {
	return proto.Account(v.serviceAccount.PublicKey())
}

func (v *SectorVerifier) token(hostKey types.PublicKey) proto.AccountToken {
	acc := proto.Account(v.serviceAccount.PublicKey())
	return acc.Token(v.serviceAccount, hostKey)
}
