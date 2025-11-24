package slabs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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
		hosts          HostClient
		log            *zap.Logger
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
func NewSectorVerifier(am AccountManager, hosts HostClient, serviceAccount types.PrivateKey, log *zap.Logger) *SectorVerifier {
	return &SectorVerifier{am: am, hosts: hosts, log: log.Named("verifier"), serviceAccount: serviceAccount}
}

// UpdateBalance debits the service account for the cost of the verify sector RPC.
func (v *SectorVerifier) UpdateBalance(ctx context.Context, hostKey types.PublicKey, usage proto.Usage) error {
	if err := v.am.DebitServiceAccount(ctx, hostKey, v.account(), usage.RenterCost()); err != nil {
		return fmt.Errorf("failed to update service account balance: %w", err)
	}
	return nil
}

// ResetBalance resets the service account balance for the host.
func (v *SectorVerifier) ResetBalance(ctx context.Context, hostKey types.PublicKey) error {
	if err := v.am.ResetAccountBalance(ctx, hostKey, v.account()); err != nil {
		return fmt.Errorf("failed to reset service account balance: %w", err)
	}
	return nil
}

// VerifySectors verifies a list of sectors on a host. If verifySectors returns
// either errInsufficientServiceAccountBalance or context.Canceled, the caller
// should handle any remaining results and then interrupt the integrity checks
// for the host.
func (v *SectorVerifier) VerifySectors(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) ([]CheckSectorsResult, error) {
	log := v.log.With(zap.Stringer("hostKey", hostKey))

	// check budget before verifying the sector
	budget, err := v.am.ServiceAccountBalance(ctx, hostKey, v.account())
	if err != nil {
		return nil, fmt.Errorf("failed to get service account balance: %w", err)
	}

	var results []CheckSectorsResult
	var resetOnce sync.Once
	for _, root := range roots {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		prices, err := v.hosts.Prices(ctx, hostKey)
		if err != nil {
			log.Debug("failed to fetch host prices", zap.Error(err))
			return nil, errHostUnreachable
		}

		usage := prices.RPCReadSectorCost(proto.LeafSize)
		cost := usage.RenterCost()

		// check our budget before verifying the sector
		if budget.Cmp(cost) < 0 {
			return results, errInsufficientServiceAccountBalance
		}

		// adjust the budget and update the balance regardless of
		// the outcome since the host may debit for the RPC anyway.
		budget = budget.Sub(cost)
		if err := v.UpdateBalance(ctx, hostKey, usage); err != nil {
			return nil, fmt.Errorf("failed to update service account balance: %w", err)
		}

		// check a random segment of the sector
		segment := frand.Uint64n(proto.LeavesPerSector)
		_, err = v.hosts.ReadSector(ctx, v.serviceAccount, hostKey, root, io.Discard, segment*proto.LeafSize, proto.LeafSize)
		if errors.Is(err, context.Canceled) || errors.Is(err, mux.ErrClosedStream) {
			return results, err // interrupted
		}

		// check results - we need to be careful here since we can't trust the
		// host and need to assume that anything that isn't a success is a
		// potentially lost sector
		if err == nil {
			results = append(results, SectorSuccess)
		} else if errors.Is(err, proto.ErrSectorNotFound) {
			results = append(results, SectorLost)
		} else {
			log.Debug("failed to verify sector",
				zap.Stringer("sector", root),
				zap.Error(err),
			)
			results = append(results, SectorFailed)
		}

		// if the host returned an insufficient balance error, reset the account
		if errors.Is(err, proto.ErrNotEnoughFunds) {
			resetOnce.Do(func() { v.ResetBalance(ctx, hostKey) })

			// NOTE: when this happens we don't interrupt on purpose and
			// continue as if our internal balance was ok. So if we still
			// expected to have enough balance to check 100 sectors, the next
			// 100 sectors will probably also have a failure recorded. This
			// means the host can't be clever about using an out-of-funds error
			// to avoid penalties but we are also not harsher than necessary.
			continue
		}
	}
	return results, nil
}

func (v *SectorVerifier) account() proto.Account {
	return proto.Account(v.serviceAccount.PublicKey())
}
