package slabs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

// CheckSectorsResult is the result of a sector verification. It indicates
// whether:
// - the sector was lost
// - the sector failed verification for any reason
// - the sector was successfully verified
type CheckSectorsResult int

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

var errInsufficientServiceAccountBalance = errors.New("insufficient service account balance")

type (
	// SectorVerifier defines the interface verifying a sector's integrity on a
	// host.
	SectorVerifier interface {
		VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error)
	}

	sectorVerifier struct {
		tc rhp.TransportClient
	}
)

func newSectorVerifier(ctx context.Context, hostAddr string, hostKey types.PublicKey) (*sectorVerifier, error) {
	tc, err := siamux.Dial(ctx, hostAddr, hostKey)
	if err != nil {
		return nil, err
	}
	return &sectorVerifier{tc: tc}, nil
}

func (v *sectorVerifier) Close() error {
	return v.tc.Close()
}

func (v *sectorVerifier) VerifySector(ctx context.Context, prices proto.HostPrices, token proto.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	return rhp.RPCVerifySector(ctx, v.tc, prices, token, root)
}

func (m *SlabManager) performIntegrityChecksForHost(ctx context.Context, verifier SectorVerifier, host hosts.Host, logger *zap.Logger) {
	hostLogger := logger.With(zap.Stringer("hostKey", host.PublicKey))

	const batchSize = 100 // batch size for sector retrieval
	for interrupt := false; !interrupt; {
		toCheck, err := m.store.SectorsForIntegrityCheck(ctx, host.PublicKey, batchSize)
		if err != nil {
			hostLogger.Error("failed to fetch sectors for integrity check", zap.Error(err))
			return
		} else if len(toCheck) == 0 {
			return
		}
		interrupt = len(toCheck) < batchSize

		// perform integrity checks
		results, err := m.verifySectors(ctx, verifier, host, toCheck)
		if errors.Is(err, context.Canceled) || errors.Is(err, errInsufficientServiceAccountBalance) {
			interrupt = true
		}
		if err != nil {
			hostLogger.Error("failed to check sectors", zap.Error(err))
			return
		}
		var lost, failed, success []types.Hash256
		for i, result := range results {
			switch result {
			case SectorLost:
				lost = append(lost, toCheck[i])
			case SectorFailed:
				failed = append(failed, toCheck[i])
			case SectorSuccess:
				success = append(success, toCheck[i])
			default:
				hostLogger.Fatal("unknown result", zap.Int("result", int(result)))
			}
		}

		// starting from here, we use a background context with a
		// timeout, to make sure even when the integrity checks are
		// interrupted, we give it some time to persist the results.
		err = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			// update lost, failed and successful sectors
			if err := m.store.MarkSectorsLost(ctx, host.PublicKey, lost); err != nil {
				hostLogger.Error("failed to mark sectors as lost", zap.Error(err))
				return fmt.Errorf("failed to mark sectors as lost: %w", err)
			}
			if err := m.store.RecordIntegrityCheck(ctx, false, time.Now().Add(m.failedIntegrityCheckInterval), host.PublicKey, failed); err != nil {
				hostLogger.Error("failed to record integrity check for failed sectors", zap.Error(err))
				return fmt.Errorf("failed to record integrity check for failed sectors: %w", err)
			}
			if err := m.store.RecordIntegrityCheck(ctx, true, time.Now().Add(m.integrityCheckInterval), host.PublicKey, success); err != nil {
				hostLogger.Error("failed to record integrity check for successful sectors", zap.Error(err))
				return fmt.Errorf("failed to record integrity check for successful sectors: %w", err)
			}

			// fetch sector roots for sectors that have now failed the check 5+
			// times and mark them lost as well
			for {
				newlyLost, err := m.store.FailingSectors(ctx, host.PublicKey, m.maxFailedIntegrityChecks, batchSize)
				if err != nil {
					hostLogger.Error("failed to fetch failing sectors", zap.Error(err))
					return fmt.Errorf("failed to fetch failing sectors: %w", err)
				}
				if err := m.store.MarkSectorsLost(ctx, host.PublicKey, newlyLost); err != nil {
					hostLogger.Error("failed to mark sectors as lost", zap.Error(err))
					return fmt.Errorf("failed to mark sectors as lost: %w", err)
				}
				if len(newlyLost) < batchSize {
					return nil
				}
			}
		}()
		if err != nil {
			hostLogger.Error("failed to persist integrity check results", zap.Error(err))
			return
		}
	}
}

// verifySectors verifies a list of sectors on a host. If verifySectors returns
// either errInsufficientServiceAccountBalance or context.Canceled, the caller
// should stop handle any remaining results and then interrupt the integrity
// checks for the host.
func (m *SlabManager) verifySectors(ctx context.Context, hc SectorVerifier, host hosts.Host, roots []types.Hash256) ([]CheckSectorsResult, error) {
	// check the account balance
	cost := host.Settings.Prices.RPCVerifySectorCost().RenterCost()
	balance, err := m.am.ServiceAccountBalance(ctx, host.PublicKey, m.serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to get service account balance: %w", err)
	} else if balance.Cmp(cost) < 0 {
		return nil, errInsufficientServiceAccountBalance
	}

	var results []CheckSectorsResult
	var resetOnce sync.Once
	for _, root := range roots {
		// check for interruption
		select {
		case <-ctx.Done():
			return results, nil
		default:
		}

		// check if we have enough funds remaining
		if balance.Cmp(cost) < 0 {
			return results, errInsufficientServiceAccountBalance
		}

		// verify the sector
		_, err := hc.VerifySector(ctx, host.Settings.Prices, m.serviceAccount.Token(m.serviceAccountKey, host.PublicKey), root)
		if errors.Is(err, context.Canceled) {
			return results, err // interrupted
		}

		// adjust balance
		balance = balance.Sub(cost)
		if err := m.am.DebitServiceAccount(ctx, host.PublicKey, m.serviceAccount, cost); err != nil {
			return nil, fmt.Errorf("failed to debit service account: %w", err)
		}

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
		// NOTE: when this happens we don't interrupt on purpose. Instead we
		// continue as if our internal balance was ok. So if we still expected
		// to have enough balance to check 100 sectors, the next 100 sectors
		// will probably also have a failure recorded. This means the host can't
		// be clever about using an out-of-funds error to avoid penalties but we
		// are also not harsher than necessary.
		var resetErr error
		resetOnce.Do(func() {
			if err != nil && strings.Contains(err.Error(), proto.ErrNotEnoughFunds.Error()) {
				resetErr = m.am.ResetAccountBalance(ctx, host.PublicKey, m.serviceAccount)
			}
		})
		if resetErr != nil {
			return nil, fmt.Errorf("failed to reset service account balance: %w", err)
		}
	}
	return results, nil
}
