package slabs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

func (m *SlabManager) performIntegrityChecksForHost(ctx context.Context, host hosts.Host, logger *zap.Logger) {
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
		results, err := m.verifier.VerifySectors(ctx, host, toCheck)
		if errors.Is(err, context.Canceled) || errors.Is(err, errInsufficientServiceAccountBalance) || errors.Is(err, errHostUnreachable) {
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
		hostLogger.Debug("performed integrity checks", zap.Int("lost", len(lost)), zap.Int("failed", len(failed)), zap.Int("successful", len(success)))

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
			err := m.store.MarkFailingSectorsLost(ctx, host.PublicKey, m.maxFailedIntegrityChecks)
			if err != nil {
				hostLogger.Error("failed to mark failing sectors as lost", zap.Error(err))
				return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
			}
			return nil
		}()
		if err != nil {
			hostLogger.Error("failed to persist integrity check results", zap.Error(err))
			return
		}
	}
}
