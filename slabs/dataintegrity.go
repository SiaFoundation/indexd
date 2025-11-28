package slabs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
)

func (m *SlabManager) performIntegrityChecksForHost(ctx context.Context, hostKey types.PublicKey, logger *zap.Logger) {
	logger = logger.With(zap.Stringer("hostKey", hostKey))

	const batchSize = 1000 // batch size for sector retrieval
	for interrupt := false; !interrupt; {
		toCheck, err := m.store.SectorsForIntegrityCheck(hostKey, batchSize)
		if err != nil {
			logger.Error("failed to fetch sectors for integrity check", zap.Error(err))
			return
		} else if len(toCheck) == 0 {
			return
		}
		interrupt = len(toCheck) < batchSize

		// perform integrity checks
		results := make([]CheckSectorsResult, 0, len(toCheck))
		for len(results) < len(toCheck) {
			usableCtx, usableCancel := context.WithTimeout(ctx, time.Minute)
			usable, err := m.hm.Usable(usableCtx, hostKey)
			usableCancel()
			if err != nil {
				logger.Error("failed to check if host is usable", zap.Error(err))
				return
			} else if !usable {
				logger.Debug("host is no longer usable, interrupting integrity checks")
				interrupt = true
				break
			}

			batch, err := m.verifier.VerifySectors(ctx, hostKey, toCheck[len(results):])
			if errors.Is(err, context.Canceled) || errors.Is(err, mux.ErrClosedStream) || errors.Is(err, errInsufficientServiceAccountBalance) || errors.Is(err, errHostUnreachable) {
				logger.Debug("integrity checks got interrupted", zap.Error(err))
				if errors.Is(err, errInsufficientServiceAccountBalance) {
					if err := m.cm.TriggerAccountRefill(ctx, hostKey, m.verifier.account()); err != nil {
						logger.Error("failed to trigger refill for integrity check service account", zap.Error(err))
					}
				}
				interrupt = true
				break
			}
			if err != nil {
				logger.Error("failed to check sectors", zap.Error(err))
				return
			}
			results = append(results, batch...)
		}
		if len(results) == 0 {
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
				logger.Fatal("unknown result", zap.Int("result", int(result)))
			}
		}
		logger.Debug("performed integrity checks", zap.Int("lost", len(lost)), zap.Int("failed", len(failed)), zap.Int("successful", len(success)))

		// starting from here, we use a background context with a
		// timeout, to make sure even when the integrity checks are
		// interrupted, we give it some time to persist the results.
		err = func() error {
			// update lost, failed and successful sectors
			if err := m.store.MarkSectorsLost(hostKey, lost); err != nil {
				logger.Error("failed to mark sectors as lost", zap.Error(err))
				return fmt.Errorf("failed to mark sectors as lost: %w", err)
			}
			if err := m.store.RecordIntegrityCheck(false, time.Now().Add(m.failedIntegrityCheckInterval), hostKey, failed); err != nil {
				logger.Error("failed to record integrity check for failed sectors", zap.Error(err))
				return fmt.Errorf("failed to record integrity check for failed sectors: %w", err)
			}
			if err := m.store.RecordIntegrityCheck(true, time.Now().Add(m.integrityCheckInterval), hostKey, success); err != nil {
				logger.Error("failed to record integrity check for successful sectors", zap.Error(err))
				return fmt.Errorf("failed to record integrity check for successful sectors: %w", err)
			}

			// fetch sector roots for sectors that have now failed the check 5+
			// times and mark them lost as well
			err := m.store.MarkFailingSectorsLost(hostKey, m.maxFailedIntegrityChecks)
			if err != nil {
				logger.Error("failed to mark failing sectors as lost", zap.Error(err))
				return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
			}
			return nil
		}()
		if err != nil {
			logger.Error("failed to persist integrity check results", zap.Error(err))
			return
		}
	}
}
