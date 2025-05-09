package slabs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const (
	healthCheckInterval = 30 * time.Minute
)

type (
	// SlabManager is responsible for managing slabs, including pinning them,
	// checking their integrity on the network and migrating their sectors if
	// necessary.
	SlabManager struct {
		integrityCheckInterval       time.Duration
		failedIntegrityCheckInterval time.Duration
		maxFailedIntegrityChecks     int

		serviceAccount    proto.Account
		serviceAccountKey types.PrivateKey

		am    AccountManager
		store Store
		tg    *threadgroup.ThreadGroup
		log   *zap.Logger
	}

	// AccountManager defines the SlabManager's dependencies on the account
	// manager.
	AccountManager interface {
		RegisterServiceAccount(account proto.Account)
		ResetAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) error
		ServiceAccountBalance(ctx context.Context, hostKey types.PublicKey, account proto.Account) (types.Currency, error)
		DebitServiceAccount(ctx context.Context, hostKey types.PublicKey, account proto.Account, amount types.Currency) error
	}

	// IntegrityChecker
	IntegrityChecker interface {
		CheckSectors(ctx context.Context, prices proto.HostPrices, account proto.Account, roots []types.Hash256) ([]CheckSectorsResult, error)
	}

	// Store defines an interface to store and update slab related information
	// in the database.
	Store interface {
		AddAccount(ctx context.Context, ak types.PublicKey) error
		FailingSectors(ctx context.Context, hostKey types.PublicKey, minChecks, limit int) ([]types.Hash256, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...hosts.HostQueryOpt) ([]hosts.Host, error)
		MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error
		PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab SlabPinParams) (SlabID, error)
		RecordIntegrityCheck(ctx context.Context, success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error
		SectorsForIntegrityCheck(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error)
		Slabs(ctx context.Context, accountID proto.Account, slabIDs []SlabID) ([]Slab, error)
	}
)

// An Option is a functional option for the SlabManager.
type Option func(*SlabManager)

// WithLogger sets the logger for the SlabManager.
func WithLogger(l *zap.Logger) Option {
	return func(m *SlabManager) {
		m.log = l
	}
}

// NewManager creates a new slab manager.
func NewManager(am AccountManager, store Store, serviceAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	m := &SlabManager{
		integrityCheckInterval:       7 * 24 * time.Hour,
		failedIntegrityCheckInterval: 6 * time.Hour,
		maxFailedIntegrityChecks:     3,

		serviceAccount:    proto.Account(serviceAccount.PublicKey()),
		serviceAccountKey: serviceAccount,

		am:    am,
		store: store,
		tg:    threadgroup.New(),
		log:   zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}

	// add account to store
	if err := store.AddAccount(ctx, types.PublicKey(m.serviceAccount)); err != nil && !errors.Is(err, accounts.ErrExists) {
		return nil, fmt.Errorf("failed to add service account: %w", err)
	}

	// let AccountManager know about the service account
	am.RegisterServiceAccount(m.serviceAccount)

	go func() {
		defer cancel()

		healthTicker := time.NewTicker(healthCheckInterval)
		defer healthTicker.Stop()

		for {
			select {
			case <-healthTicker.C:
			case <-ctx.Done():
				return
			}

			if err := m.performIntegrityChecks(ctx); err != nil {
				m.log.Error("failed to perform integrity checks", zap.Error(err))
			}

			if err := m.performSlabMigrations(); err != nil {
				m.log.Error("failed to perform slab migrations", zap.Error(err))
			}
		}
	}()

	return m, nil
}

// Close closes the manager.
func (m *SlabManager) Close() error {
	m.tg.Stop()
	return nil
}

func (m *SlabManager) performIntegrityChecks(ctx context.Context) error {
	start := time.Now()
	logger := m.log.Named("integrity")
	logger.Debug("starting integrity checks", zap.Time("start", start))

	usedHosts, err := m.store.Hosts(ctx, 0, math.MaxInt64,
		hosts.WithUsable(true), hosts.WithBlocked(false), hosts.WithActiveContracts(true))
	if err != nil {
		return fmt.Errorf("failed to fetch hosts to block: %w", err)
	}

	sem := make(chan struct{}, 50)
	var wg sync.WaitGroup
	for _, host := range usedHosts {
		select {
		case <-m.tg.Done():
			return nil
		default:
		}

		// TODO: dial host and get fresh prices
		var hc HostTransport

		sem <- struct{}{}
		wg.Add(1)
		go func(host hosts.Host) {
			defer func() {
				<-sem
				wg.Done()
			}()
			hostLogger := logger.With(zap.Stringer("hostKey", host.PublicKey))

			// TODO: batching
			toCheck, err := m.store.SectorsForIntegrityCheck(ctx, host.PublicKey, math.MaxInt)
			if err != nil {
				hostLogger.Error("failed to fetch sectors for integrity check", zap.Error(err))
				return
			}

			// perform integrity checks
			results, err := m.checkSectors(ctx, hc, host, toCheck)
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

			// update lost, failed and successful sectors
			if err := m.store.MarkSectorsLost(ctx, host.PublicKey, lost); err != nil {
				hostLogger.Error("failed to mark sectors as lost", zap.Error(err))
				return
			}
			if err := m.store.RecordIntegrityCheck(ctx, false, time.Now().Add(m.failedIntegrityCheckInterval), host.PublicKey, lost); err != nil {
				hostLogger.Error("failed to record integrity check for failed sectors", zap.Error(err))
				return
			}
			if err := m.store.RecordIntegrityCheck(ctx, true, time.Now().Add(m.integrityCheckInterval), host.PublicKey, success); err != nil {
				hostLogger.Error("failed to record integrity check for successful sectors", zap.Error(err))
				return
			}

			// fetch sector roots for sectors that have now failed the check 3+
			// times and mark them lost as well
			const batchSize = 100
			for {
				newlyLost, err := m.store.FailingSectors(ctx, host.PublicKey, m.maxFailedIntegrityChecks, batchSize)
				if err != nil {
					hostLogger.Error("failed to fetch failing sectors", zap.Error(err))
					return
				}
				if err := m.store.MarkSectorsLost(ctx, host.PublicKey, newlyLost); err != nil {
					hostLogger.Error("failed to mark sectors as lost", zap.Error(err))
					return
				}
				if len(newlyLost) < batchSize {
					return
				}
			}
		}(host)
	}
	wg.Wait()

	logger.Debug("finished integrity checks", zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (m *SlabManager) performSlabMigrations() error {
	start := time.Now()
	logger := m.log.Named("migrations")
	logger.Debug("starting slab migrations", zap.Time("start", start))

	// TODO: implement

	logger.Debug("finished slab migrations", zap.Duration("elapsed", time.Since(start)))
	return nil
}
