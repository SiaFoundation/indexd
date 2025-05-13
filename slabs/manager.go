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
		hm    HostManager
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

	// HostManager defines the minimal interface of HostManager functionality
	// the SlabManager requires.
	HostManager interface {
		ScanHost(ctx context.Context, hk types.PublicKey) (hosts.Host, error)
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
func NewManager(am AccountManager, hm HostManager, store Store, serviceAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	m, err := newSlabManager(am, hm, store, serviceAccount, opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}

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

func newSlabManager(am AccountManager, hm HostManager, store Store, serviceAccount types.PrivateKey, opts ...Option) (*SlabManager, error) {
	m := &SlabManager{
		integrityCheckInterval:       7 * 24 * time.Hour,
		failedIntegrityCheckInterval: 6 * time.Hour,
		maxFailedIntegrityChecks:     5,

		serviceAccount:    proto.Account(serviceAccount.PublicKey()),
		serviceAccountKey: serviceAccount,

		am:    am,
		hm:    hm,
		store: store,
		tg:    threadgroup.New(),
		log:   zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	// add account to store
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.AddAccount(ctx, types.PublicKey(m.serviceAccount)); err != nil && !errors.Is(err, accounts.ErrExists) {
		return nil, fmt.Errorf("failed to add service account: %w", err)
	}

	// let AccountManager know about the service account
	am.RegisterServiceAccount(m.serviceAccount)
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

		sem <- struct{}{}
		wg.Add(1)
		go func(hostKey types.PublicKey) {
			defer func() {
				<-sem
				wg.Done()
			}()

			// fetch good price table
			host, err = m.hm.ScanHost(ctx, hostKey)
			if err != nil {
				logger.With(zap.Stringer("hostKey", hostKey)).Error("failed to scan host", zap.Error(err))
				return
			}

			// create verifier
			verifier, err := newSectorVerifier(ctx, host.SiamuxAddr(), host.PublicKey, host.Settings.Prices)
			if err != nil {
				// NOTE: If we can't dial the host we don't mark sectors as lost.
				// Instead we leave it up to the scan code to determine whether the host
				// is offline.
				logger.With(zap.Stringer("hostKey", host.PublicKey)).Warn("failed to create sector verifier", zap.Error(err))
				return
			}
			defer verifier.Close()

			m.performIntegrityChecksForHost(ctx, verifier, logger)
		}(host.PublicKey)
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
