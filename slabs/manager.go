package slabs

import (
	"context"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/coreutils/threadgroup"
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
		integrityCheckInterval time.Duration

		store Store
		tg    *threadgroup.ThreadGroup
		log   *zap.Logger
	}

	// Store defines an interface to store and update slab related information
	// in the database.
	Store interface {
		PinSlab(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, slab SlabPinParams) (SlabID, error)
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
func NewManager(store Store, opts ...Option) (*SlabManager, error) {
	m := &SlabManager{
		integrityCheckInterval: 14 * 24 * time.Hour, // 2 weeks

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

			if err := m.performIntegrityChecks(); err != nil {
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

func (m *SlabManager) performIntegrityChecks() error {
	start := time.Now()
	logger := m.log.Named("integrity")
	logger.Debug("starting integrity checks", zap.Time("start", start))

	// TODO: implement

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
