package stats

import (
	"context"
	"errors"
	"time"

	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
)

const (
	flushInterval  = 5 * time.Second
	flushBatchSize = 1000
)

// A Store flushes accumulated stat deltas.
type Store interface {
	FlushStatsDelta(limit int) (bool, error)
}

// An Option is a functional option for the Manager.
type Option func(*Manager)

// WithLogger sets the logger for the Manager.
func WithLogger(l *zap.Logger) Option {
	return func(m *Manager) {
		m.log = l
	}
}

// A Manager periodically flushes stat deltas into the stats table.
type Manager struct {
	store Store
	tg    *threadgroup.ThreadGroup
	log   *zap.Logger
}

// NewManager creates a new stats Manager.
func NewManager(store Store, opts ...Option) (*Manager, error) {
	m := &Manager{
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
		m.flushLoop(ctx)
	}()

	return m, nil
}

// Close stops the manager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

func (m *Manager) flushLoop(ctx context.Context) {
	t := time.NewTicker(flushInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.flush()
		}
	}
}

func (m *Manager) flush() {
	for {
		more, err := m.store.FlushStatsDelta(flushBatchSize)
		if err != nil {
			if !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				m.log.Error("failed to flush stats delta", zap.Error(err))
			}
			return
		} else if !more {
			return
		}
	}
}
