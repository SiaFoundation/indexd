package subscriber

import "go.uber.org/zap"

// An Option is a functional option for the Subscriber.
type Option func(*Subscriber)

// WithLogger sets the logger for the Subscriber.
func WithLogger(l *zap.Logger) Option {
	return func(m *Subscriber) {
		m.log = l
	}
}

// WithBatchSize sets the batch size for chain updates.
func WithBatchSize(bs int) Option {
	return func(m *Subscriber) {
		m.updateBatchSize = bs
	}
}

// WithPruneTarget sets the prune target of the subscriber. A prune target
// of 0 means pruning is disabled. A target n > 0 means that only the last
// n blocks will be kept. Must be at least 6 hours of blocks if enabled.
func WithPruneTarget(target uint64) Option {
	return func(m *Subscriber) {
		m.pruneTarget = target
	}
}
