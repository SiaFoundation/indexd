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
