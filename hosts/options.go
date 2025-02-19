package hosts

import (
	"time"

	"go.uber.org/zap"
)

// An Option is a functional option for the HostManager.
type Option func(*HostManager)

// WithLogger sets the logger for the HostManager.
func WithLogger(l *zap.Logger) Option {
	return func(m *HostManager) {
		m.log = l
	}
}

// WithAnnouncementMaxAge sets the maximum age of an announcement before it gets
// ignored.
func WithAnnouncementMaxAge(maxAge time.Duration) Option {
	return func(m *HostManager) {
		m.announcementMaxAge = maxAge
	}
}

// WithScanFrequency sets the scanning frequency.
func WithScanFrequency(d time.Duration) Option {
	return func(m *HostManager) {
		m.scanFrequency = d
	}
}

// WithScanInterval sets the interval between scans.
func WithScanInterval(d time.Duration) Option {
	return func(m *HostManager) {
		m.scanInterval = d
	}
}
