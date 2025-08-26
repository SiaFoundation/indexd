package hosts

import (
	"time"

	"go.sia.tech/coreutils/chain"
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

// WithScanFrequency sets the frequency with which we check for hosts that need
// scanning.
func WithScanFrequency(d time.Duration) Option {
	return func(m *HostManager) {
		m.scanFrequency = d
	}
}

// WithScanInterval sets the minimum amount of time we wait before scanning the
// host again after a successful scan.
func WithScanInterval(d time.Duration) Option {
	return func(m *HostManager) {
		m.scanInterval = d
	}
}

type (
	// UsableHostQueryOpt is a functional option for querying usable hosts.
	UsableHostQueryOpt func(*UsableHostsQueryOpts)

	// UsableHostsQueryOpts holds optional parameters for querying usable
	// hosts.
	UsableHostsQueryOpts struct {
		Protocol *chain.Protocol
	}
)

// WithProtocol causes UsableHosts to only return hosts with the given protocol.
func WithProtocol(protocol chain.Protocol) UsableHostQueryOpt {
	return func(opts *UsableHostsQueryOpts) {
		opts.Protocol = &protocol
	}
}
