package hosts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/geoip"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	announcementMaxAddressesPerProtocol = 2

	pruneFrequency                  = time.Hour * 24
	pruneMinConsecutiveScanFailures = 10
	pruneMinDowntime                = time.Hour * 24 * 365 // 1 year

	scanThreads                    = 50
	scanTimeout                    = time.Minute
	scanIntervalOffsetHours        = 6
	scanExponentialBackoffHours    = 8
	scanExponentialBackoffMaxHours = 128
)

var (
	errNodeOffline = errors.New("node is offline")

	// ErrBadHost is returned when a host can't be interacted with due to being
	// considered bad.
	ErrBadHost = errors.New("host is bad")
)

type (
	// HostManager manages the host announcements.
	HostManager struct {
		announcementMaxAge time.Duration
		scanFrequency      time.Duration
		scanInterval       time.Duration

		onlineChecker OnlineChecker
		resolver      Resolver
		scanner       Scanner
		locator       Locator
		store         Store

		triggerHostScanningChan chan struct{}

		tg  *threadgroup.ThreadGroup
		log *zap.Logger
	}

	// HostStats contains various statistics about a single host.
	HostStats struct {
		AccountUsage        types.Currency  `json:"accountUsage"`
		ActiveContractsSize int64           `json:"activeContractsSize"`
		PublicKey           types.PublicKey `json:"publicKey"`
		LostSectors         int64           `json:"lostSectors"`
		TotalUsage          types.Currency  `json:"totalUsage"`
	}

	// OnlineChecker defines an interface to check whether the indexer is online. It's
	// used to ensure hosts aren't punished for failing a scan if the indexer is
	// offline.
	OnlineChecker interface {
		IsOnline() bool
	}

	// Resolver defines an interface to resolve hostnames.
	Resolver interface {
		LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	}

	// Scanner defines an interface to scan hosts.
	Scanner interface {
		ScanSiamux(context.Context, types.PublicKey, string) (proto4.HostSettings, error)
		ScanQuic(context.Context, types.PublicKey, string) (proto4.HostSettings, error)
	}

	// Store defines an interface to fetch hosts that need to be scanned and
	// persist the scan results in the database.
	Store interface {
		Host(ctx context.Context, hk types.PublicKey) (Host, error)
		Hosts(ctx context.Context, offset, limit int, queryOpts ...HostQueryOpt) ([]Host, error)
		HostStats(ctx context.Context, offset, limit int) ([]HostStats, error)

		HostsForScanning(ctx context.Context) ([]types.PublicKey, error)
		HostsForPruning(ctx context.Context) ([]types.PublicKey, error)
		HostsForPinning(ctx context.Context) ([]types.PublicKey, error)
		HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error)

		BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reasons []string) error
		BlockedHosts(ctx context.Context, offset, limit int) ([]types.PublicKey, error)
		UnblockHost(ctx context.Context, hk types.PublicKey) error

		PruneHosts(ctx context.Context, lastSuccessfulScanCutoff time.Time, minConsecutiveFailedScans int) (int64, error)
		UpdateHost(ctx context.Context, hk types.PublicKey, hs proto4.HostSettings, loc geoip.Location, scanSucceeded bool, nextScan time.Time) error

		UsabilitySettings(ctx context.Context) (UsabilitySettings, error)
		UpdateUsabilitySettings(ctx context.Context, us UsabilitySettings) error
	}

	// Syncer defines an interface that exposes the Peers method.
	Syncer interface {
		Peers() []*syncer.Peer
	}

	// UpdateTx defines what the host manager needs to atomically process a
	// chain update in the database.
	UpdateTx interface {
		AddHostAnnouncement(hk types.PublicKey, ha chain.V2HostAnnouncement, ts time.Time) error
	}

	// A Locator maps IP addresses to their location.
	Locator interface {
		Close() error
		Locate(ip net.IP) (geoip.Location, error)
	}
)

// RHP4Addrs returns the addresses of the host.
func (h *Host) RHP4Addrs() []chain.NetAddress {
	return h.Addresses
}

// Host returns the host with the given key.
func (hm *HostManager) Host(ctx context.Context, hostKey types.PublicKey) (Host, error) {
	return hm.store.Host(ctx, hostKey)
}

// Hosts returns a list of hosts filtered by the given query options.
func (hm *HostManager) Hosts(ctx context.Context, offset, limit int, queryOpts ...HostQueryOpt) ([]Host, error) {
	return hm.store.Hosts(ctx, offset, limit, queryOpts...)
}

// HostsForPruning returns a list of hosts with sectors that need pruning
func (hm *HostManager) HostsForPruning(ctx context.Context) ([]types.PublicKey, error) {
	return hm.store.HostsForPruning(ctx)
}

// HostsForPinning returns a list of hosts that have sectors that need pinning
func (hm *HostManager) HostsForPinning(ctx context.Context) ([]types.PublicKey, error) {
	return hm.store.HostsForPinning(ctx)
}

// BlockHosts blocks the given hosts for the given list of reasons
func (hm *HostManager) BlockHosts(ctx context.Context, hostKeys []types.PublicKey, reasons []string) error {
	return hm.store.BlockHosts(ctx, hostKeys, reasons)
}

// HostsWithUnpinnableSectors returns any hosts that have sectors that could not be pinned
func (hm *HostManager) HostsWithUnpinnableSectors(ctx context.Context) ([]types.PublicKey, error) {
	return hm.store.HostsWithUnpinnableSectors(ctx)
}

// BlockedHosts returns a list of blocked hosts
func (hm *HostManager) BlockedHosts(ctx context.Context, offset, limit int) ([]types.PublicKey, error) {
	return hm.store.BlockedHosts(ctx, offset, limit)
}

// UnblockHost unblocks the given host
func (hm *HostManager) UnblockHost(ctx context.Context, hk types.PublicKey) error {
	return hm.store.UnblockHost(ctx, hk)
}

// NewManager creates a new host manager.
func NewManager(syncer Syncer, locator Locator, store Store, opts ...Option) (*HostManager, error) {
	m := &HostManager{
		announcementMaxAge: time.Hour * 24 * 365,
		scanFrequency:      time.Hour,
		scanInterval:       time.Hour * 24,

		onlineChecker: &onlineChecker{addresses: fallbackSites, syncer: syncer},
		resolver:      &net.Resolver{},
		scanner:       &scanner{},
		locator:       locator,
		store:         store,

		triggerHostScanningChan: make(chan struct{}, 1),

		tg:  threadgroup.New(),
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	if m.announcementMaxAge == 0 {
		return nil, fmt.Errorf("announcementMaxAge can not be zero")
	} else if m.scanFrequency == 0 {
		return nil, fmt.Errorf("scanFrequency can not be zero")
	} else if m.scanInterval == 0 {
		return nil, fmt.Errorf("scanInterval can not be zero")
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		pruneTicker := time.NewTicker(pruneFrequency)
		scanTicker := time.NewTicker(m.scanFrequency)
		defer func() {
			pruneTicker.Stop()
			scanTicker.Stop()
		}()

		for {
			select {
			case <-pruneTicker.C:
				m.pruneHosts(ctx)
			case <-m.triggerHostScanningChan:
				// reset ticker
				scanTicker.Stop()
				scanTicker = time.NewTicker(m.scanFrequency)
				m.log.Debug("triggered host scanning")
				m.scanHosts(ctx, m.hostsForScanning(ctx, true))
			case <-scanTicker.C:
				m.scanHosts(ctx, m.hostsForScanning(ctx, false))
			case <-ctx.Done():
				return
			}
		}
	}()

	return m, nil
}

// Close closes the manager.
func (m *HostManager) Close() error {
	m.tg.Stop()
	return nil
}

// TriggerHostScanning triggers a scan of all hosts.
func (m *HostManager) TriggerHostScanning() {
	select {
	case m.triggerHostScanningChan <- struct{}{}:
	default:
	}
}

// UsabilitySettings returns the current usability settings.
func (m *HostManager) UsabilitySettings(ctx context.Context) (UsabilitySettings, error) {
	return m.store.UsabilitySettings(ctx)
}

// UpdateUsabilitySettings updates the host's usability settings.
func (m *HostManager) UpdateUsabilitySettings(ctx context.Context, us UsabilitySettings) error {
	// perhaps this should reset next_scan to NOW() on all hosts that succeeded their last scan?
	return m.store.UpdateUsabilitySettings(ctx, us)
}

// WithScannedHost calls the given function with the Host with the given host
// key. If the call fails due to the host's price table being outdated, it will
// scan the host and try again. If the host is bad, it returns ErrBadHost.
// NOTE: It's important that the function passed to WithScannedHost can be
// called twice.
func (m *HostManager) WithScannedHost(ctx context.Context, hk types.PublicKey, fn func(h Host) error) error {
	// fetch host
	host, err := m.store.Host(ctx, hk)
	if err != nil {
		return fmt.Errorf("failed to get host, %w", err)
	} else if !host.IsGood() {
		return fmt.Errorf("%w: blocked=%t, usable=%t", ErrBadHost, host.Blocked, host.Usability.Usable())
	}

	// optimistically call the function if the prices are still valid for a
	// bit
	const validPriceBuf = 5 * time.Minute
	if host.Settings.Prices.ValidUntil.After(time.Now().Add(validPriceBuf)) {
		if err := fn(host); err != nil && !errors.Is(err, proto4.ErrPricesExpired) {
			return err
		} else if err == nil {
			return nil // 'fn' succeeded so we're done
		}
	}

	// scan the host if the prices were outdated
	host, err = m.ScanHost(ctx, hk)
	if err != nil {
		return fmt.Errorf("failed to scan host, %w", err)
	} else if !host.IsGood() {
		return fmt.Errorf("%w: blocked=%t, usable=%t", ErrBadHost, host.Blocked, host.Usability.Usable())
	}

	// try again
	return fn(host)
}

// ScanHost scans the host with given host key and returns it with updated
// settings and checks.
func (m *HostManager) ScanHost(ctx context.Context, hk types.PublicKey) (Host, error) {
	logger := m.log.With(zap.Stringer("hk", hk))

	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()

	host, err := m.store.Host(ctx, hk)
	if err != nil {
		return Host{}, fmt.Errorf("failed to get host, %w", err)
	}

	addrs, loc, err := resolveHost(ctx, m.resolver, m.locator, host.Addresses, logger)
	if err != nil {
		return Host{}, fmt.Errorf("failed to resolve host, %w", err)
	}

	settings, err := fetchSettings(ctx, m.scanner, hk, addrs, logger)
	if err != nil {
		return Host{}, fmt.Errorf("failed to fetch settings, %w", err)
	}

	consecutiveFailures := host.ConsecutiveFailedScans
	success := settings != (proto4.HostSettings{})
	if !success && !m.onlineChecker.IsOnline() {
		return Host{}, errNodeOffline
	} else if !success {
		consecutiveFailures++
	}

	nextScan := calculateNextScanTime(
		time.Now(),
		success,
		consecutiveFailures,
		m.scanInterval,
		scanIntervalOffsetHours,
		scanExponentialBackoffHours,
		scanExponentialBackoffMaxHours,
	)

	// derive a context from the background context for persisting the scan
	// results to prevent a scan that timed out from not being recorded
	updateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = m.store.UpdateHost(updateCtx, hk, settings, loc, success, nextScan)
	if err != nil {
		return Host{}, fmt.Errorf("failed to update host, %w", err)
	}

	return m.store.Host(ctx, hk)
}

// UpdateChainState updates the host announcements in the database.
func (m *HostManager) UpdateChainState(tx UpdateTx, applied []chain.ApplyUpdate) error {
	for _, update := range applied {
		// ignore announcements that are too old
		if time.Since(update.Block.Timestamp) > time.Duration(m.announcementMaxAge) {
			continue
		}

		has := make(map[types.PublicKey]chain.V2HostAnnouncement)
		chain.ForEachV2HostAnnouncement(update.Block, func(hk types.PublicKey, addrs []chain.NetAddress) {
			filtered := make(map[chain.Protocol][]chain.NetAddress)
			for _, addr := range addrs {
				if err := validateAddress(addr); err != nil {
					m.log.Debug("ignoring host announcement", zap.Stringer("hk", hk), zap.Error(err))
				} else if len(filtered[addr.Protocol]) < announcementMaxAddressesPerProtocol {
					filtered[addr.Protocol] = append(filtered[addr.Protocol], addr)
				}
			}
			for _, addrs := range filtered {
				has[hk] = append(has[hk], addrs...)
			}
		})

		for hk, ha := range has {
			if err := tx.AddHostAnnouncement(hk, ha, update.Block.Timestamp); err != nil {
				return fmt.Errorf("failed to add host announcement: %w", err)
			}
		}
	}
	return nil
}

// Stats returns statistics about the hosts in the database.
func (m *HostManager) Stats(ctx context.Context, offset, limit int) ([]HostStats, error) {
	return m.store.HostStats(ctx, offset, limit)
}

// hostsForScanning returns the public keys of the hosts that need to be
// scanned, if force is true, this method will return the public keys of all
// hosts in the database.
func (m *HostManager) hostsForScanning(ctx context.Context, force bool) []types.PublicKey {
	if !force {
		hosts, err := m.store.HostsForScanning(ctx)
		if err != nil {
			m.log.Error("failed to get hosts for scanning", zap.Error(err))
			return nil
		}
		return hosts
	}

	// forcing a rescan of all hosts is only exposed with the debug flag
	// enabled, therefore it's fine to pay the price here and fetch all hosts
	// from the database only to get their public keys
	hosts, err := m.store.Hosts(ctx, 0, math.MaxInt)
	if err != nil {
		m.log.Error("failed to get hosts for scanning", zap.Error(err))
		return nil
	}

	var hks []types.PublicKey
	for _, h := range hosts {
		hks = append(hks, h.PublicKey)
	}
	return hks
}

func (m *HostManager) pruneHosts(ctx context.Context) {
	cutoff := time.Now().Add(-pruneMinDowntime)
	n, err := m.store.PruneHosts(ctx, time.Now().Add(-pruneMinDowntime), pruneMinConsecutiveScanFailures)
	if err != nil {
		m.log.Error("failed to prune hosts",
			zap.Time("minLastSuccessfulScan", cutoff),
			zap.Int("minConsecutiveFailures", pruneMinConsecutiveScanFailures),
			zap.Error(err),
		)
		return
	} else if n > 0 {
		m.log.Debug("pruned hosts",
			zap.Time("minLastSuccessfulScan", cutoff),
			zap.Int("minConsecutiveFailures", pruneMinConsecutiveScanFailures),
			zap.Int64("removed", n),
		)
	}
}

func (m *HostManager) scanHosts(ctx context.Context, hosts []types.PublicKey) {
	if len(hosts) == 0 {
		m.log.Debug("scan skipped, no hosts for scanning")
		return
	}

	start := time.Now()
	m.log.Debug("scan started", zap.Int("hosts", len(hosts)))

	sema := make(chan struct{}, scanThreads)
	defer close(sema)

	var once sync.Once
	var wg sync.WaitGroup
loop:
	for _, hk := range hosts {
		select {
		case <-ctx.Done():
			break loop
		case sema <- struct{}{}:
		}

		wg.Add(1)
		go func(ctx context.Context, hk types.PublicKey) {
			defer func() {
				<-sema
				wg.Done()
			}()

			if _, err := m.ScanHost(ctx, hk); errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			} else if errors.Is(err, errNodeOffline) {
				once.Do(func() { m.log.Warn("indexer is offline, skipping scans") })
				return
			} else if err != nil {
				m.log.Error("failed to perform host scan", zap.Stringer("hk", hk), zap.Error(err))
			}
		}(ctx, hk)
	}

	wg.Wait()

	m.log.Debug("host scans finished", zap.Int("hosts", len(hosts)), zap.Duration("duration", time.Since(start)))
}

// fetchSettings uses the given scanner to fetch the settings of the host with
// given public key. It only returns the settings if the host is available on
// every address. The only error this function returns is [context.Canceled],
// other errors that occur during the resolving and parsing are debug logged but
// otherwise ignored.
func fetchSettings(ctx context.Context, scanner Scanner, hk types.PublicKey, addresses []chain.NetAddress, log *zap.Logger) (proto4.HostSettings, error) {
	var settings []proto4.HostSettings
	for _, addr := range addresses {
		var hs proto4.HostSettings
		var err error
		switch addr.Protocol {
		case siamux.Protocol:
			hs, err = scanner.ScanSiamux(ctx, hk, addr.Address)
		case quic.Protocol:
			hs, err = scanner.ScanQuic(ctx, hk, addr.Address)
		default:
			continue // ignore
		}
		if errors.Is(err, context.Canceled) {
			return proto4.HostSettings{}, err
		} else if err != nil {
			log.Debug("failed to get host settings",
				zap.String("address", addr.Address),
				zap.String("protocol", string(addr.Protocol)),
				zap.Error(err))
			settings = nil // require host to be available on all addresses
			break
		}
		settings = append(settings, hs)
	}

	// return a random setting to avoid the host cheating us by providing
	// different settings over different addresses or protocols
	if len(settings) == 0 {
		return proto4.HostSettings{}, nil
	}
	return settings[frand.Intn(len(settings))], nil
}

// calculateNextScanTime calculates the time of the next scan based on the given
// parameters.
func calculateNextScanTime(lastScan time.Time, success bool, consecScanFailures int, interval time.Duration, offsetHours, expBackoffHours, expBackoffHoursMax int) time.Time {
	// sanity check input
	if interval == 0 {
		panic("interval can not be zero")
	} else if offsetHours < 0 {
		panic("invalid offset hours")
	} else if interval < time.Duration(offsetHours)*time.Hour {
		offsetHours = 0
	}

	if success {
		randomOffset := time.Duration(frand.Intn(2*offsetHours+1)-offsetHours) * time.Hour
		return lastScan.Add(interval).Add(randomOffset)
	}

	expBackoff := time.Duration(min(expBackoffHours*consecScanFailures, expBackoffHoursMax)) * time.Hour
	return lastScan.Add(expBackoff)
}

// resolveHost uses the given resolver to resolve the given list of host
// addresses. It returns a filtered list of addresses, leaving out any invalid
// and/or private addresses, and a list of networks in CIDR notation. The only
// error this function returns is [context.Canceled], other errors that occur
// during the resolving and parsing are debug logged but otherwise ignored.
func resolveHost(ctx context.Context, resolver Resolver, locator Locator, addresses []chain.NetAddress, log *zap.Logger) ([]chain.NetAddress, geoip.Location, error) {
	var filtered []chain.NetAddress
	var loc geoip.Location
	for _, na := range addresses {
		host, _, err := net.SplitHostPort(na.Address)
		if err != nil {
			log.Debug("failed to split host port", zap.String("address", na.Address), zap.Error(err))
			continue
		}

		ips, err := resolver.LookupIPAddr(ctx, host)
		if errors.Is(err, context.Canceled) {
			return nil, geoip.Location{}, err
		} else if err != nil {
			log.Debug("failed to resolve host", zap.String("host", host), zap.Error(err))
			continue
		} else if len(ips) == 0 {
			log.Debug("no IPs found for host", zap.String("host", host))
			continue
		}

		var hasPrivateIP bool
		for _, ip := range ips {
			if isPrivateIP(ip.IP) {
				hasPrivateIP = true
				break
			}
		}
		if hasPrivateIP {
			log.Debug("host has private IP", zap.String("host", host))
			continue
		}

		if loc.CountryCode == "" {
			loc, err = locator.Locate(ips[0].IP)
			if err != nil {
				log.Debug("failed to locate IP address", zap.Error(err))
			}
		}

		filtered = append(filtered, na)
	}

	return filtered, loc, nil
}

func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	privateIPBlocks := []net.IPNet{
		{IP: net.IPv4(10, 0, 0, 0), Mask: net.CIDRMask(8, 32)},
		{IP: net.IPv4(172, 16, 0, 0), Mask: net.CIDRMask(12, 32)},
		{IP: net.IPv4(192, 168, 0, 0), Mask: net.CIDRMask(16, 32)},
		{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}, // CGNAT
		{IP: net.ParseIP("fc00::"), Mask: net.CIDRMask(7, 128)},
		{IP: net.ParseIP("fe80::"), Mask: net.CIDRMask(10, 128)},
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func validateAddress(na chain.NetAddress) error {
	if !(na.Protocol == siamux.Protocol || na.Protocol == quic.Protocol) {
		return fmt.Errorf("unknown protocol %q", na.Protocol)
	}
	if na.Address == "" {
		return fmt.Errorf("empty address")
	}
	return nil
}
