package hosts

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"slices"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
)

// resolver is the default implementation of Resolver.  It will attempt to
// lookup a hostname with the `main` resolver first.  If that fails for a
// non context related reason, then it will try the `fallback` resolver.
type resolver struct {
	main     Resolver
	fallback Resolver
}

// LookupIPAddr implements [Resolver].
func (r *resolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	addrs, err := r.main.LookupIPAddr(ctx, host)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	} else if err != nil {
		// try fallback if main fails
		return r.fallback.LookupIPAddr(ctx, host)
	}
	return addrs, nil
}

// scanner is the default implementation of Scanner.
type scanner struct{}

// ScanSiamux executes the RPCSettings RPC on the host and returns its settings
// using the SiaMux protocol.
func (c *scanner) ScanSiamux(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
	t, err := siamux.Dial(ctx, addr, hk)
	if err != nil {
		return proto4.HostSettings{}, fmt.Errorf("failed to upgrade connection: %w", err)
	}
	defer t.Close()

	return rhp.RPCSettings(ctx, t)
}

// ScanQuic executes the RPCSettings RPC on the host and returns its settings
// using the QUIC protocol.
func (c *scanner) ScanQuic(ctx context.Context, hk types.PublicKey, addr string) (proto4.HostSettings, error) {
	t, err := quic.Dial(ctx, addr, hk)
	if err != nil {
		return proto4.HostSettings{}, fmt.Errorf("failed to upgrade connection: %w", err)
	}
	defer t.Close()

	return rhp.RPCSettings(ctx, t)
}

var connectivitySites = []string{
	"1.1.1.1:443", // Cloudflare
	"www.google.com:443",
	"www.amazon.com:443",
}

type onlineChecker struct {
	addresses []string

	mu          sync.Mutex
	online      bool
	lastChecked time.Time
}

// IsOnline reports whether any of the connectivity sites are reachable. Results
// are cached for onlineCheckInterval.
func (p *onlineChecker) IsOnline() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if time.Since(p.lastChecked) >= onlineCheckInterval {
		p.online = slices.ContainsFunc(p.addresses, isReachable)
		p.lastChecked = time.Now()
	}
	return p.online
}

// isReachable attempts to establish a TCP connection to the given host with a short timeout.
func isReachable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
