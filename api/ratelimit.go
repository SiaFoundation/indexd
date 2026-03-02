package api

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.sia.tech/jape"
	"golang.org/x/time/rate"
)

type (
	// A RateLimiter allows or denies a request for the given key.
	RateLimiter interface {
		Allow(key string) bool
	}

	limiterEntry struct {
		limiter  *rate.Limiter
		lastSeen time.Time
	}

	// An IPRateLimiter is a per-key rate limiter using token buckets.
	// Each unique key gets its own bucket. Stale entries are pruned
	// lazily on access.
	IPRateLimiter struct {
		mu      sync.Mutex // protects entries
		entries map[string]*limiterEntry
		limit   rate.Limit
		burst   int
		ttl     time.Duration
	}
)

// NewIPRateLimiter creates a new per-key rate limiter. every is the minimum
// time between requests in steady state, burst is the maximum number of
// requests allowed at once, and ttl is how long a bucket is retained
// after its last request before being pruned.
func NewIPRateLimiter(every time.Duration, burst int, ttl time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		entries: make(map[string]*limiterEntry),
		limit:   rate.Every(every),
		burst:   burst,
		ttl:     ttl,
	}
}

// prune removes entries that haven't been seen within the TTL. Must be
// called with rl.mu held.
func (rl *IPRateLimiter) prune(now time.Time) {
	for key, e := range rl.entries {
		if now.Sub(e.lastSeen) > rl.ttl {
			delete(rl.entries, key)
		}
	}
}

// Allow returns true if the request for the given key is within the
// rate limit.
func (rl *IPRateLimiter) Allow(key string) bool {
	now := time.Now()

	rl.mu.Lock()
	e, ok := rl.entries[key]
	if !ok {
		rl.prune(now)
		e = &limiterEntry{limiter: rate.NewLimiter(rl.limit, rl.burst)}
		rl.entries[key] = e
	}
	e.lastSeen = now
	rl.mu.Unlock()

	return e.limiter.Allow()
}

// WrapRateLimit returns a jape middleware that rate limits requests
// based on client IP. If rl is nil the handler is returned unmodified.
func WrapRateLimit(rl RateLimiter, next jape.Handler) jape.Handler {
	if rl == nil {
		return next
	}
	return func(jc jape.Context) {
		if !rl.Allow(clientIP(jc.Request)) {
			jc.Error(errors.New("too many requests"), http.StatusTooManyRequests)
			return
		}
		next(jc)
	}
}

// clientIP extracts the client IP from the request. It checks
// X-Forwarded-For first (for reverse proxy deployments) and falls back
// to RemoteAddr. IPv6 addresses are normalized to their /64 subnet to
// prevent attackers from rotating addresses within a subnet.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// use the first (leftmost) IP, which is the original client
		if i := strings.IndexByte(xff, ','); i != -1 {
			xff = xff[:i]
		}
		if ip := strings.TrimSpace(xff); ip != "" {
			return normalizeIP(ip)
		}
	}

	// fall back to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return normalizeIP(r.RemoteAddr)
	}
	return normalizeIP(host)
}

// normalizeIP returns the IP string for rate limiting. IPv6 addresses
// are masked to /64 to prevent subnet rotation attacks.
func normalizeIP(s string) string {
	ip := net.ParseIP(s)
	if ip == nil {
		return s
	}
	if ip.To4() != nil {
		return ip.To4().String()
	}
	// mask IPv6 to /64 subnet
	return ip.Mask(net.CIDRMask(64, 128)).String()
}
