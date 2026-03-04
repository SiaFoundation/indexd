package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// An IPRateLimiter is a per-key rate limiter using token buckets.
// Each unique key gets its own bucket that is automatically pruned
// after the TTL expires.
type IPRateLimiter struct {
	mu       sync.Mutex // protects limiters
	limiters map[string]*rate.Limiter
	limit    rate.Limit
	burst    int
	ttl      time.Duration
}

// NewIPRateLimiter creates a new per-key rate limiter. every is the minimum
// time between requests in steady state, burst is the maximum number of
// requests allowed at once, and ttl is how long a bucket is retained
// after creation before being pruned.
func NewIPRateLimiter(every time.Duration, burst int, ttl time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		limit:    rate.Every(every),
		burst:    burst,
		ttl:      ttl,
	}
}

// Allow returns true if the request for the given key is within the
// rate limit.
func (rl *IPRateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	l, ok := rl.limiters[key]
	if !ok {
		l = rate.NewLimiter(rl.limit, rl.burst)
		rl.limiters[key] = l
		time.AfterFunc(rl.ttl, func() {
			rl.mu.Lock()
			delete(rl.limiters, key)
			rl.mu.Unlock()
		})
	}
	rl.mu.Unlock()
	return l.Allow()
}

// ClientIP extracts the client IP from the request. It checks
// X-Forwarded-For first (for reverse proxy deployments) and falls back
// to RemoteAddr. IPv6 addresses are normalized to their /64 subnet to
// prevent attackers from rotating addresses within a subnet.
func ClientIP(r *http.Request) string {
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
