package app

import (
	"net"
	"net/http"
	"sync"
	"time"
)

type ipRateLimiter struct {
	mu sync.Mutex

	limit  int
	window time.Duration

	counts    map[string]int
	lastReset time.Time
}

func newIPRateLimiter(limit int, window time.Duration) *ipRateLimiter {
	return &ipRateLimiter{
		limit:     limit,
		window:    window,
		counts:    make(map[string]int),
		lastReset: time.Now().Add(window),
	}
}

// allow returns true if the request from ip is within the rate limit
func (rl *ipRateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if time.Now().After(rl.lastReset) {
		rl.counts = make(map[string]int)
		rl.lastReset = time.Now().Add(rl.window)
	}

	if rl.counts[ip] >= rl.limit {
		return false
	}
	rl.counts[ip]++
	return true
}

// clientIP extracts the client IP from the request.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
