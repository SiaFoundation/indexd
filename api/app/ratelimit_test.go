package app

import (
	"testing"
	"time"
)

func TestIPRateLimiter(t *testing.T) {
	const interval = 100 * time.Millisecond
	rl := newIPRateLimiter(3, interval)

	// first 3 requests should succeed
	for i := 0; i < 3; i++ {
		if !rl.allow("1.2.3.4") {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	// 4th request from same IP should be rejected
	if rl.allow("1.2.3.4") {
		t.Fatal("4th request should be rejected")
	}

	// different IP should still be allowed
	if !rl.allow("5.6.7.8") {
		t.Fatal("different IP should be allowed")
	}

	// after the window expires, should be allowed again
	time.Sleep(interval)
	if !rl.allow("1.2.3.4") {
		t.Fatal("request should be allowed after window expires")
	}
}
