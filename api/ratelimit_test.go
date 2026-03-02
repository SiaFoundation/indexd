package api

import (
	"testing"
	"time"
)

func TestRateLimiter(t *testing.T) {
	// 10 req/s with burst of 3
	rl := NewIPRateLimiter(100*time.Millisecond, 3, time.Minute)

	// first 3 requests should succeed (burst)
	for i := 0; i < 3; i++ {
		if !rl.Allow("1.2.3.4") {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	// 4th request from same IP should be rejected (burst exhausted)
	if rl.Allow("1.2.3.4") {
		t.Fatal("4th request should be rejected")
	}

	// different IP should still be allowed
	if !rl.Allow("5.6.7.8") {
		t.Fatal("different IP should be allowed")
	}

	// after tokens refill, should be allowed again
	time.Sleep(150 * time.Millisecond)
	if !rl.Allow("1.2.3.4") {
		t.Fatal("request should be allowed after token refill")
	}
}
