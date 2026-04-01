package sdk

import (
	"testing"
	"time"
)

func TestUploadTimeout(t *testing.T) {
	tests := []struct {
		attempts int
		expected time.Duration
	}{
		{1, 15 * time.Second},
		{2, 20 * time.Second},
		{3, 25 * time.Second},
		{10, 60 * time.Second},
		{22, 120 * time.Second},
		{23, 120 * time.Second},
		{100, 120 * time.Second},
	}

	for _, tc := range tests {
		got := uploadTimeout(tc.attempts)
		if got != tc.expected {
			t.Errorf("uploadTimeout(%d) = %v, expected %v", tc.attempts, got, tc.expected)
		}
	}
}
