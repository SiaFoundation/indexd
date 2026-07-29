package client

import (
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func TestRPCAverage(t *testing.T) {
	var ra rpcAverage

	if _, sampled := ra.Value(); sampled {
		t.Fatal("unsampled average should report sampled=false")
	}

	ra.AddSample(100)
	if v, sampled := ra.Value(); !sampled || v != 100 {
		t.Fatalf("expected 100 sampled, got %f sampled=%v", v, sampled)
	}

	ra.AddSample(200)
	expected := 0.2*200 + 0.8*100
	if v, sampled := ra.Value(); !sampled || v != expected {
		t.Fatalf("expected %f sampled, got %f sampled=%v", expected, v, sampled)
	}
}

func TestFailureRate(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var fr failureRate

			if fr.Value() != 0 {
				t.Fatal("initial value should be zero")
			}

			fr.AddSample(true)
			if v := fr.Value(); v != 0 {
				t.Fatalf("expected 0, got %f", v)
			}

			fr.AddSample(false)
			if v := fr.Value(); v != emaAlpha {
				t.Fatalf("expected %f, got %f", emaAlpha, v)
			}

			for range 13 {
				fr.AddSample(true)
			}
			if v := fr.Value(); v == 0 {
				t.Fatal("expected value just above the threshold to be reported")
			}

			fr.AddSample(true)
			if v := fr.Value(); v != 0 {
				t.Fatalf("expected samples below the threshold to be clamped to zero, got %f", v)
			}
		})
	})

	t.Run("TimeDecay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var fr failureRate

			fr.AddSample(false)
			if v := fr.Value(); v != 1 {
				t.Fatalf("expected 1, got %f", v)
			}

			time.Sleep(failureRateHalfLife - time.Second)
			if v := fr.Value(); v != 1 {
				t.Fatalf("expected no decay before the half-life, got %f", v)
			}

			time.Sleep(time.Second)
			if v := fr.Value(); v != math.Ldexp(1, -1) {
				t.Fatalf("expected 0.5 after one half-life, got %f", v)
			}

			time.Sleep(5 * failureRateHalfLife)
			if v := fr.Value(); v != math.Ldexp(1, -6) {
				t.Fatalf("expected value above threshold after six half-lives, got %f", v)
			}

			time.Sleep(failureRateHalfLife)
			if v := fr.Value(); v != 0 {
				t.Fatalf("expected failure rate to be forgiven after seven half-lives, got %f", v)
			}
		})
	})

	t.Run("DecayBeforeSample", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var fr failureRate

			fr.AddSample(false)
			time.Sleep(failureRateHalfLife)
			// AddSample must decay the idle value before applying the EMA,
			// without any intervening Value call doing the decay for it.
			fr.AddSample(false)

			decayed := math.Ldexp(1, -1)
			expected := emaAlpha*1.0 + (1.0-emaAlpha)*decayed
			if v := fr.Value(); v != expected {
				t.Fatalf("expected %f, got %f", expected, v)
			}
		})
	})

	t.Run("ForgivesSingleFailure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var fr failureRate

			fr.AddSample(true)
			fr.AddSample(false)
			if v := fr.Value(); v != emaAlpha {
				t.Fatalf("expected %f, got %f", emaAlpha, v)
			}

			time.Sleep(5 * failureRateHalfLife)
			if v := fr.Value(); v != 0 {
				t.Fatalf("expected a single failure to be forgiven, got %f", v)
			}
		})
	})
}
