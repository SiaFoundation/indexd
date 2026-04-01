package client

import (
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func TestRPCAverage(t *testing.T) {
	var ra rpcAverage

	if ra.Value() != 0 {
		t.Fatal("initial value should be zero")
	}

	ra.AddSample(100)
	if v := ra.Value(); v != 100 {
		t.Fatalf("expected 100, got %f", v)
	}

	ra.AddSample(200)
	expected := 0.2*200 + 0.8*100
	if v := ra.Value(); v != expected {
		t.Fatalf("expected %f, got %f", expected, v)
	}
}

func TestFailureRate(t *testing.T) {
	var fr failureRate

	if fr.Value() != 0 {
		t.Fatal("initial value should be zero")
	}

	fr.AddSample(true)
	if v := fr.Value(); v != 0 {
		t.Fatalf("expected 0, got %f", v)
	}

	fr.AddSample(false)
	expected := 0.2
	if v := fr.Value(); v != expected {
		t.Fatalf("expected %f, got %f", expected, v)
	}
}

func TestFailureRateTimeDecay(t *testing.T) {
	const (
		minutesBetweenDecays = 5
		totalDecayMinutes    = 10
	)
	synctest.Test(t, func(t *testing.T) {
		var fr failureRate

		fr.AddSample(false)
		expected := 1.0
		if v := fr.Value(); v != expected {
			t.Fatalf("expected %f, got %f", expected, v)
		}

		time.Sleep(totalDecayMinutes * time.Minute)
		synctest.Wait()

		decayFactor := math.Pow(1.0-emaAlpha, totalDecayMinutes/minutesBetweenDecays)
		expected *= decayFactor
		if v := fr.Value(); v != expected {
			t.Fatalf("expected %f, got %f", expected, v)
		}
	})
}
