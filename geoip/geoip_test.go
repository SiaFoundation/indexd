package geoip

import (
	"math"
	"testing"
)

func TestHaversineDistance(t *testing.T) {
	almostEqual := func(a, b, tol float64) bool {
		return math.Abs(a-b) <= tol
	}

	tests := []struct {
		name      string
		a, b      Location
		expected  float64
		tolerance float64
	}{
		{
			name:      "same point is zero",
			a:         Location{Latitude: 50.8503, Longitude: 4.3517}, // Brussels
			b:         Location{Latitude: 50.8503, Longitude: 4.3517},
			expected:  0,
			tolerance: 1e-9, // zero tolerance
		},
		{
			name:      "1 degree of longitude at equator ≈ 111.19508 km",
			a:         Location{Latitude: 0, Longitude: 0},
			b:         Location{Latitude: 0, Longitude: 1},
			expected:  111.1950802335329,
			tolerance: 1e-4, // ~0.1 meters for 1° at equator
		},
		{
			name:      "Brussels ↔ Antwerp ≈ 41.1955 km",
			a:         Location{Latitude: 50.8503, Longitude: 4.3517},
			b:         Location{Latitude: 51.2194, Longitude: 4.4025},
			expected:  41.19553412569231,
			tolerance: 1e-4, // ~50 meters tolerance for city pair
		},
		{
			name:      "antipodes (0,0) ↔ (0,180) = πR",
			a:         Location{Latitude: 0, Longitude: 0},
			b:         Location{Latitude: 0, Longitude: 180},
			expected:  math.Pi * radiusKm,
			tolerance: 1e-9, // zero tolerance
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.a.HaversineDistanceKm(tc.b)
			if !almostEqual(got, tc.expected, tc.tolerance) {
				t.Fatalf("HaversineDistance(%+v, %+v) = %.12f km, want %.12f km (±%.6f)",
					tc.a, tc.b, got, tc.expected, tc.tolerance)
			}
			// Symmetry check: d(a,b) == d(b,a)
			gotBA := tc.b.HaversineDistanceKm(tc.a)
			if !almostEqual(got, gotBA, tc.tolerance) {
				t.Fatalf("distance not symmetric: d(a,b)=%.12f, d(b,a)=%.12f (±%.6f)",
					got, gotBA, tc.tolerance)
			}
		})
	}
}
