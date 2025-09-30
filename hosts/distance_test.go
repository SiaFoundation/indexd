package hosts

import (
	"testing"

	"go.sia.tech/core/types"
)

func TestSpacedSet(t *testing.T) {
	// Distances (approx):
	// Brussels–Ghent: ~50 km
	// Brussels–Liège: ~90 km
	// Ghent–Liège: ~125 km

	brussels := Host{
		PublicKey:   types.PublicKey{1},
		CountryCode: "BE",
		Latitude:    50.8503,
		Longitude:   4.3517,
	} // Brussels

	ghent := Host{
		PublicKey:   types.PublicKey{2},
		CountryCode: "BE",
		Latitude:    51.0543,
		Longitude:   3.7174,
	} // Ghent
	liege := Host{
		PublicKey:   types.PublicKey{3},
		CountryCode: "BE",
		Latitude:    50.6326,
		Longitude:   5.5797,
	} // Liège

	// assert behaviour of the spaced set with a min distance of 100 km
	s := NewSpacedSet(100)
	if !s.Add(brussels) {
		t.Fatal("expected to add Brussels")
	} else if s.Add(ghent) {
		t.Fatal("expected Ghent to be rejected (<100 km from Brussels)")
	} else if s.Add(liege) {
		t.Fatal("expected Liège to be rejected (<100 km from Brussels)")
	}

	// lower threshold to 45km
	s2 := NewSpacedSet(45)
	if !s2.Add(brussels) {
		t.Fatal("expected to add Brussels")
	} else if !s2.Add(ghent) {
		t.Fatal("expected Ghent to be accepted (~50 km from Brussels)")
	} else if !s2.Add(liege) {
		t.Fatal("expected Liège to be accepted (~90–125 km from others)")
	}

	// assert behaviour of the spaced set with a min distance of 0 km (only uniqueness)
	s3 := NewSpacedSet(0)
	if !s3.Add(brussels) {
		t.Fatal("expected to add Brussels")
	} else if !s3.Add(ghent) {
		t.Fatal("expected to add Ghent (0 km min distance)")
	} else if !s3.Add(liege) {
		t.Fatal("expected to add Liège (0 km min distance)")
	} else if s3.Add(brussels) {
		t.Fatal("expected Brussels to be rejected (duplicate)")
	}
}
