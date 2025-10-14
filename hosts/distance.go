package hosts

import (
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/geoip"
)

// SpacedSet is a set of hosts that are sufficiently spaced apart based on a
// minimum distance. A spaced set is not thread-safe.
type SpacedSet struct {
	minDistanceKm float64
	selected      map[types.PublicKey]geoip.Location
}

// NewSpacedSet creates a new SpacedSet with the given minimum distance.
func NewSpacedSet(minDistanceKm float64) *SpacedSet {
	return &SpacedSet{
		minDistanceKm: minDistanceKm,
		selected:      make(map[types.PublicKey]geoip.Location),
	}
}

// Add adds the host to the set if it is sufficiently spaced apart from the
// existing hosts. It returns true if the host was added, false otherwise.
func (s *SpacedSet) Add(h HostInfo) bool {
	if s.CanAddHost(h) {
		s.selected[h.PublicKey] = h.Location()
		return true
	}
	return false
}

// CanAddHost returns true if the host is sufficiently far removed from the
// existing hosts in the set. Uniqueness is always enforced.
func (s *SpacedSet) CanAddHost(h HostInfo) bool {
	if _, exists := s.selected[h.PublicKey]; exists {
		return false
	}

	if s.minDistanceKm > 0 {
		location := h.Location()
		for _, other := range s.selected {
			distance := location.HaversineDistanceKm(other)
			if distance < s.minDistanceKm {
				return false
			}
		}
	}

	return true
}
