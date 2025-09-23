package hosts

import (
	"go.sia.tech/indexd/geoip"
)

// SpacedSet is a set of hosts that are sufficiently spaced apart based on a
// minimum distance. A spaced set is not thread-safe.
type SpacedSet struct {
	minDistance geoip.Distance
	selected    []Host
}

// NewSpacedSet creates a new SpacedSet with the given minimum distance.
func NewSpacedSet(minDistance geoip.Distance) *SpacedSet {
	return &SpacedSet{minDistance: minDistance}
}

// Add adds the host to the set if it is sufficiently spaced apart from the
// existing hosts. It returns true if the host was added, false otherwise.
func (s *SpacedSet) Add(h Host) bool {
	if s.CanAddHost(h) {
		s.selected = append(s.selected, h)
		return true
	}
	return false
}

// CanAddHost returns true if the host is sufficiently far removed from the
// existing hosts in the set. If the minimum distance is zero, it only checks
// for uniqueness.
func (s *SpacedSet) CanAddHost(h Host) bool {
	if s.minDistance.IsZero() {
		return !s.contains(h)
	}

	location := h.Location()
	for _, other := range s.selected {
		distance := location.HaversineDistance(other.Location())
		if distance.LessThan(s.minDistance) {
			return false
		}
	}

	return true
}

func (s *SpacedSet) contains(h Host) bool {
	for _, other := range s.selected {
		if h.PublicKey == other.PublicKey {
			return true
		}
	}
	return false
}
