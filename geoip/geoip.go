package geoip

import (
	_ "embed" // needed for geolocation database
	"math"
	"net"
	"sync"

	"github.com/oschwald/geoip2-golang"
)

//go:embed GeoLite2-City.mmdb
var maxMindCityDB []byte

const radiusKm = 6371.0088

type distanceUnit int

const (
	// Kilometers is a distance unit in kilometers.
	Kilometers distanceUnit = iota
	// Miles is a distance unit in miles.
	Miles
)

type (
	// Distance represents a distance measurement in a specific unit.
	Distance struct {
		value float64
		unit  distanceUnit
	}
)

// Km returns a Distance representing the provided number of kilometers.
func Km(v float64) Distance { return Distance{value: v, unit: Kilometers} }

// MilesD returns a Distance representing the provided number of miles.
func MilesD(v float64) Distance { return Distance{value: v, unit: Miles} }

// Compare compares two distances, returning -1 if d < other, 1 if d > other, and
// 0 if they are equal.
func (d Distance) Compare(other Distance) int {
	dKm := d.ToKm()
	otherKm := other.ToKm()

	switch {
	case dKm < otherKm:
		return -1
	case dKm > otherKm:
		return 1
	default:
		return 0
	}
}

// LessThan returns true if the receiver is smaller than the other.
func (d Distance) LessThan(other Distance) bool {
	return d.Compare(other) < 0
}

// IsZero returns true if the distance is zero.
func (d Distance) IsZero() bool {
	return d.value == 0
}

// ToKm converts the distance to kilometers.
func (d Distance) ToKm() float64 {
	switch d.unit {
	case Kilometers:
		return d.value
	case Miles:
		return d.value * 1.60934
	default:
		panic("unknown distance unit")
	}
}

// A Location represents an ISO 3166-1 A-2 country codes and an approximate
// latitude/longitude.
type Location struct {
	CountryCode string `json:"countryCode"`

	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// HaversineDistance returns the great-circle distance between the location and
// the other location in kilometers.
func (l Location) HaversineDistance(other Location) Distance {
	φ1 := l.Latitude * math.Pi / 180
	φ2 := other.Latitude * math.Pi / 180
	dφ := (other.Latitude - l.Latitude) * math.Pi / 180
	dλ := (other.Longitude - l.Longitude) * math.Pi / 180

	sinDφ := math.Sin(dφ / 2)
	sinDλ := math.Sin(dλ / 2)
	a := sinDφ*sinDφ + math.Cos(φ1)*math.Cos(φ2)*sinDλ*sinDλ
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return Km(radiusKm * c)
}

// A Locator maps IP addresses to their location.
// It is assumed that it implementations are thread-safe.
type Locator interface {
	// Close closes the Locator.
	Close() error
	// Locate maps IP addresses to a Location.
	Locate(ip net.IP) (Location, error)
}

type maxMindLocator struct {
	mu sync.Mutex

	db *geoip2.Reader
}

// Locate implements Locator.
func (m *maxMindLocator) Locate(addr net.IP) (Location, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, err := m.db.City(addr)
	if err != nil {
		return Location{}, err
	}
	return Location{
		CountryCode: record.Country.IsoCode,
		Latitude:    record.Location.Latitude,
		Longitude:   record.Location.Longitude,
	}, nil
}

// Close implements Locator.
func (m *maxMindLocator) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.db.Close()
}

// NewMaxMindLocator returns a Locator that uses an underlying MaxMind
// database.  If no path is provided, a default embedded GeoLite2-City database
// is used.
func NewMaxMindLocator(path string) (Locator, error) {
	var db *geoip2.Reader
	var err error
	if path == "" {
		db, err = geoip2.FromBytes(maxMindCityDB)
	} else {
		db, err = geoip2.Open(path)
	}
	if err != nil {
		return nil, err
	}

	return &maxMindLocator{db: db}, nil
}
