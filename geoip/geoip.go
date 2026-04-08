package geoip

import (
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/oschwald/geoip2-golang"
	"go.uber.org/zap"
)

const (
	radiusKm = 6371.0088

	// maxMindCityDBURL is the URL to download the GeoLite2-City database from.
	maxMindCityDBURL = "https://sia.tech/api/media/file/GeoLite2-City.mmdb"
	// maxMindCityDBFilename is the filename of the GeoLite2-City database.
	maxMindCityDBFilename = "GeoLite2-City.mmdb"
)

// A Location represents an ISO 3166-1 A-2 country codes and an approximate
// latitude/longitude.
type Location struct {
	CountryCode string `json:"countryCode"`

	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// HaversineDistanceKm returns the great-circle distance between the location and
// the other location in kilometers.
func (l Location) HaversineDistanceKm(other Location) float64 {
	φ1 := l.Latitude * math.Pi / 180
	φ2 := other.Latitude * math.Pi / 180
	dφ := (other.Latitude - l.Latitude) * math.Pi / 180
	dλ := (other.Longitude - l.Longitude) * math.Pi / 180

	sinDφ := math.Sin(dφ / 2)
	sinDλ := math.Sin(dλ / 2)
	a := sinDφ*sinDφ + math.Cos(φ1)*math.Cos(φ2)*sinDλ*sinDλ
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return radiusKm * c
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

// downloadMaxMindDB downloads the GeoLite2-City database to the given path.
func downloadMaxMindDB(path string) error {
	resp, err := http.Get(maxMindCityDBURL)
	if err != nil {
		return fmt.Errorf("failed to download MaxMind database: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download MaxMind database: unexpected status %d", resp.StatusCode)
	}

	f, err := os.CreateTemp(filepath.Dir(path), ".geolite2-*.mmdb.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("failed to write MaxMind database: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	} else if err := os.Rename(f.Name(), path); err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	return nil
}

// NewMaxMindLocator returns a Locator that uses an underlying MaxMind
// database. The dataDir is checked for a GeoLite2-City.mmdb file. If
// one does not exist, it is downloaded from sia.tech.
func NewMaxMindLocator(dataDir string, log *zap.Logger) (Locator, error) {
	path := filepath.Join(dataDir, maxMindCityDBFilename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Info("downloading GeoLite2 database", zap.String("url", maxMindCityDBURL), zap.String("path", path))
		if err := downloadMaxMindDB(path); err != nil {
			return nil, err
		}
		log.Info("GeoLite2 database downloaded successfully")
	} else if err != nil {
		return nil, fmt.Errorf("failed to stat MaxMind database: %w", err)
	}

	db, err := geoip2.Open(path)
	if err != nil {
		return nil, err
	}
	return &maxMindLocator{db: db}, nil
}
