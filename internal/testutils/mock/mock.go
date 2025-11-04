package mock

import (
	"net"

	"go.sia.tech/indexd/geoip"
)

// Location is the location that will always be returned by the
// mock locator.
var Location = geoip.Location{
	CountryCode: "US",
	Latitude:    10,
	Longitude:   -20,
}

// Locator is a geoip.Locator that always returns the same location.
type Locator struct{}

// Close implements geoip.Locator.
func (m *Locator) Close() error {
	return nil
}

// Locate implements geoip.Locator.
func (m *Locator) Locate(addr net.IP) (geoip.Location, error) {
	return Location, nil
}
