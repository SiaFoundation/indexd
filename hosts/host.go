package hosts

import (
	"errors"
	"net"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

var (
	// ErrNotFound is returned by database operations that fail due to a host
	// not being found.
	ErrNotFound = errors.New("host not found")
)

type (
	// Host is a host on the network.
	Host struct {
		PublicKey              types.PublicKey     `json:"publicKey"`
		LastAnnouncement       time.Time           `json:"lastAnnouncement"`
		LastFailedScan         time.Time           `json:"lastFailedScan"`
		LastSuccessfulScan     time.Time           `json:"lastSuccessfulScan"`
		NextScan               time.Time           `json:"nextScan"`
		ConsecutiveFailedScans int                 `json:"consecutiveFailedScans"`
		RecentUptime           float64             `json:"recentUptime"`
		Addresses              []chain.NetAddress  `json:"addresses"`
		Networks               []net.IPNet         `json:"networks"`
		Settings               proto4.HostSettings `json:"settings"`
		Usability              Usability           `json:"usability"`
		Blocked                bool                `json:"blocked"`
	}

	// Usability represents a series of host checks that can be used to
	// determine whether the host is usable or not.
	Usability struct {
		Uptime              bool `json:"uptime"`
		MaxContractDuration bool `json:"maxContractDuration"`
		MaxCollateral       bool `json:"maxCollateral"`
		ProtocolVersion     bool `json:"protocolVersion"`
		PriceValidity       bool `json:"priceValidity"`
		AcceptingContracts  bool `json:"acceptingContracts"`

		ContractPrice   bool `json:"contractPrice"`
		Collateral      bool `json:"collateral"`
		StoragePrice    bool `json:"storagePrice"`
		IngressPrice    bool `json:"ingressPrice"`
		EgressPrice     bool `json:"egressPrice"`
		FreeSectorPrice bool `json:"freeSectorPrice"`
	}
)

// Usable returns true if all checks passed.
func (u Usability) Usable() bool {
	return u.Uptime && u.MaxContractDuration && u.MaxCollateral && u.ProtocolVersion && u.PriceValidity && u.AcceptingContracts &&
		u.ContractPrice && u.Collateral && u.StoragePrice && u.IngressPrice && u.EgressPrice && u.FreeSectorPrice
}
