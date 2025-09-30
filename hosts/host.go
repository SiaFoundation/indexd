package hosts

import (
	"errors"
	"strings"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/geoip"
)

const (
	blocksPerMonth = 144 * 30
	oneTB          = 1e12
)

var (
	// ErrNotFound is returned by database operations that fail due to a host
	// not being found.
	ErrNotFound = errors.New("host not found")
)
var (
	// DefaultHostsQueryOpts re the default options applied when querying hosts. By
	// default no hosts are filtered out.
	DefaultHostsQueryOpts = hostsQueryOpts{
		Blocked: nil,
		Good:    nil,
	}

	// DefaultUsabilitySettings are the default settings used to determine
	// whether a host is usable or not. These settings are configured in the
	// database as defaults when the global settings are initialized.
	DefaultUsabilitySettings = UsabilitySettings{
		MaxEgressPrice:     types.Siacoins(3000).Div64(oneTB),                       // 3000 SC / TB
		MaxIngressPrice:    types.Siacoins(3000).Div64(oneTB),                       // 3000 SC / TB
		MaxStoragePrice:    types.Siacoins(3000).Div64(oneTB).Div64(blocksPerMonth), // 3000 SC / TB / month
		MinCollateral:      types.Siacoins(100).Div64(oneTB).Div64(blocksPerMonth),  // 100 SC / TB / month
		MinProtocolVersion: rhp.ProtocolVersion501,
	}
)

type (
	// HostQueryOpt is a functional option for querying hosts.
	HostQueryOpt func(*hostsQueryOpts)

	hostsQueryOpts struct {
		ActiveContracts *bool // return hosts that have active contracts or not
		Blocked         *bool // return (un)blocked hosts
		Good            *bool // return good/bad hosts
	}
)

// WithUsable causes only usable or unusable hosts being returned depending on
// whether 'usable' is true or false.
func WithUsable(usable bool) HostQueryOpt {
	return func(opts *hostsQueryOpts) {
		opts.Good = &usable
	}
}

// WithBlocked causes only blocked or unblocked hosts being returned depending
// on whether 'blocked' is true or false.
func WithBlocked(blocked bool) HostQueryOpt {
	return func(opts *hostsQueryOpts) {
		opts.Blocked = &blocked
	}
}

// WithActiveContracts causes only hosts with contracts in the provided state
// being returned
func WithActiveContracts(activeContracts bool) HostQueryOpt {
	return func(opts *hostsQueryOpts) {
		opts.ActiveContracts = &activeContracts
	}
}

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
		CountryCode            string              `json:"countryCode"`
		Latitude               float64             `json:"latitude"`
		Longitude              float64             `json:"longitude"`
		Settings               proto4.HostSettings `json:"settings"`
		Usability              Usability           `json:"usability"`
		Blocked                bool                `json:"blocked"`
		BlockedReason          string              `json:"blockedReason"`
		LostSectors            uint64              `json:"lostSectors"`
		AccountFunding         types.Currency      `json:"accountFunding"`
		TotalSpent             types.Currency      `json:"totalSpent"`
	}

	// HostInfo is a subset of the Host struct that contains only the public
	// key and addresses. It is used for listing usable hosts in the
	// application API.
	HostInfo struct {
		PublicKey types.PublicKey    `json:"publicKey"`
		Addresses []chain.NetAddress `json:"addresses"`

		CountryCode string  `json:"countryCode"`
		Latitude    float64 `json:"latitude"`
		Longitude   float64 `json:"longitude"`
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

	// UsabilitySettings contains the settings that are used to check if a host
	// is usable.
	UsabilitySettings struct {
		// MaxEgressPrice is the maximum egress price in Hastings per byte that
		// a host can charge before we consider the host bad.
		MaxEgressPrice types.Currency `json:"maxEgressPrice"`

		// MaxIngressPrice is the maximum ingress price in Hastings per byte that
		// a host can charge before we consider the host bad.
		MaxIngressPrice types.Currency `json:"maxIngressPrice"`

		// MaxStoragePrice is the maximum storage price in Hastings per byte
		// per block that a host can charge before we consider the host bad.
		MaxStoragePrice types.Currency `json:"maxStoragePrice"`

		// MinCollateral is the minimum collateral in Hastings per byte per
		// block a host needs to allocate to be considered good in addition to a
		// 2x minimum ratio between storage price and collateral.
		// The minimum collateral protects us against hosts with a very low
		// storage price not having incentive to keep our data stored while the
		// ratio makes sure that a host burns more funds for losing data than
		// the renter paid for storing it.
		MinCollateral types.Currency `json:"minCollateral"`

		// MinProtocolVersion is the minimum protocol version that a host must
		// support to be considered good.
		MinProtocolVersion proto4.ProtocolVersion `json:"minProtocolVersion"`
	}
)

// IsGood returns true if the host is considered good for storing data.
func (h *Host) IsGood() bool {
	return h.Usability.Usable() && !h.Blocked
}

// Location returns the geoip.Location of the host.
func (h *Host) Location() geoip.Location {
	return geoip.Location{
		CountryCode: h.CountryCode,
		Latitude:    h.Latitude,
		Longitude:   h.Longitude,
	}
}

// GoodUsability is the usability struct indicating that all checks passed.
var GoodUsability = Usability{
	Uptime:              true,
	MaxContractDuration: true,
	MaxCollateral:       true,
	ProtocolVersion:     true,
	PriceValidity:       true,
	AcceptingContracts:  true,

	ContractPrice:   true,
	Collateral:      true,
	StoragePrice:    true,
	IngressPrice:    true,
	EgressPrice:     true,
	FreeSectorPrice: true,
}

// Usable returns true if all checks passed.
func (u Usability) Usable() bool {
	return u == GoodUsability
}

// FailedChecks returns a string representing all failed Usability checks.
func (u Usability) FailedChecks() string {
	var reasons []string
	if !u.Uptime {
		reasons = append(reasons, "Uptime")
	}
	if !u.MaxContractDuration {
		reasons = append(reasons, "MaxContractDuration")
	}
	if !u.MaxCollateral {
		reasons = append(reasons, "MaxCollateral")
	}
	if !u.ProtocolVersion {
		reasons = append(reasons, "ProtocolVersion")
	}
	if !u.PriceValidity {
		reasons = append(reasons, "PriceValidity")
	}
	if !u.AcceptingContracts {
		reasons = append(reasons, "AcceptingContracts")
	}
	if !u.ContractPrice {
		reasons = append(reasons, "ContractPrice")
	}
	if !u.Collateral {
		reasons = append(reasons, "Collateral")
	}
	if !u.StoragePrice {
		reasons = append(reasons, "StoragePrice")
	}
	if !u.IngressPrice {
		reasons = append(reasons, "IngressPrice")
	}
	if !u.EgressPrice {
		reasons = append(reasons, "EgressPrice")
	}
	if !u.FreeSectorPrice {
		reasons = append(reasons, "FreeSectorPrice")
	}
	return strings.Join(reasons, ",")
}
