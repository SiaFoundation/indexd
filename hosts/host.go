package hosts

import (
	"errors"
	"net"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

const (
	blocksPerMonth = 144 * 30
	oneTB          = 1e12
)

var (
	// ErrNotFound is returned by database operations that fail due to a host
	// not being found.
	ErrNotFound = errors.New("host not found")

	// ErrNoNetworks is returned when a host has no networks even though it
	// should.
	ErrNoNetworks = errors.New("host has no networks")
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
		MinProtocolVersion: [3]uint8{1, 0, 0},
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
		Networks               []net.IPNet         `json:"networks"`
		Settings               proto4.HostSettings `json:"settings"`
		Usability              Usability           `json:"usability"`
		Blocked                bool                `json:"blocked"`
		BlockedReason          string              `json:"blockedReason"`
		LostSectors            uint64              `json:"lostSectors"`
	}

	// HostInfo is a subset of the Host struct that contains only the public
	// key and addresses. It is used for listing usable hosts in the
	// application API.
	HostInfo struct {
		PublicKey types.PublicKey    `json:"publicKey"`
		Addresses []chain.NetAddress `json:"addresses"`
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
		MaxEgressPrice     types.Currency `json:"maxEgressPrice"`
		MaxIngressPrice    types.Currency `json:"maxIngressPrice"`
		MaxStoragePrice    types.Currency `json:"maxStoragePrice"`
		MinCollateral      types.Currency `json:"minCollateral"`
		MinProtocolVersion [3]uint8       `json:"minProtocolVersion"`
	}
)

// IsGood returns true if the host is considered good for storing data.
func (h *Host) IsGood() bool {
	return h.Usability.Usable() && !h.Blocked && len(h.Networks) > 0
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
