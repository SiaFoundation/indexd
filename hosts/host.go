package hosts

import (
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	// Host is a host on the network.
	Host struct {
		PublicKey        types.PublicKey     `json:"publicKey"`
		LastAnnouncement time.Time           `json:"lastAnnouncement"`
		Addresses        []chain.NetAddress  `json:"addresses"`
		Settings         proto4.HostSettings `json:"settings"`
	}
)
