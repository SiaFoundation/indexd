package hosts

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	// Host is a host on the network.
	Host struct {
		PublicKey        types.PublicKey    `json:"publicKey"`
		LastAnnouncement time.Time          `json:"lastAnnouncement"`
		Addresses        []chain.NetAddress `json:"addresses"`
	}
)
