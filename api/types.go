package api

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
)

type (
	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		wallet.Balance

		Address types.Address `json:"address"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send
	// endpoint.
	WalletSendSiacoinsRequest struct {
		Address          types.Address  `json:"address"`
		Amount           types.Currency `json:"amount"`
		SubtractMinerFee bool           `json:"subtractMinerFee"`
		UseUnconfirmed   bool           `json:"useUnconfirmed"`
	}
)

type (
	// Host is a host on the network.
	Host struct {
		PublicKey        types.PublicKey    `json:"publicKey"`
		LastAnnouncement time.Time          `json:"lastAnnouncement"`
		Addresses        []chain.NetAddress `json:"addresses"`
	}
)
