package api

import (
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

type (
	// AccountRotateKeyRequest is the request body for the [PUT]
	// /account/:accountkey request.
	AccountRotateKeyRequest struct {
		NewAccountKey types.PublicKey `json:"newAccountKey"`
	}

	// HostsBlocklistRequest is the request body for the [POST] /hosts/blocklist.
	HostsBlocklistRequest struct {
		HostKeys []types.PublicKey `json:"hostKeys"`
		Reason   string            `json:"reason"`
	}

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
