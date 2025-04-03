package accounts

import (
	"errors"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

var (
	// ErrExists is returned by database operations that fail due to an account
	// already existing.
	ErrExists = errors.New("account already exists")
)

type (
	// HostAccount represents an ephemeral account on a host.
	HostAccount struct {
		AccountKey             proto.Account
		HostKey                types.PublicKey
		ConsecutiveFailedFunds int
		NextFund               time.Time
	}
)
