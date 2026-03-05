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

	// ErrNotFound is returned by database operations that fail due to an
	// account not being found.
	ErrNotFound = errors.New("account not found")

	// ErrNoQuota is returned when trying to create a connect key without a quota.
	ErrNoQuota = errors.New("quota is required")

	// ErrAccountStorageLimitExceeded is returned when an operation fails due
	// to the account exceeding its storage limit.  We use the term "app
	// storage limit" here because from the user's perspective, they will have
	// one connect key with multiple apps attached, each of which is
	// actually represented by an account in the database.
	ErrAccountStorageLimitExceeded = errors.New("app storage limit exceeded")
)

type (
	// AddAccountOptions holds optional parameters for account creation.
	AddAccountOptions struct {
		MaxPinnedData uint64
	}

	// AddAccountOption is a functional option for configuring optional
	// parameters during account creation.
	AddAccountOption func(*AddAccountOptions)
)

// WithMaxPinnedData sets the maximum amount of data that can be pinned
func WithMaxPinnedData(maxPinnedData uint64) AddAccountOption {
	return func(opts *AddAccountOptions) {
		opts.MaxPinnedData = maxPinnedData
	}
}

type (
	// QueryAccountsOptions holds options for querying accounts.
	QueryAccountsOptions struct {
		ConnectKey *string
	}

	// QueryAccountsOpt is a functional option for querying accounts.
	QueryAccountsOpt func(o *QueryAccountsOptions)
)

// WithConnectKey filters the accounts by the connect key they are associated
// with.
func WithConnectKey(connectKey string) QueryAccountsOpt {
	return func(opt *QueryAccountsOptions) {
		opt.ConnectKey = &connectKey
	}
}

type (
	// Account represents an account in the indexer.
	Account struct {
		AccountKey    proto.Account `json:"accountKey"`
		ConnectKey    string        `json:"connectKey"`
		MaxPinnedData uint64        `json:"maxPinnedData"`
		PinnedData    uint64        `json:"pinnedData"`
		App           AppMeta       `json:"app"`
		LastUsed      time.Time     `json:"lastUsed"`
	}

	// HostAccount represents an ephemeral account on a host.
	HostAccount struct {
		AccountKey             proto.Account
		HostKey                types.PublicKey
		ConsecutiveFailedFunds int
		NextFund               time.Time
	}

	// UpdateAccountRequest is the request body for the
	// [PUT] /account/:accountkey/limit endpoint.
	UpdateAccountRequest struct {
		MaxPinnedData *uint64 `json:"maxPinnedData"`
	}

	// QuotaFundInfo contains funding info for a quota including the number
	// of active accounts.
	QuotaFundInfo struct {
		QuotaName       string
		FundTargetBytes uint64
		ActiveAccounts  uint64
	}
)
