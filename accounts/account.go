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

	// ErrServiceAccount is returned by operations that fail due to an account
	// being a service account.
	ErrServiceAccount = errors.New("account is a service account")

	// ErrStorageLimitExceeded is returned when an operation fails due to the
	// account exceeding its storage limit.
	ErrStorageLimitExceeded = errors.New("storage limit exceeded")
)

type (
	// AddAccountOptions holds optional parameters for account creation.
	AddAccountOptions struct {
		Description   string
		LogoURL       string
		MaxPinnedData uint64
		ServiceURL    string
	}

	// AddAccountOption is a functional option for configuring optional
	// parameters during account creation.
	AddAccountOption func(*AddAccountOptions)
)

// WithDescription sets the description for the account
func WithDescription(description string) AddAccountOption {
	return func(opts *AddAccountOptions) {
		opts.Description = description
	}
}

// WithLogoURL sets the logo URL for the account
func WithLogoURL(logoURL string) AddAccountOption {
	return func(opts *AddAccountOptions) {
		opts.LogoURL = logoURL
	}
}

// WithMaxPinnedData sets the maximum amount of data that can be pinned
func WithMaxPinnedData(maxPinnedData uint64) AddAccountOption {
	return func(opts *AddAccountOptions) {
		opts.MaxPinnedData = maxPinnedData
	}
}

// WithServiceURL sets the service URL for the account
func WithServiceURL(serviceURL string) AddAccountOption {
	return func(opts *AddAccountOptions) {
		opts.ServiceURL = serviceURL
	}
}

type (
	// QueryAccountsOptions holds options for querying accounts.
	QueryAccountsOptions struct {
		ServiceAccount *bool
	}

	// QueryAccountsOpt is a functional option for querying accounts.
	QueryAccountsOpt func(o *QueryAccountsOptions)
)

// WithServiceAccount sets the service account filter for querying accounts.
// Defaults to all accounts.
func WithServiceAccount(serviceAccount bool) QueryAccountsOpt {
	return func(opt *QueryAccountsOptions) {
		opt.ServiceAccount = &serviceAccount
	}
}

type (
	// Account represents an account in the indexer.
	Account struct {
		AccountKey     proto.Account `json:"accountKey"`
		ServiceAccount bool          `json:"serviceAccount"`
		MaxPinnedData  uint64        `json:"maxPinnedData"`
		PinnedData     uint64        `json:"pinnedData"`
		Description    string        `json:"description,omitempty"`
		LogoURL        string        `json:"logoURL,omitempty"`
		ServiceURL     string        `json:"serviceURL,omitempty"`
	}

	// HostAccount represents an ephemeral account on a host.
	HostAccount struct {
		AccountKey             proto.Account
		HostKey                types.PublicKey
		ConsecutiveFailedFunds int
		NextFund               time.Time
	}
)
