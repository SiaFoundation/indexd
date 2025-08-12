package app

import (
	"errors"
	"time"
)

var (
	// ErrKeyExhausted is returned when an app connect key has
	// no remaining uses.
	ErrKeyExhausted = errors.New("key has no remaining uses")
	// ErrKeyNotFound is returned when an app connect key is not found.
	ErrKeyNotFound = errors.New("key not found")
)

type (
	// A ConnectKey represents a key used to authenticate
	// when connecting a new application.
	ConnectKey struct {
		Key           string    `json:"key"`
		Description   string    `json:"description"`
		TotalUses     int       `json:"totalUses"`
		RemainingUses int       `json:"remainingUses"`
		DateCreated   time.Time `json:"dateCreated"`
		LastUpdated   time.Time `json:"lastUpdated"`
		LastUsed      time.Time `json:"lastUsed"`
		MaxPinnedData int64     `json:"maxPinnedData"`
	}

	// AddConnectKeyRequest is the request type for adding a new app connect key.
	AddConnectKeyRequest struct {
		Description   string `json:"description"`
		MaxPinnedData int64  `json:"maxPinnedData,omitempty"`
		RemainingUses int    `json:"remainingUses"`
	}

	// UpdateAppConnectKey represents a request to add or update
	// an app connect key.
	UpdateAppConnectKey struct {
		Key           string `json:"key"`
		Description   string `json:"description"`
		MaxPinnedData int64  `json:"maxPinnedData,omitempty"`
		RemainingUses int    `json:"remainingUses"`
	}
)
