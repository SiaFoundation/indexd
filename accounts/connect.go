package accounts

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

var (
	// ErrKeyExhausted is returned when an app connect key has
	// no remaining uses.
	ErrKeyExhausted = errors.New("key has no remaining uses")
	// ErrKeyNotFound is returned when an app connect key is not found.
	ErrKeyNotFound = errors.New("key not found")
	// ErrKeyInUse is returned when deleting an app connect key with accounts
	// associated to it.
	ErrKeyInUse = errors.New("key in use")
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

	// AccountMeta contains additional metadata associated with an account.
	AccountMeta struct {
		Description string
		LogoURL     string
		ServiceURL  string
	}
)

// AddAppConnectKey adds a new app connect key.
func (m *AccountManager) AddAppConnectKey(ctx context.Context, key UpdateAppConnectKey) (ConnectKey, error) {
	return m.store.AddAppConnectKey(key)
}

// UpdateAppConnectKey updates an existing app connect key.
// If the key does not exist, it returns [ErrKeyNotFound].
func (m *AccountManager) UpdateAppConnectKey(ctx context.Context, key UpdateAppConnectKey) (ConnectKey, error) {
	return m.store.UpdateAppConnectKey(key)
}

// DeleteAppConnectKey deletes an existing app connect key.
// If the key does not exist, it returns [ErrKeyNotFound].
func (m *AccountManager) DeleteAppConnectKey(ctx context.Context, key string) error {
	return m.store.DeleteAppConnectKey(key)
}

// AppConnectKey returns the given app connect keys.
func (m *AccountManager) AppConnectKey(ctx context.Context, key string) (ConnectKey, error) {
	return m.store.AppConnectKey(key)
}

// AppConnectKeys returns a list of app connect keys.
func (m *AccountManager) AppConnectKeys(ctx context.Context, offset, limit int) ([]ConnectKey, error) {
	return m.store.AppConnectKeys(offset, limit)
}

// UseAppConnectKey uses an existing app connect key to add an account. If the key is exhausted, it
// returns [ErrKeyExhausted]. If the key is not found, it returns [ErrKeyNotFound].
func (m *AccountManager) UseAppConnectKey(ctx context.Context, key string, pk types.PublicKey, meta AccountMeta) error {
	if err := m.store.UseAppConnectKey(key, pk, meta); err != nil {
		return fmt.Errorf("failed to use app connect key: %w", err)
	}
	return nil
}

// ValidAppConnectKey checks if an app connect key is valid. If the key is not found, it
// returns [ErrKeyNotFound].
func (m *AccountManager) ValidAppConnectKey(ctx context.Context, key string) (bool, error) {
	return m.store.ValidAppConnectKey(key)
}
