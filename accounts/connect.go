package accounts

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/keys"
	"lukechampine.com/frand"
)

var (
	// ErrKeyAlreadyExists is returned when trying to add an app connect key
	// with an app key that already exists.
	ErrKeyAlreadyExists = errors.New("key already exists")

	// ErrKeyExhausted is returned when an app connect key has
	// no remaining uses.
	ErrKeyExhausted = errors.New("key has no remaining uses")

	// ErrKeyNotFound is returned when an app connect key is not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyInUse is returned when deleting an app connect key with accounts
	// associated to it.
	ErrKeyInUse = errors.New("key in use")

	// ErrQuotaNotFound is returned when a quota is not found.
	ErrQuotaNotFound = errors.New("quota not found")

	// ErrQuotaInUse is returned when deleting a quota with connect keys
	// associated to it.
	ErrQuotaInUse = errors.New("quota in use")

	// ErrAppKeyStorageLimitExceeded is returned when an operation fails due to
	// the connect key exceeding its storage limit.    We use the term "account
	// storage limit" here because from the user's perspective, the app connect
	// key is their "account" which has all of their apps under it.
	ErrAppKeyStorageLimitExceeded = errors.New("account storage limit exceeded")
)

type (
	// Quota represents a usage quota for connect keys.
	Quota struct {
		Key             string `json:"key"`
		Description     string `json:"description"`
		MaxPinnedData   uint64 `json:"maxPinnedData"`
		TotalUses       int    `json:"totalUses"`
		FundTargetBytes uint64 `json:"fundTargetBytes"`
	}

	// A ConnectKey represents a key used to authenticate
	// when connecting a new application.
	ConnectKey struct {
		Key           string    `json:"key"`
		Description   string    `json:"description"`
		Quota         string    `json:"quota"`
		RemainingUses int       `json:"remainingUses"`
		DateCreated   time.Time `json:"dateCreated"`
		LastUpdated   time.Time `json:"lastUpdated"`
		LastUsed      time.Time `json:"lastUsed"`
		PinnedData    uint64    `json:"pinnedData"`
		PinnedSize    uint64    `json:"pinnedSize"`
	}

	// AppConnectKeyRequest represents a request to add or update
	// an app connect key.
	AppConnectKeyRequest struct {
		Key         string `json:"key"`
		Description string `json:"description"`
		Quota       string `json:"quota"`
	}

	// AppMeta contains additional metadata associated with an account.
	AppMeta struct {
		ID          types.Hash256 `json:"id"`
		Description string        `json:"description"`
		LogoURL     string        `json:"logoURL"`
		ServiceURL  string        `json:"serviceURL"`
	}

	// PutQuotaRequest is the request type for creating or updating a quota.
	PutQuotaRequest struct {
		Description     string  `json:"description"`
		MaxPinnedData   uint64  `json:"maxPinnedData"`
		TotalUses       int     `json:"totalUses"`
		FundTargetBytes *uint64 `json:"fundTargetBytes"`
	}
)

// AddAppConnectKey adds a new app connect key. If the key field in the
// request is empty, a random key will be generated.
func (m *AccountManager) AddAppConnectKey(ctx context.Context, key AppConnectKeyRequest) (ConnectKey, error) {
	if key.Quota == "" {
		return ConnectKey{}, ErrNoQuota
	}
	if key.Key == "" {
		key.Key = hex.EncodeToString(frand.Bytes(32))
	}
	return m.store.AddAppConnectKey(key)
}

// UpdateAppConnectKey updates an existing app connect key.
// If the key does not exist, it returns [ErrKeyNotFound].
func (m *AccountManager) UpdateAppConnectKey(ctx context.Context, key AppConnectKeyRequest) (ConnectKey, error) {
	if key.Quota == "" {
		return ConnectKey{}, ErrNoQuota
	}
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

// RegisterAppKey uses an existing app connect key to add an account. If the key is exhausted, it
// returns [ErrKeyExhausted]. If the key is not found, it returns [ErrKeyNotFound].
func (m *AccountManager) RegisterAppKey(key string, pk types.PublicKey, meta AppMeta) error {
	if err := m.store.RegisterAppKey(key, pk, meta); err != nil {
		return fmt.Errorf("failed to register app connect key: %w", err)
	}
	return nil
}

// ValidAppConnectKey checks if an app connect key exists. If the key is not found, it
// returns [ErrKeyNotFound].
func (m *AccountManager) ValidAppConnectKey(ctx context.Context, key string) error {
	return m.store.ValidAppConnectKey(key)
}

// PutQuota creates or updates a quota.
func (m *AccountManager) PutQuota(ctx context.Context, key string, req PutQuotaRequest) error {
	return m.store.PutQuota(key, req)
}

// DeleteQuota deletes an existing quota. If the quota does not exist, it returns [ErrQuotaNotFound].
// If the quota is in use by connect keys, it returns [ErrQuotaInUse].
func (m *AccountManager) DeleteQuota(ctx context.Context, key string) error {
	return m.store.DeleteQuota(key)
}

// Quota returns the quota with the given key. If the quota does not exist, it returns [ErrQuotaNotFound].
func (m *AccountManager) Quota(ctx context.Context, key string) (Quota, error) {
	return m.store.Quota(key)
}

// Quotas returns a list of quotas.
func (m *AccountManager) Quotas(ctx context.Context, offset, limit int) ([]Quota, error) {
	return m.store.Quotas(offset, limit)
}

// AppSecret derives a unique application secret using a stored user secret
// associated with the given connect key and the provided app ID.
func (m *AccountManager) AppSecret(connectKey string, appID types.Hash256) (types.Hash256, error) {
	secret, err := m.store.AppConnectKeyUserSecret(connectKey)
	if err != nil {
		return types.Hash256{}, err
	}
	return types.Hash256(keys.Derive(secret[:], appID[:], []byte("server app secret"), 32)), nil
}
