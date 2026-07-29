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

	// ErrPreAuthorizedKeyExpired is returned when a pre-authorized key has
	// passed its expiration time.
	ErrPreAuthorizedKeyExpired = errors.New("pre-authorized key expired")

	// ErrPreAuthorizedKeyExhausted is returned when a pre-authorized key has no
	// remaining uses.
	ErrPreAuthorizedKeyExhausted = errors.New("pre-authorized key has no remaining uses")

	// ErrPreAuthorizedKeyAppMismatch is returned when a pre-authorized key is
	// restricted to a different application.
	ErrPreAuthorizedKeyAppMismatch = errors.New("pre-authorized key is not valid for this app")

	// ErrInvalidPreAuthorizedKey is returned when a pre-authorized key request
	// has invalid fields or signature.
	ErrInvalidPreAuthorizedKey = errors.New("invalid pre-authorized key")

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

	// PreAuthorizedKey is a limited-use authorization that allows an application
	// to skip the interactive approval step for a connect key.
	PreAuthorizedKey struct {
		PublicKey     types.PublicKey `json:"publicKey"`
		ConnectKey    string          `json:"connectKey"`
		Expiration    time.Time       `json:"expiration"`
		TotalUses     int             `json:"totalUses"`
		RemainingUses int             `json:"remainingUses"`
		AllowedAppID  *types.Hash256  `json:"allowedAppID,omitempty"`
		DateCreated   time.Time       `json:"dateCreated"`
		LastUsed      time.Time       `json:"lastUsed"`
	}

	// PreAuthorizedKeyRequest is the request type for creating a
	// pre-authorized key.
	PreAuthorizedKeyRequest struct {
		PublicKey types.PublicKey `json:"publicKey"`
		Signature types.Signature `json:"signature"`

		ConnectKey   string         `json:"connectKey"`
		Expiration   time.Time      `json:"expiration"`
		TotalUses    int            `json:"totalUses"`
		AllowedAppID *types.Hash256 `json:"allowedAppID,omitempty"`
	}

	// AppAuthorization contains the information needed to complete an
	// application connection.
	AppAuthorization struct {
		ConnectKey   string
		UserSecret   types.Hash256
		Reconnecting bool
	}

	// AppMeta contains additional metadata associated with an account.
	AppMeta struct {
		ID          types.Hash256 `json:"id"`
		Name        string        `json:"name"`
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

// SigHash returns the domain-separated hash signed when creating a
// pre-authorized key.
func (req PreAuthorizedKeyRequest) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteString("indexd/preauthorized-key/create/v1")
	req.PublicKey.EncodeTo(h.E)
	h.E.WriteString(req.ConnectKey)
	h.E.WriteTime(req.Expiration)
	h.E.WriteUint64(uint64(req.TotalUses))
	h.E.WriteBool(req.AllowedAppID != nil)
	if req.AllowedAppID != nil {
		h.E.Write(req.AllowedAppID[:])
	}
	return h.Sum()
}

// Sign proves control of privateKey and binds the complete pre-authorization
// policy to its corresponding public key.
func (req *PreAuthorizedKeyRequest) Sign(privateKey types.PrivateKey) {
	req.PublicKey = privateKey.PublicKey()
	req.Signature = privateKey.SignHash(req.SigHash())
}

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

// AddPreAuthorizedKey creates a limited-use authorization for bypassing the
// interactive application approval flow.
func (m *AccountManager) AddPreAuthorizedKey(ctx context.Context, req PreAuthorizedKeyRequest) (PreAuthorizedKey, error) {
	if req.PublicKey == (types.PublicKey{}) || req.ConnectKey == "" || req.TotalUses <= 0 || !req.Expiration.After(time.Now()) || !req.PublicKey.VerifyHash(req.SigHash(), req.Signature) {
		return PreAuthorizedKey{}, ErrInvalidPreAuthorizedKey
	}
	return m.store.AddPreAuthorizedKey(req)
}

// DeletePreAuthorizedKey deletes a pre-authorized key.
func (m *AccountManager) DeletePreAuthorizedKey(ctx context.Context, publicKey types.PublicKey) error {
	return m.store.DeletePreAuthorizedKey(publicKey)
}

// PreAuthorizedKey returns the pre-authorized key with the given public key.
func (m *AccountManager) PreAuthorizedKey(ctx context.Context, publicKey types.PublicKey) (PreAuthorizedKey, error) {
	return m.store.PreAuthorizedKey(publicKey)
}

// PreAuthorizedKeys returns a paginated list of pre-authorized keys.
func (m *AccountManager) PreAuthorizedKeys(ctx context.Context, offset, limit int) ([]PreAuthorizedKey, error) {
	return m.store.PreAuthorizedKeys(offset, limit)
}

// AuthorizePreAuthorizedKey consumes one use of a pre-authorized key and
// returns the information needed to complete an application connection.
func (m *AccountManager) AuthorizePreAuthorizedKey(publicKey types.PublicKey, appID types.Hash256) (AppAuthorization, error) {
	connectKey, userSecret, reconnecting, err := m.store.ConsumePreAuthorizedKey(publicKey, appID)
	if err != nil {
		return AppAuthorization{}, err
	}
	return authorizeApp(connectKey, &userSecret, appID, reconnecting), nil
}

// AuthorizeAppConnectKey authorizes an application connection using a regular
// connect key. It returns the app-specific secret and whether the application
// was previously connected.
func (m *AccountManager) AuthorizeAppConnectKey(connectKey string, appID types.Hash256) (AppAuthorization, error) {
	userSecret, reconnecting, err := m.store.AppAuthorization(connectKey, appID)
	if err != nil {
		return AppAuthorization{}, err
	}
	return authorizeApp(connectKey, &userSecret, appID, reconnecting), nil
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

func authorizeApp(connectKey string, userSecret *types.Hash256, appID types.Hash256, reconnecting bool) AppAuthorization {
	defer clear(userSecret[:])
	return AppAuthorization{
		ConnectKey:   connectKey,
		UserSecret:   types.Hash256(keys.Derive(userSecret[:], appID[:], []byte("server app secret"), 32)),
		Reconnecting: reconnecting,
	}
}

// DeriveSharingAccountKey deterministically derives a connect key's sharing
// account key pair from its user secret.
func DeriveSharingAccountKey(userSecret types.Hash256) types.PrivateKey {
	seed := keys.Derive(userSecret[:], []byte("sharing"), nil, 32)
	defer clear(seed)
	return types.NewPrivateKeyFromSeed(seed)
}
