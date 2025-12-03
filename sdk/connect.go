package sdk

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/keys"
	"lukechampine.com/frand"
)

type (
	// AppMetadata contains metadata about an application.
	// This metadata is provided during app registration
	// and is used to identify the application to users.
	AppMetadata struct {
		// ID is a unique identifier for an application.
		// It should be generated once and stay constant for
		// the lifetime of the app.
		//
		// Changing it will invalidate any existing app keys
		// and prevent access to associated data.
		//
		// It should be a randomly generated 32-byte value.
		// You can use GenerateAppID to create a new app ID.
		ID          types.Hash256
		Name        string
		Description string
		LogoURL     string
		ServiceURL  string
		CallbackURL string
	}

	// An AppKey represents a key used to authenticate an application.
	// It is derived from a BIP-39 seed phrase and a shared secret
	// provided by the indexer during app connection.
	//
	// This key should be stored securely by the application and never
	// shared with anyone else. It can be regenerated using the same app
	// ID, user account, and seed phrase.
	AppKey struct {
		key types.PrivateKey
	}
)

// String returns the base64-encoded representation of the app key.
func (ak *AppKey) String() string {
	return base64.RawURLEncoding.EncodeToString(ak.key[:32])
}

// MarshalText encodes the app key as a base64-encoded string.
func (ak *AppKey) MarshalText() ([]byte, error) {
	return []byte(ak.String()), nil
}

// UnmarshalText decodes a base64-encoded app key.
func (ak *AppKey) UnmarshalText(text []byte) error {
	buf, err := base64.RawURLEncoding.DecodeString(string(text))
	if err != nil {
		return fmt.Errorf("failed to decode app key: %w", err)
	} else if len(buf) != ed25519.SeedSize {
		return fmt.Errorf("invalid app key size: expected %d, got %d", ed25519.SeedSize, len(buf))
	}
	ak.key = types.NewPrivateKeyFromSeed(buf)
	return nil
}

// A Builder helps connect an application to an indexer
// and initialize an SDK instance.
type Builder struct {
	client *app.Client

	request      app.RegisterAppRequest
	registerResp *app.RegisterAppResponse
	sharedSecret types.Hash256
}

// WaitForApproval waits for the user to approve the app connection request.
// The user must visit the response URL returned by [Builder.Connect] to approve
// the request. It will block until the request is either approved or denied.
func (b *Builder) WaitForApproval(ctx context.Context) (bool, error) {
	if b.registerResp == nil {
		return false, fmt.Errorf("no connection request to wait for approval")
	} else if time.Until(b.registerResp.Expiration) <= 0 {
		return false, fmt.Errorf("request expired")
	}

	ctx, cancel := context.WithDeadline(ctx, b.registerResp.Expiration)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Second):
			if status, err := b.client.RequestStatus(ctx, b.registerResp.StatusURL); errors.Is(err, app.ErrUserRejected) {
				return false, nil
			} else if err != nil {
				return false, fmt.Errorf("failed to check request status: %w", err)
			} else if status.Approved {
				b.sharedSecret = status.UserSecret
				return true, nil
			}
		}
	}
}

// GenerateAppKey derives an application key from a BIP-39 seed phrase that
// can be used to upload and download data from the indexer and
// registers it with the indexer.
//
// This key should be stored securely by the application and never
// shared with anyone else. It can be regenerated using the same app
// ID, user account, and seed phrase.
func (b *Builder) GenerateAppKey(ctx context.Context, mnemonic string) (*AppKey, error) {
	if b.sharedSecret == (types.Hash256{}) {
		return nil, fmt.Errorf("app not connected")
	}

	appKey, err := deriveAppKey(mnemonic, b.request.AppID, b.sharedSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to derive app key: %w", err)
	} else if err := b.client.RegisterApp(ctx, b.registerResp.RegisterURL, appKey.key); err != nil {
		return nil, fmt.Errorf("failed to register app key: %w", err)
	}

	// prevent attempted re-use
	b.registerResp = nil
	clear(b.sharedSecret[:])
	return appKey, nil
}

// RequestConnection sends a request to connect an application to the indexer.
//
// It returns a response URL that the user must visit to approve the request.
// The app should display the response URL to the user.
func (b *Builder) RequestConnection(ctx context.Context) (string, error) {
	resp, err := b.client.RequestAppConnection(ctx, b.request)
	if err != nil {
		return "", fmt.Errorf("failed to request app connection: %w", err)
	}
	b.registerResp = &resp
	return resp.ResponseURL, nil
}

// Connected checks if the application is connected to the indexer.
func (b *Builder) Connected(ctx context.Context, appKey *AppKey) (bool, error) {
	return b.client.CheckAppAuth(ctx, appKey.key)
}

// SDK creates a new SDK instance using the given application key.
func (b *Builder) SDK(appKey *AppKey, opts ...Option) (*SDK, error) {
	if ok, err := b.client.CheckAppAuth(context.Background(), appKey.key); err != nil {
		return nil, fmt.Errorf("failed to check app auth: %w", err)
	} else if !ok {
		return nil, fmt.Errorf("app key is not authorized")
	}
	hostStore, err := newCachedHostStore(b.client, appKey.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create host store: %w", err)
	}
	return initSDK(appKey.key, b.client, client.New(client.NewProvider(hostStore)), opts...), nil
}

func deriveAppKey(mnemonic string, appID types.Hash256, sharedSecret types.Hash256) (*AppKey, error) {
	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, mnemonic); err != nil {
		return nil, fmt.Errorf("failed to derive seed from phrase: %w", err)
	}
	defer clear(seed[:])
	buf := keys.Derive(append(seed[:], sharedSecret[:]...), appID[:], []byte("indexd app key derivation"), 32)
	defer clear(buf)

	return &AppKey{types.NewPrivateKeyFromSeed(buf)}, nil
}

// NewBuilder creates a new Builder for connecting applications to the indexer.
func NewBuilder(indexerURL string, metadata AppMetadata) *Builder {
	return &Builder{
		request: app.RegisterAppRequest{
			AppID:       metadata.ID,
			Name:        metadata.Name,
			Description: metadata.Description,
			LogoURL:     metadata.LogoURL,
			ServiceURL:  metadata.ServiceURL,
			CallbackURL: metadata.CallbackURL,
		},
		client: app.NewClient(indexerURL),
	}
}

// GenerateAppID generates a new random application ID.
func GenerateAppID() (id types.Hash256) {
	frand.Read(id[:])
	return id
}

// NewSeedPhrase generates a new seed phrase.
func NewSeedPhrase() string {
	return wallet.NewSeedPhrase()
}
