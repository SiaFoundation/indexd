package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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
)

// A Builder helps connect an application to an indexer
// and initialize an SDK instance.
type Builder struct {
	ephemeralKey types.PrivateKey
	client       *app.Client

	request      app.RegisterAppRequest
	registerResp *app.RegisterAppResponse
	sharedSecret types.Hash256

	consumed *atomic.Bool
}

// consume marks the builder as consumed, preventing further use. It panics if the builder has already been consumed.
func (b *Builder) consume() {
	if !b.consumed.CompareAndSwap(false, true) {
		panic("Builder can only be used once")
	}
}

// checkConsumed panics if the builder has already been consumed.
func (b *Builder) checkConsumed() {
	if b.consumed.Load() {
		panic("Builder can only be used once")
	}
}

// WaitForApproval waits for the user to approve the app connection request.
// The user must visit the response URL returned by [Builder.Connect] to approve
// the request. It will block until the request is either approved or denied.
//
// Panics if the builder has already created an SDK instance.
func (b *Builder) WaitForApproval(ctx context.Context) (bool, error) {
	b.checkConsumed()

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
			if status, err := b.client.RequestStatus(ctx, b.ephemeralKey, b.registerResp.StatusURL); errors.Is(err, app.ErrUserRejected) {
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

// Register derives an application key from a BIP-39 seed phrase and
// registers it with the indexer.
//
// This key should be stored securely by the application and never
// shared with anyone else. It can be regenerated using the same app
// ID, user account, and seed phrase.
//
// Panics if the builder has already created an SDK instance.
func (b *Builder) Register(ctx context.Context, mnemonic string) (*SDK, error) {
	b.checkConsumed()

	if b.sharedSecret == (types.Hash256{}) {
		return nil, fmt.Errorf("app not connected")
	}

	appKey, err := deriveAppKey(mnemonic, b.request.AppID, b.sharedSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to derive app key: %w", err)
	} else if err := b.client.RegisterApp(ctx, b.registerResp.RegisterURL, b.ephemeralKey, appKey); err != nil {
		return nil, fmt.Errorf("failed to register app key: %w", err)
	}

	// prevent attempted re-use
	b.registerResp = nil
	clear(b.sharedSecret[:])
	return b.SDK(appKey)
}

// RequestConnection sends a request to connect an application to the indexer.
//
// It returns a response URL that the user must visit to approve the request.
// The app should display the response URL to the user.
//
// // Panics if the builder has already created an SDK instance.
func (b *Builder) RequestConnection(ctx context.Context) (string, error) {
	b.checkConsumed()
	resp, err := b.client.RequestAppConnection(ctx, b.ephemeralKey, b.request)
	if err != nil {
		return "", fmt.Errorf("failed to request app connection: %w", err)
	}
	b.registerResp = &resp
	return resp.ResponseURL, nil
}

// SDK creates a new SDK instance using the given application key. If the
// key is not authorized, an error is returned.
//
// Panics if the builder has already created an SDK instance.
func (b *Builder) SDK(appKey types.PrivateKey, opts ...Option) (*SDK, error) {
	b.checkConsumed()

	if ok, err := b.client.CheckAppAuth(context.Background(), appKey); err != nil {
		return nil, fmt.Errorf("failed to check app auth: %w", err)
	} else if !ok {
		return nil, fmt.Errorf("app key is not authorized")
	}
	hostStore, err := newCachedHostStore(b.client, appKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create host store: %w", err)
	}
	b.consume()
	return initSDK(appKey, b.client, client.NewProvider(hostStore), opts...), nil
}

func deriveAppKey(mnemonic string, appID types.Hash256, sharedSecret types.Hash256) (types.PrivateKey, error) {
	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, mnemonic); err != nil {
		return nil, fmt.Errorf("failed to derive seed from phrase: %w", err)
	}
	defer clear(seed[:])
	buf := keys.Derive(append(seed[:], sharedSecret[:]...), appID[:], []byte("indexd app key derivation"), 32)
	defer clear(buf)

	return types.NewPrivateKeyFromSeed(buf), nil
}

// NewBuilder creates a new Builder for connecting applications to the indexer.
//
// A builder can only be used to create a single SDK instance. Attempting to
// reuse a builder will result in a panic.
func NewBuilder(indexerURL string, metadata AppMetadata) *Builder {
	return &Builder{
		ephemeralKey: types.GeneratePrivateKey(),
		request: app.RegisterAppRequest{
			AppID:       metadata.ID,
			Name:        metadata.Name,
			Description: metadata.Description,
			LogoURL:     metadata.LogoURL,
			ServiceURL:  metadata.ServiceURL,
			CallbackURL: metadata.CallbackURL,
		},
		client:   app.NewClient(indexerURL),
		consumed: &atomic.Bool{},
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
