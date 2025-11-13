package hosts

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

// A ProviderStore implements the HostStore interface using a Store.
type ProviderStore struct {
	store Store
}

// UsableHosts implements the [HostStore] interface.
func (ps *ProviderStore) UsableHosts() ([]HostInfo, error) {
	return ps.store.UsableHosts(0, 1000)
}

// Addresses implements the [HostStore] interface.
func (ps *ProviderStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	host, err := ps.store.Host(hostKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get host: %w", err)
	}
	return host.Addresses, nil
}

// Usable implements the [HostStore] interface.
func (ps *ProviderStore) Usable(hostKey types.PublicKey) (bool, error) {
	host, err := ps.store.Host(hostKey)
	if err != nil {
		return false, fmt.Errorf("failed to get host: %w", err)
	}
	return host.Usability.Usable() && !host.Blocked, nil
}

// NewProviderStore wraps a [Store] to provide a client ProviderStore.
func NewProviderStore(store Store) *ProviderStore {
	return &ProviderStore{
		store: store,
	}
}
