package hosts

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

// A HostStore implements the [client.Store] interface using a Store.
type HostStore struct {
	store Store
}

// UsableHosts implements the [HostStore] interface.
func (hs *HostStore) UsableHosts() ([]HostInfo, error) {
	return hs.store.UsableHosts(0, 1000)
}

// Addresses implements the [HostStore] interface.
func (hs *HostStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	host, err := hs.store.Host(hostKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get host: %w", err)
	}
	return host.Addresses, nil
}

// Usable implements the [HostStore] interface.
func (hs *HostStore) Usable(hostKey types.PublicKey) (bool, error) {
	host, err := hs.store.Host(hostKey)
	if err != nil {
		return false, fmt.Errorf("failed to get host: %w", err)
	}
	return host.Usability.Usable() && !host.Blocked, nil
}

// NewHostStore wraps a [Store] to provide a client HostStore.
func NewHostStore(store Store) *HostStore {
	return &HostStore{
		store: store,
	}
}
