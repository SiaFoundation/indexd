package sdk

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/hosts"
)

type cachedHostStore struct {
	tg     *threadgroup.ThreadGroup
	client *app.Client

	mu    sync.Mutex
	hosts map[types.PublicKey]hosts.HostInfo
}

// UsableHosts returns all cached hosts.
func (chs *cachedHostStore) UsableHosts() ([]hosts.HostInfo, error) {
	done, err := chs.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	chs.mu.Lock()
	defer chs.mu.Unlock()

	hosts := make([]hosts.HostInfo, 0, len(chs.hosts))
	for _, host := range chs.hosts {
		hosts = append(hosts, host)
	}
	return hosts, nil
}

// Addresses returns the network addresses for the specified host.
func (chs *cachedHostStore) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	done, err := chs.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	chs.mu.Lock()
	defer chs.mu.Unlock()

	host, exists := chs.hosts[hostKey]
	if !exists {
		return nil, errors.New("unknown host")
	}
	return host.Addresses, nil
}

// Usable returns whether the host is usable (i.e. cached).
func (chs *cachedHostStore) Usable(hostKey types.PublicKey) (bool, error) {
	done, err := chs.tg.Add()
	if err != nil {
		return false, err
	}
	defer done()

	chs.mu.Lock()
	defer chs.mu.Unlock()

	_, exists := chs.hosts[hostKey]
	return exists, nil
}

func (chs *cachedHostStore) updateHosts(ctx context.Context) error {
	hostInfos, err := chs.client.Hosts(ctx)
	if err != nil {
		return err
	}
	hosts := make(map[types.PublicKey]hosts.HostInfo)
	for _, host := range hostInfos {
		hosts[host.PublicKey] = host
	}

	chs.mu.Lock()
	chs.hosts = hosts
	chs.mu.Unlock()
	return nil
}

func newCachedHostStore(client *app.Client) (*cachedHostStore, error) {
	chs := &cachedHostStore{
		tg:     threadgroup.New(),
		client: client,
		hosts:  make(map[types.PublicKey]hosts.HostInfo),
	}
	if err := chs.updateHosts(context.Background()); err != nil {
		return nil, err
	}
	ctx, cancel, err := chs.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}

			chs.updateHosts(ctx)
		}
	}()
	return chs, nil
}
