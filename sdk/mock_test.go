package sdk

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
)

type mockHost struct {
	mu      sync.Mutex
	sectors map[types.Hash256][proto4.SectorSize]byte
}

func (mh *mockHost) WriteSector(ctx context.Context, sector *[proto4.SectorSize]byte) (types.Hash256, error) {
	root := proto4.SectorRoot(sector)

	mh.mu.Lock()
	defer mh.mu.Unlock()
	if mh.sectors == nil {
		mh.sectors = make(map[types.Hash256][proto4.SectorSize]byte)
	}
	mh.sectors[root] = *sector
	return root, nil
}

func (mh *mockHost) ReadSector(ctx context.Context, root types.Hash256) (*[proto4.SectorSize]byte, error) {
	mh.mu.Lock()
	defer mh.mu.Unlock()
	sector, ok := mh.sectors[root]
	if !ok {
		return nil, errors.New("sector not found")
	}
	return &sector, nil
}

type mockHostDialer struct {
	hosts map[types.PublicKey]struct{}

	delayMu   sync.Mutex
	slowHosts map[types.PublicKey]time.Duration

	sectorsMu   sync.Mutex
	hostSectors map[types.PublicKey]*mockHost
}

// Hosts implements the [HostDialer] interface.
func (m *mockHostDialer) Hosts() []types.PublicKey {
	return slices.Collect(maps.Keys(m.hosts))
}

// ActiveHosts implements the [HostDialer] interface.
func (m *mockHostDialer) ActiveHosts() []types.PublicKey {
	return slices.Collect(maps.Keys(m.hostSectors))
}

func (m *mockHostDialer) delay(ctx context.Context, hostKey types.PublicKey) error {
	m.delayMu.Lock()
	delay, ok := m.slowHosts[hostKey]
	m.delayMu.Unlock()
	if !ok || delay <= 0 {
		return nil
	}

	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
	return ctx.Err()
}

// WriteSector implements the [HostDialer] interface.
func (m *mockHostDialer) WriteSector(ctx context.Context, hostKey types.PublicKey, sector *[proto4.SectorSize]byte) (types.Hash256, error) {
	if _, ok := m.hosts[hostKey]; !ok {
		panic("host not found: " + hostKey.String()) // developer error
	}

	// simulate i/o
	if err := m.delay(ctx, hostKey); err != nil {
		return types.Hash256{}, err
	}

	m.sectorsMu.Lock()
	mh, ok := m.hostSectors[hostKey]
	if !ok {
		mh = &mockHost{}
		m.hostSectors[hostKey] = mh
	}
	m.sectorsMu.Unlock()
	return m.hostSectors[hostKey].WriteSector(ctx, sector)
}

// ReadSector implements the [HostDialer] interface.
func (m *mockHostDialer) ReadSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) (*[proto4.SectorSize]byte, error) {
	// simulate timeout
	if err := m.delay(ctx, hostKey); err != nil {
		return nil, err
	}

	m.sectorsMu.Lock()
	mh, ok := m.hostSectors[hostKey]
	if !ok {
		return nil, errors.New("host not found")
	}
	m.sectorsMu.Unlock()
	return mh.ReadSector(ctx, sectorRoot)
}

func (m *mockHostDialer) ResetSlowHosts() {
	m.delayMu.Lock()
	defer m.delayMu.Unlock()
	m.slowHosts = make(map[types.PublicKey]time.Duration)
}

func (m *mockHostDialer) SetSlowHosts(n int, d time.Duration) {
	m.delayMu.Lock()
	defer m.delayMu.Unlock()

	var set int
	for hostKey := range maps.Keys(m.hosts) {
		if set >= n {
			break // already set enough hosts
		}
		set++
		m.slowHosts[hostKey] = d
	}
}

func newMockDialer(hosts int) *mockHostDialer {
	m := &mockHostDialer{
		hosts:       make(map[types.PublicKey]struct{}),
		slowHosts:   make(map[types.PublicKey]time.Duration),
		hostSectors: make(map[types.PublicKey]*mockHost),
	}
	for range hosts {
		sk := types.GeneratePrivateKey()
		m.hosts[sk.PublicKey()] = struct{}{}
	}
	return m
}

type mockAppClient struct {
	mu     sync.Mutex
	pinned map[slabs.SlabID]slabs.PinnedSlab
}

// PinSlab implements the [AppClient] interface.
func (mc *mockAppClient) PinSlab(_ context.Context, s slabs.SlabPinParams) (slabs.SlabID, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	id, err := s.Digest()
	if err != nil {
		return slabs.SlabID{}, err
	}
	ps := slabs.PinnedSlab{
		ID:            id,
		EncryptionKey: s.EncryptionKey,
		MinShards:     s.MinShards,
		Sectors:       make([]slabs.PinnedSector, len(s.Sectors)),
	}
	for i, sector := range s.Sectors {
		ps.Sectors[i] = slabs.PinnedSector{
			Root:    sector.Root,
			HostKey: sector.HostKey,
		}
	}
	mc.pinned[id] = ps
	return id, nil
}

// UnpinSlab implements the [AppClient] interface.
func (mc *mockAppClient) UnpinSlab(_ context.Context, id slabs.SlabID) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.pinned, id)
	return nil
}

// Slab implements the [AppClient] interface.
func (mc *mockAppClient) Slab(_ context.Context, id slabs.SlabID) (slabs.PinnedSlab, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	slab, ok := mc.pinned[id]
	if !ok {
		return slabs.PinnedSlab{}, errors.New("slab not found")
	}
	return slab, nil
}

func (mc *mockAppClient) Hosts(context.Context, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error) {
	return nil, nil
}

func newMockAppClient() *mockAppClient {
	return &mockAppClient{
		pinned: make(map[slabs.SlabID]slabs.PinnedSlab),
	}
}
