package sdk

import (
	"context"
	"encoding/hex"
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

type mockHostDialer struct {
	hosts map[types.PublicKey]struct{}

	delayMu   sync.Mutex
	slowHosts map[types.PublicKey]time.Duration

	sectorsMu   sync.Mutex
	hostSectors map[types.PublicKey]map[types.Hash256][proto4.SectorSize]byte
}

// Hosts implements the [HostDialer] interface.
func (m *mockHostDialer) Hosts() []types.PublicKey {
	return slices.Collect(maps.Keys(m.hosts))
}

// ActiveHosts implements the [HostDialer] interface.
func (m *mockHostDialer) ActiveHosts() []types.PublicKey {
	m.sectorsMu.Lock()
	defer m.sectorsMu.Unlock()
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
	defer m.sectorsMu.Unlock()

	root := proto4.SectorRoot(sector)
	if _, ok := m.hostSectors[hostKey]; !ok {
		m.hostSectors[hostKey] = make(map[types.Hash256][proto4.SectorSize]byte)
	}
	m.hostSectors[hostKey][root] = *sector
	return root, nil
}

// ReadSector implements the [HostDialer] interface.
func (m *mockHostDialer) ReadSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) (*[proto4.SectorSize]byte, error) {
	// simulate timeout
	if err := m.delay(ctx, hostKey); err != nil {
		return nil, err
	}

	m.sectorsMu.Lock()
	defer m.sectorsMu.Unlock()

	var sector [proto4.SectorSize]byte
	sectors, ok := m.hostSectors[hostKey]
	if !ok {
		return nil, errors.New("host not found")
	}
	sector, ok = sectors[sectorRoot]
	if !ok {
		return nil, errors.New("sector not found")
	}
	return &sector, nil
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
		hostSectors: make(map[types.PublicKey]map[types.Hash256][proto4.SectorSize]byte),
	}
	for range hosts {
		sk := types.GeneratePrivateKey()
		m.hosts[sk.PublicKey()] = struct{}{}
	}
	return m
}

type mockAppClient struct {
	mu      sync.Mutex
	pinned  map[slabs.SlabID]slabs.PinnedSlab
	objects map[types.Hash256]slabs.SealedObject
}

// PinSlab implements the [AppClient] interface.
func (mc *mockAppClient) PinSlabs(_ context.Context, toPin ...slabs.SlabPinParams) (digests []slabs.SlabID, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for _, s := range toPin {
		id, err := s.Digest()
		if err != nil {
			return nil, err
		}
		digests = append(digests, id)

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
	}
	return
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

// Hosts implements the [AppClient] interface.
func (mc *mockAppClient) Hosts(context.Context, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error) {
	return nil, nil
}

func (mc *mockAppClient) Object(ctx context.Context, objectKey types.Hash256) (slabs.SealedObject, error) {
	return mc.objects[objectKey], nil
}

func (mc *mockAppClient) ListObjects(ctx context.Context, cursor slabs.Cursor, limit int) ([]slabs.ObjectEvent, error) {
	var objs []slabs.ObjectEvent
	for _, obj := range mc.objects {
		objs = append(objs, slabs.ObjectEvent{
			Key:       obj.ID(),
			Deleted:   false,
			UpdatedAt: obj.UpdatedAt,
			Object:    &obj,
		})
	}
	return objs, nil
}

// SharedObject implements the [AppClient] interface.
func (mc *mockAppClient) SharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, []byte, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	buf, err := hex.DecodeString(sharedURL)
	if err != nil {
		return slabs.SharedObject{}, nil, errors.New("invalid shared URL")
	} else if len(buf) != 64 {
		return slabs.SharedObject{}, nil, errors.New("invalid shared URL")
	}

	objKey := (types.Hash256)(buf[:32])
	encryptionKey := buf[32:]

	obj, ok := mc.objects[objKey]
	if !ok {
		return slabs.SharedObject{}, nil, errors.New("object not found")
	}

	var objSlabs []slabs.SharedSlab
	for _, slab := range obj.Slabs {
		objSlabs = append(objSlabs, slabs.SharedSlab{
			PinnedSlab: mc.pinned[slab.SlabID],
			Offset:     slab.Offset,
			Length:     slab.Length,
		})
	}

	return slabs.SharedObject{Slabs: objSlabs}, encryptionKey, nil
}

// SaveObject implements the [AppClient] interface.
func (mc *mockAppClient) SaveObject(ctx context.Context, obj slabs.SealedObject) (err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.objects[obj.ID()] = obj
	return nil
}

// CreateSharedObjectURL implements the [AppClient] interface.
func (mc *mockAppClient) CreateSharedObjectURL(ctx context.Context, objectKey types.Hash256, encryptionKey []byte, validUntil time.Time) (string, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	_, ok := mc.objects[objectKey]
	if !ok {
		return "", errors.New("object not found")
	}

	key := make([]byte, 64)
	copy(key[:32], objectKey[:])
	copy(key[32:], encryptionKey)
	return hex.EncodeToString(key), nil
}

func newMockAppClient() *mockAppClient {
	return &mockAppClient{
		objects: make(map[types.Hash256]slabs.SealedObject),
		pinned:  make(map[slabs.SlabID]slabs.PinnedSlab),
	}
}
