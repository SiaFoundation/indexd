package indexd_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd"
	"lukechampine.com/frand"
)

type mockHostDialer struct {
	mu          sync.Mutex
	hosts       map[types.PublicKey]struct{}
	slowHosts   map[types.PublicKey]time.Duration
	hostSectors map[types.PublicKey]map[types.Hash256][proto4.SectorSize]byte
}

// Hosts implements the [indexd.HostDialer] interface.
func (m *mockHostDialer) Hosts() []types.PublicKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Collect(maps.Keys(m.hosts))
}

// WriteSector implements the [indexd.HostDialer] interface.
func (m *mockHostDialer) WriteSector(ctx context.Context, hostKey types.PublicKey, sector *[proto4.SectorSize]byte) (types.Hash256, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.hosts[hostKey]; !ok {
		return types.Hash256{}, errors.New("host not found")
	}
	// simulate timeout
	select {
	case <-ctx.Done():
		return types.Hash256{}, ctx.Err()
	case <-time.After(m.slowHosts[hostKey]):
	}

	root := proto4.SectorRoot(sector)
	if _, ok := m.hostSectors[hostKey]; !ok {
		m.hostSectors[hostKey] = make(map[types.Hash256][proto4.SectorSize]byte)
	}
	m.hostSectors[hostKey][root] = *sector
	return root, nil
}

// ReadSector implements the [indexd.HostDialer] interface.
func (m *mockHostDialer) ReadSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) (*[proto4.SectorSize]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// simulate timeout
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(m.slowHosts[hostKey]):
	}

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

func (m *mockHostDialer) SetSlowHosts(n int, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if n > len(m.hosts) {
		n = len(m.hosts)
	}
	m.slowHosts = make(map[types.PublicKey]time.Duration)
	hosts := slices.Collect(maps.Keys(m.hosts))
	for _, key := range hosts[:n] {
		m.slowHosts[key] = d
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

func TestRoundtrip(t *testing.T) {
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()

	sdk := indexd.NewSDK("", appKey, dialer)

	data := frand.Bytes(4096)
	slabs, err := sdk.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(slabs))
	} else if slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), slabs[0].Length)
	}

	buf := bytes.NewBuffer(nil)
	if err := sdk.Download(context.Background(), buf, slabs); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}
}

func TestUploadTimeout(t *testing.T) {
	dialer := newMockDialer(50)
	// make most of the hosts timeout
	dialer.SetSlowHosts(30, 150*time.Millisecond)

	appKey := types.GeneratePrivateKey()

	sdk := indexd.NewSDK("", appKey, dialer)

	data := frand.Bytes(4096)
	_, err := sdk.Upload(context.Background(), bytes.NewReader(data), indexd.WithUploadHostTimeout(100*time.Millisecond))
	if !errors.Is(err, indexd.ErrNotEnoughShards) {
		t.Fatalf("expected ErrNotEnoughShards, got %v", err)
	}
}

func TestDownloadTimeout(t *testing.T) {
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()

	sdk := indexd.NewSDK("", appKey, dialer)

	data := frand.Bytes(4096)
	slabs, err := sdk.Upload(context.Background(), bytes.NewReader(data), indexd.WithUploadHostTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	// make most of the hosts timeout
	dialer.SetSlowHosts(30, 150*time.Millisecond)
	buf := bytes.NewBuffer(nil)
	err = sdk.Download(context.Background(), buf, slabs, indexd.WithDownloadHostTimeout(100*time.Millisecond))
	if !errors.Is(err, indexd.ErrNotEnoughShards) {
		t.Fatalf("expected ErrNotEnoughShards, got %v", err)
	}

}

func BenchmarkUpload(b *testing.B) {
	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()

	sdk := indexd.NewSDK("", appKey, dialer)

	data := frand.Bytes(benchmarkSize)

	b.SetBytes(benchmarkSize)
	b.ResetTimer()
	for b.Loop() {
		if _, err := sdk.Upload(context.Background(), bytes.NewReader(data)); err != nil {
			b.Fatalf("failed to upload: %v", err)
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()
	sdk := indexd.NewSDK("", appKey, dialer)

	data := frand.Bytes(benchmarkSize)

	slabs, err := sdk.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		b.Fatalf("failed to upload: %v", err)
	}

	buf := bytes.NewBuffer(make([]byte, 0, benchmarkSize))
	b.SetBytes(benchmarkSize)
	b.ResetTimer()
	for b.Loop() {
		buf.Reset()
		if err := sdk.Download(context.Background(), buf, slabs); err != nil {
			b.Fatalf("failed to download: %v", err)
		}
	}
}
