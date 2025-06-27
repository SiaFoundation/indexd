package sdk_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"lukechampine.com/frand"
)

type mockHostDialer struct {
	hosts map[types.PublicKey]struct{}

	delayMu   sync.Mutex
	slowHosts map[types.PublicKey]time.Duration

	sectorsMu   sync.Mutex
	hostSectors map[types.PublicKey]map[types.Hash256][proto4.SectorSize]byte
}

// Hosts implements the [sdk.HostDialer] interface.
func (m *mockHostDialer) Hosts() []types.PublicKey {
	return slices.Collect(maps.Keys(m.hosts))
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

// WriteSector implements the [sdk.HostDialer] interface.
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

// ReadSector implements the [sdk.HostDialer] interface.
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

func TestRoundtrip(t *testing.T) {
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()

	s := sdk.NewSDK("", appKey, dialer)

	data := frand.Bytes(4096)
	slabs, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(slabs))
	} else if slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), slabs[0].Length)
	}

	buf := bytes.NewBuffer(nil)
	if err := s.Download(context.Background(), buf, slabs); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}
}

func TestUpload(t *testing.T) {
	dialer := newMockDialer(50)
	appKey := types.GeneratePrivateKey()
	s := sdk.NewSDK("", appKey, dialer)
	data := frand.Bytes(4096)

	t.Run("timeout", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make enough hosts timeout to fail
		dialer.SetSlowHosts(30, time.Second)
		_, err := s.Upload(context.Background(), bytes.NewReader(data), sdk.WithUploadHostTimeout(100*time.Millisecond))
		if !errors.Is(err, sdk.ErrNoMoreHosts) {
			t.Fatalf("expected ErrNoMoreHosts, got %v", err)
		}
	})

	t.Run("slow", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make most of the hosts slow,
		// but not enough to fail to upload
		dialer.SetSlowHosts(20, time.Second)
		slabs, err := s.Upload(context.Background(), bytes.NewReader(data), sdk.WithUploadHostTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		} else if len(slabs) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(slabs))
		}
	})
}

func TestDownload(t *testing.T) {
	dialer := newMockDialer(30)

	appKey := types.GeneratePrivateKey()

	s := sdk.NewSDK("", appKey, dialer)

	data := frand.Bytes(4096)
	slabs, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	if err = s.Download(context.Background(), buf, slabs); err != nil {
		t.Fatalf("failed to download: %v", err)
	}

	t.Run("timeout", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make enough hosts timeout to fail to download
		dialer.SetSlowHosts(21, time.Second)
		buf := bytes.NewBuffer(nil)
		err = s.Download(context.Background(), buf, slabs, sdk.WithDownloadHostTimeout(200*time.Millisecond))
		if !errors.Is(err, sdk.ErrNotEnoughShards) {
			t.Fatalf("expected ErrNotEnoughShards, got %v", err)
		}
	})

	t.Run("slow", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make most of the hosts timeout
		dialer.SetSlowHosts(20, time.Second)
		buf := bytes.NewBuffer(nil)
		err = s.Download(context.Background(), buf, slabs, sdk.WithDownloadHostTimeout(200*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkUpload(b *testing.B) {
	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	appKey := types.GeneratePrivateKey()
	data := frand.Bytes(benchmarkSize)

	benchMatrix := func(b *testing.B, slow, timeout, inflight int) {
		b.Helper()
		b.Run(fmt.Sprintf("slow %d timeout %d inflight %d", slow, timeout, inflight), func(b *testing.B) {
			dialer := newMockDialer(30 + timeout) // increase the chance that a timeout will affect us without failing the test
			dialer.ResetSlowHosts()
			dialer.SetSlowHosts(slow, time.Second)       // slow, but not too slow
			dialer.SetSlowHosts(timeout, 30*time.Second) // longer than the default timeout

			s := sdk.NewSDK("", appKey, dialer)

			r := bytes.NewReader(data)
			b.SetBytes(benchmarkSize)
			b.ResetTimer()
			for b.Loop() {
				r.Reset(data)
				if _, err := s.Upload(context.Background(), r, sdk.WithUploadInflight(inflight)); err != nil {
					b.Fatalf("failed to upload: %v", err)
				}
			}
		})
	}

	inflight := []int{runtime.NumCPU(), 5, 10, 20, 30}
	// testing more variants is not particularly useful
	slow := []int{0, 1, 3, 5}
	timeout := []int{0, 1, 3, 5}
	for _, s := range slow {
		for _, t := range timeout {
			for _, i := range inflight {
				benchMatrix(b, s, t, i)
			}
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	dialer := newMockDialer(30)

	appKey := types.GeneratePrivateKey()

	s := sdk.NewSDK("", appKey, dialer)

	data := frand.Bytes(benchmarkSize)
	slabs, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		b.Fatalf("failed to upload: %v", err)
	}

	benchMatrix := func(b *testing.B, slow, inflight int) {
		b.Helper()
		b.Run(fmt.Sprintf("slow %d inflight %d", slow, inflight), func(b *testing.B) {
			// needs to be longer than the default timeout
			dialer.SetSlowHosts(slow, 30*time.Second)

			buf := bytes.NewBuffer(nil)
			b.SetBytes(benchmarkSize)
			b.ResetTimer()
			for b.Loop() {
				buf.Reset()
				err = s.Download(context.Background(), buf, slabs, sdk.WithDownloadInflight(inflight))
				if err != nil {
					b.Fatalf("failed to download: %v", err)
				}
			}
		})
	}

	benchMatrix(b, 0, runtime.NumCPU())

	inflight := []int{1, 3, 5, 10, 20, 30}
	slow := []int{0, 1, 3, 5, 10, 20}

	for _, s := range slow {
		for _, i := range inflight {
			benchMatrix(b, s, i)
		}
	}
}
