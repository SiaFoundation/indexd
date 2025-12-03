package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type countWriter struct {
	count int

	w io.Writer
}

func (c *countWriter) Write(p []byte) (int, error) {
	c.count++
	return c.w.Write(p)
}

func TestRoundtripCount(t *testing.T) {
	appKey := types.GeneratePrivateKey()
	dialer := newMockDialer(50)
	s := initSDK(appKey, newMockAppClient(), dialer)
	defer s.Close()

	// 1 MB
	data := frand.Bytes(1 << 20)
	obj, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(obj.Slabs()) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
	} else if obj.Slabs()[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), obj.Slabs()[0].Length)
	}

	buf := bytes.NewBuffer(nil)
	cw := &countWriter{w: buf}
	if err := s.Download(context.Background(), cw, obj); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}
	t.Logf("Downloaded: %d bytes, Write calls: %d", buf.Len(), cw.count)
}

func TestUpload(t *testing.T) {
	appKey := types.GeneratePrivateKey()
	dialer := newMockDialer(50)
	s := initSDK(appKey, newMockAppClient(), dialer)
	defer s.Close()
	data := frand.Bytes(4096)

	t.Run("timeout", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make enough hosts timeout to fail
		dialer.SetSlowHosts(30, time.Second)
		_, err := s.Upload(context.Background(), bytes.NewReader(data), WithUploadHostTimeout(100*time.Millisecond))
		if !errors.Is(err, ErrNoMoreHosts) {
			t.Fatalf("expected ErrNoMoreHosts, got %v", err)
		}
	})

	t.Run("slow", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make most of the hosts slow,
		// but not enough to fail to upload
		dialer.SetSlowHosts(20, time.Second)
		obj, err := s.Upload(context.Background(), bytes.NewReader(data), WithUploadHostTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		} else if len(obj.Slabs()) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
		}
	})
}

func TestDownload(t *testing.T) {
	dialer := newMockDialer(30)
	appKey := types.GeneratePrivateKey()
	s := initSDK(appKey, newMockAppClient(), dialer)
	defer s.Close()

	slabSize := uint64(proto.SectorSize) * 10
	dataSize := slabSize * 3 // 3 slabs
	data := frand.Bytes(int(dataSize))

	obj, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	err = s.client.SaveObject(t.Context(), appKey, obj.Seal(appKey))
	if err != nil {
		t.Fatalf("failed to save object to mock client: %v", err)
	}

	sharedURL, err := s.CreateSharedObjectURL(t.Context(), obj.ID(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("failed to create shared object URL: %v", err)
	}

	t.Run("full", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		if err = s.Download(context.Background(), buf, obj); err != nil {
			t.Fatalf("failed to download: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}

		buf.Reset()
		if err := s.DownloadSharedObject(t.Context(), buf, sharedURL); err != nil {
			t.Fatalf("failed to download shared object: %v", err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}
	})

	t.Run("ranges", func(t *testing.T) {
		randomOffsetLength := func() [2]uint64 {
			offset := frand.Uint64n(dataSize - 1)
			length := frand.Uint64n(dataSize - offset + 1)
			return [2]uint64{offset, length}
		}

		cases := [][2]uint64{
			{0, proto.SectorSize},
			{proto.SectorSize, proto.SectorSize},
			{proto.LeafSize, proto.LeafSize},
			{proto.LeafSize + 1, proto.LeafSize / 2},            // within a leaf
			{proto.LeafSize + proto.LeafSize/2, proto.LeafSize}, // across leaves
			{slabSize / 2, 2 * slabSize},                        // across slabs
			{dataSize - proto.SectorSize, proto.SectorSize},
			{dataSize - proto.LeafSize, proto.LeafSize},
			{dataSize, 0},
		}
		for range 10 {
			cases = append(cases, randomOffsetLength())
		}

		for _, c := range cases {
			buf := bytes.NewBuffer(nil)
			if err = s.Download(context.Background(), buf, obj, WithDownloadRange(c[0], c[1])); err != nil {
				t.Fatalf("failed to download: %v", err)
			} else if !bytes.Equal(buf.Bytes(), data[c[0]:c[0]+c[1]]) {
				t.Fatal("data mismatch")
			}

			buf.Reset()
			if err := s.DownloadSharedObject(t.Context(), buf, sharedURL, WithDownloadRange(c[0], c[1])); err != nil {
				t.Fatalf("failed to download shared object: %v", err)
			} else if !bytes.Equal(buf.Bytes(), data[c[0]:c[0]+c[1]]) {
				t.Fatal("data mismatch")
			}
		}

		// assert that out-of-bounds ranges fail
		if err := s.Download(context.Background(), nil, obj, WithDownloadRange(dataSize, 1)); err == nil {
			t.Fatal("expected error for out-of-bounds range, got nil")
		} else if err := s.DownloadSharedObject(t.Context(), nil, sharedURL, WithDownloadRange(dataSize, 1)); err == nil {
			t.Fatal("expected error for out-of-bounds range, got nil")
		}
	})

	t.Run("timeout", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make enough hosts timeout to fail to download
		dialer.SetSlowHosts(21, time.Second)
		buf := bytes.NewBuffer(nil)
		err = s.Download(context.Background(), buf, obj, WithDownloadHostTimeout(200*time.Millisecond))
		if !errors.Is(err, ErrNotEnoughShards) {
			t.Fatalf("expected ErrNotEnoughShards, got %v", err)
		}
	})

	t.Run("slow", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make most of the hosts timeout
		dialer.SetSlowHosts(20, time.Second)
		buf := bytes.NewBuffer(nil)
		err = s.Download(context.Background(), buf, obj, WithDownloadHostTimeout(200*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}
	})
}

func TestE2E(t *testing.T) {
	log := zaptest.NewLogger(t)
	ms := testutils.MaintenanceSettings
	ms.WantedContracts = 15
	cluster := testutils.NewCluster(t, testutils.WithHosts(15), testutils.WithLogger(log.Named("cluster")), testutils.WithIndexer(testutils.WithMaintenanceSettings(ms)))

	privateKey := types.GeneratePrivateKey()
	cluster.Indexer.AddTestAccount(t, privateKey.PublicKey())
	cluster.WaitForContracts(t)

	b := NewBuilder(cluster.Indexer.AppAPIAddr(), AppMetadata{})
	client, err := b.SDK(&AppKey{privateKey}, WithLogger(log.Named("sdk")))
	if err != nil {
		t.Fatal(err)
	}

	data := frand.Bytes(4096)
	obj, err := client.Upload(context.Background(), bytes.NewReader(data), WithRedundancy(2, 8))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	if err := client.Download(context.Background(), buf, obj); err != nil {
		t.Fatalf("failed to download: %v", err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Log(data[:64])
		t.Log(buf.Bytes()[:64])
		t.Fatal("data mismatch")
	}
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

			s := initSDK(appKey, newMockAppClient(), dialer)
			defer s.Close()

			r := bytes.NewReader(data)
			b.SetBytes(benchmarkSize)
			b.ResetTimer()
			for b.Loop() {
				r.Reset(data)
				if _, err := s.Upload(context.Background(), r, WithUploadInflight(inflight)); err != nil {
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

	appKey := types.GeneratePrivateKey()
	dialer := newMockDialer(30)
	s := initSDK(appKey, newMockAppClient(), dialer)
	defer s.Close()

	data := frand.Bytes(benchmarkSize)
	obj, err := s.Upload(context.Background(), bytes.NewReader(data))
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
				err = s.Download(context.Background(), buf, obj, WithDownloadInflight(inflight))
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
