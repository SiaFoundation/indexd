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

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestRoundtrip(t *testing.T) {
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()

	s, err := initSDK(newMockAppClient(), dialer, appKey)
	if err != nil {
		t.Fatal(err)
	}

	// each upload has varying encryption configurations so we should get
	// different slab IDs every time
	seen := make(map[slabs.SlabID]struct{})
	checkSlabIDs := func(slabs []Slab) {
		t.Helper()

		for _, slab := range slabs {
			if _, ok := seen[slab.ID]; ok {
				t.Fatal("slab ID seen twice")
			}
			seen[slab.ID] = struct{}{}
		}
	}

	// without client side encryption
	data := frand.Bytes(4096)
	obj, err := s.Upload(context.Background(), bytes.NewReader(data), WithDisableEncryption())
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs))
	} else if obj.Slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), obj.Slabs[0].Length)
	}
	checkSlabIDs(obj.Slabs)

	buf := bytes.NewBuffer(nil)
	if err := s.Download(context.Background(), buf, obj); err != nil {
		t.Fatal(err)
	}

	// with client side encryption
	obj, err = s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs))
	} else if obj.Slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), obj.Slabs[0].Length)
	}
	checkSlabIDs(obj.Slabs)

	buf = bytes.NewBuffer(nil)
	if err := s.Download(context.Background(), buf, obj); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}

	// with client side encryption, custom key
	var key [32]byte
	frand.Read(key[:])
	obj, err = s.Upload(context.Background(), bytes.NewReader(data), WithXChaCha20Secret(key))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs))
	} else if obj.Slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), obj.Slabs[0].Length)
	}
	checkSlabIDs(obj.Slabs)

	buf = bytes.NewBuffer(nil)
	if err := s.Download(context.Background(), buf, obj); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}

	if _, err = s.Upload(context.Background(), bytes.NewReader(data), WithDisableEncryption(), WithXChaCha20Secret(key)); err == nil {
		t.Fatal("expected error when disabling encryption but still passing custom key")
	}
}

type countWriter struct {
	count int

	w io.Writer
}

func (c *countWriter) Write(p []byte) (int, error) {
	c.count++
	return c.w.Write(p)
}

func TestRoundtripCount(t *testing.T) {
	dialer := newMockDialer(50)

	appKey := types.GeneratePrivateKey()

	s, err := initSDK(newMockAppClient(), dialer, appKey)
	if err != nil {
		t.Fatal(err)
	}

	// 1 MB
	data := frand.Bytes(1 << 20)
	obj, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(obj.Slabs))
	} else if obj.Slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), obj.Slabs[0].Length)
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
	dialer := newMockDialer(50)
	appKey := types.GeneratePrivateKey()
	s, err := initSDK(newMockAppClient(), dialer, appKey)
	if err != nil {
		t.Fatal(err)
	}
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
		} else if len(obj.Slabs) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(obj.Slabs))
		}
	})
}

func TestDownload(t *testing.T) {
	dialer := newMockDialer(30)

	appKey := types.GeneratePrivateKey()

	s, err := initSDK(newMockAppClient(), dialer, appKey)
	if err != nil {
		t.Fatal(err)
	}

	data := frand.Bytes(4096)
	obj, err := s.Upload(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	if err = s.Download(context.Background(), buf, obj); err != nil {
		t.Fatalf("failed to download: %v", err)
	}

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

			s, err := initSDK(newMockAppClient(), dialer, appKey)
			if err != nil {
				b.Fatal(err)
			}

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
	dialer := newMockDialer(30)

	appKey := types.GeneratePrivateKey()

	s, err := initSDK(newMockAppClient(), dialer, appKey)
	if err != nil {
		b.Fatal(err)
	}

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
