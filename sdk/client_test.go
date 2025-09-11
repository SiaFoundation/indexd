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
	s, err := initSDK(newMockAppClient(), dialer, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	data := frand.Bytes(4096)

	// assertRoundtrip uploads the data and asserts download matches the original data
	seen := make(map[slabs.SlabID]struct{})
	assertRoundtrip := func(encryptionKey *[32]byte) {
		t.Helper()

		buf := bytes.NewBuffer(nil)
		slabs, err := s.Upload(context.Background(), EncryptStream(encryptionKey, bytes.NewReader(data), 0))
		if err != nil {
			t.Fatal(err)
		} else if len(slabs) != 1 {
			t.Fatal("expected 1 slab, got", len(slabs))
		} else if slabs[0].Length != uint32(len(data)) {
			t.Fatal("expected slab length", len(data), "got", slabs[0].Length)
		} else if err := s.Download(context.Background(), DecryptStream(encryptionKey, buf, 0), slabs); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}

		for _, slab := range slabs {
			if _, ok := seen[slab.SlabID]; ok {
				t.Fatal("duplicate slabID")
			}
			seen[slab.SlabID] = struct{}{}
		}
	}

	var encryptionKey [32]byte
	assertRoundtrip(&encryptionKey) // passthrough encryption
	frand.Read(encryptionKey[:])
	assertRoundtrip(&encryptionKey) // random encryption key
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
	s, err := initSDK(newMockAppClient(), newMockDialer(50), types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	ek := randomEncryptionKey()

	// 1 MB
	data := frand.Bytes(1 << 20)
	slabs, err := s.Upload(context.Background(), EncryptStream(ek, bytes.NewReader(data), 0))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	} else if len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(slabs))
	} else if slabs[0].Length != uint32(len(data)) {
		t.Fatalf("expected slab length %d, got %d", len(data), slabs[0].Length)
	}

	buf := bytes.NewBuffer(nil)
	cw := &countWriter{w: buf}
	if err := s.Download(context.Background(), DecryptStream(ek, cw, 0), slabs); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}

	t.Logf("Downloaded: %d bytes, Write calls: %d", buf.Len(), cw.count)
}

func TestUpload(t *testing.T) {
	dialer := newMockDialer(50)
	s, err := initSDK(newMockAppClient(), dialer, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	sk := randomEncryptionKey()
	data := frand.Bytes(4096)

	t.Run("timeout", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make enough hosts timeout to fail
		dialer.SetSlowHosts(30, time.Second)
		_, err := s.Upload(context.Background(), EncryptStream(sk, bytes.NewReader(data), 0), WithUploadHostTimeout(100*time.Millisecond))
		if !errors.Is(err, ErrNoMoreHosts) {
			t.Fatalf("expected ErrNoMoreHosts, got %v", err)
		}
	})

	t.Run("slow", func(t *testing.T) {
		dialer.ResetSlowHosts()
		// make most of the hosts slow, but not enough to fail to upload
		dialer.SetSlowHosts(20, time.Second)
		slabs, err := s.Upload(context.Background(), EncryptStream(sk, bytes.NewReader(data), 0), WithUploadHostTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		} else if len(slabs) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(slabs))
		}
	})
}

func TestDownload(t *testing.T) {
	dialer := newMockDialer(30)
	s, err := initSDK(newMockAppClient(), dialer, types.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	ek := randomEncryptionKey()
	data := frand.Bytes(4096)
	slabs, err := s.Upload(context.Background(), EncryptStream(ek, bytes.NewReader(data), 0))
	if err != nil {
		t.Fatalf("failed to upload: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	if err = s.Download(context.Background(), DecryptStream(ek, buf, 0), slabs); err != nil {
		t.Fatalf("failed to download: %v", err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}

	// make enough hosts timeout to fail to download
	t.Run("timeout", func(t *testing.T) {
		dialer.ResetSlowHosts()
		dialer.SetSlowHosts(21, time.Second)
		buf := bytes.NewBuffer(nil)
		err = s.Download(context.Background(), DecryptStream(ek, buf, 0), slabs, WithDownloadHostTimeout(200*time.Millisecond))
		if !errors.Is(err, ErrNotEnoughShards) {
			t.Fatalf("expected ErrNotEnoughShards, got %v", err)
		}
	})

	// make most of the hosts timeout
	t.Run("slow", func(t *testing.T) {
		dialer.ResetSlowHosts()
		dialer.SetSlowHosts(20, time.Second)
		buf := bytes.NewBuffer(nil)
		err = s.Download(context.Background(), DecryptStream(ek, buf, 0), slabs, WithDownloadHostTimeout(200*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkUpload(b *testing.B) {
	const benchmarkSize = 256 * 1000 * 1000 // 256 MB
	appKey := types.GeneratePrivateKey()

	sk := randomEncryptionKey()
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
				if _, err := s.Upload(context.Background(), EncryptStream(sk, r, 0), WithUploadInflight(inflight)); err != nil {
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
	s, err := initSDK(newMockAppClient(), dialer, types.GeneratePrivateKey())
	if err != nil {
		b.Fatal(err)
	}

	sk := randomEncryptionKey()
	data := frand.Bytes(benchmarkSize)

	slabs, err := s.Upload(context.Background(), EncryptStream(sk, bytes.NewReader(data), 0))
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
				err = s.Download(context.Background(), DecryptStream(sk, buf, 0), slabs, WithDownloadInflight(inflight))
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

func randomEncryptionKey() *[32]byte {
	var encryptionKey [32]byte
	frand.Read(encryptionKey[:])
	return &encryptionKey
}
