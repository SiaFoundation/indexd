package sdk

import (
	"bytes"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestOutOfOrderDownload(t *testing.T) {
	dialer := newMockDialer(30)
	appKey := types.GeneratePrivateKey()
	s := newTestSDK(t, appKey, newMockAppClient(), dialer)
	defer s.Close()

	slabSize := uint64(proto.SectorSize) * 10
	data := frand.Bytes(int(slabSize))
	obj := NewEmptyObject()
	if err := s.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// decreasing per-sector delays make later chunks more likely to finish
	// first, which exercises ordered output.
	for _, slab := range obj.Slabs() {
		for _, sector := range slab.Sectors {
			dialer.SetSectorReadDelay(sector.Root, 500*time.Millisecond)
		}
	}

	buf := bytes.NewBuffer(nil)
	if err := s.Download(t.Context(), buf, obj, WithDownloadInflight(40)); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("data mismatch")
	}
}

func TestSlabRecovery(t *testing.T) {
	dialer := newMockDialer(30)
	appKey := types.GeneratePrivateKey()
	s := newTestSDK(t, appKey, newMockAppClient(), dialer)
	defer s.Close()

	slabSize := int(proto.SectorSize) * 10
	data := frand.Bytes(slabSize)
	obj := NewEmptyObject()
	if err := s.Upload(t.Context(), &obj, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		offset uint64
		length uint64
	}{
		{"full slab", 0, uint64(slabSize)},
		{"first half", 0, uint64(slabSize / 2)},
		{"second half", uint64(slabSize / 2), uint64(slabSize / 2)},
		{"first 30 bytes", 0, 30},
		{"middle 30 bytes", uint64(slabSize/2 - 15), 30},
		{"last 30 bytes", uint64(slabSize - 30), 30},
		{"first 4KiB", 0, 4096},
		{"middle 4KiB", uint64(slabSize/2 - 2048), 4096},
		{"last 4KiB", uint64(slabSize - 4096), 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			if err := s.Download(t.Context(), buf, obj, WithDownloadRange(tt.offset, tt.length)); err != nil {
				t.Fatalf("download failed: %v", err)
			}
			expected := data[tt.offset : tt.offset+tt.length]
			if !bytes.Equal(buf.Bytes(), expected) {
				t.Fatalf("data mismatch: got %d bytes, expected %d", buf.Len(), len(expected))
			}
		})
	}
}
