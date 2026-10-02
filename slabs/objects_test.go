package slabs

import (
	"math"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestObjectEquivalency(t *testing.T) {
	sk := types.GeneratePrivateKey()
	so := SealedObject{
		EncryptedDataKey: frand.Bytes(32 + 16),
		Slabs: func() []SlabSlice {
			slabs := make([]SlabSlice, 30)
			for i := range slabs {
				slabs[i] = SlabSlice{
					EncryptionKey: frand.Entropy256(),
					MinShards:     10,
					Sectors: func() []PinnedSector {
						sectors := make([]PinnedSector, 30)
						for j := range sectors {
							sectors[j] = PinnedSector{
								Root:    frand.Entropy256(),
								HostKey: frand.Entropy256(),
							}
						}
						return sectors
					}(),
					Offset: uint32(frand.Uint64n(math.MaxUint32)),
					Length: uint32(frand.Uint64n(math.MaxUint32)),
				}
			}
			return slabs
		}(),
		EncryptedMetadataKey: frand.Bytes(32 + 16),
		EncryptedMetadata:    frand.Bytes(32),
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}
	so.Sign(sk)

	pr := so.PinRequest()
	if pr.ID != so.ID() {
		t.Fatalf("unexpected ID: got %v, want %v", pr.ID, so.ID())
	} else if pr.DataSigHash() != so.DataSigHash() {
		t.Fatalf("unexpected data sig hash: got %v, want %v", pr.DataSigHash(), so.DataSigHash())
	} else if pr.MetaSigHash() != so.MetaSigHash() {
		t.Fatalf("unexpected metadata sig hash: got %v, want %v", pr.MetaSigHash(), so.MetaSigHash())
	} else if err := pr.VerifySignatures(sk.PublicKey()); err != nil {
		t.Fatalf("unexpected error verifying signatures: %v", err)
	}
}

func TestSealedObjectSize(t *testing.T) {
	tests := []struct {
		slices []SlabSlice
		size   uint64
	}{
		{slices: nil, size: 0},
		{slices: []SlabSlice{{Length: 100}}, size: 100},
		{slices: []SlabSlice{{Offset: 10, Length: 100}, {Length: 50}}, size: 150},
		// slice lengths are summed as uint64 so they can't overflow
		{slices: []SlabSlice{{Length: math.MaxUint32}, {Length: math.MaxUint32}}, size: 2 * math.MaxUint32},
	}
	for _, test := range tests {
		so := SealedObject{Slabs: test.slices}
		pr := so.PinRequest()
		withoutSlabs := so.WithoutSlabs()
		if so.Size() != test.size {
			t.Fatalf("expected size %d, got %d", test.size, so.Size())
		} else if pr.Size() != test.size {
			t.Fatalf("expected pin request size %d, got %d", test.size, pr.Size())
		} else if withoutSlabs.Size != test.size {
			t.Fatalf("expected size without slabs %d, got %d", test.size, withoutSlabs.Size)
		} else if got := withoutSlabs.WithSlabs(test.slices).Size(); got != test.size {
			t.Fatalf("expected reassembled size %d, got %d", test.size, got)
		}
	}
}
