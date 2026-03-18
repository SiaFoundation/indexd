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
