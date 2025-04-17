package slabs

import (
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// TestSlabPinParamsDigest is a unit test for the SlabPinParams.Digest method.
func TestSlabPinParamsDigest(t *testing.T) {
	params := SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors: []SectorPinParams{
			{
				Root:    frand.Entropy256(),
				HostKey: frand.Entropy256(),
			},
			{
				Root:    frand.Entropy256(),
				HostKey: frand.Entropy256(),
			},
		},
	}

	hasher := types.NewHasher()
	hasher.E.WriteUint64(uint64(params.MinShards))
	hasher.E.Write(params.EncryptionKey[:])
	for _, sector := range params.Sectors {
		if _, err := hasher.E.Write(sector.Root[:]); err != nil {
			t.Fatal(err)
		}
	}
	expectedID := SlabID(hasher.Sum())
	slabID, err := params.Digest()
	if err != nil {
		t.Fatal(err)
	} else if slabID != expectedID {
		t.Fatalf("expected %v, got %v", expectedID, slabID)
	}
}
