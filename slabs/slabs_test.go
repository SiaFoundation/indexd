package slabs

import (
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestSlabPinParamsValidate(t *testing.T) {
	params := SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []PinnedSector{
			{
				Root:    frand.Entropy256(),
				HostKey: frand.Entropy256(),
			},
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
	if err := params.Validate(); err != nil {
		t.Fatal("unexpected", err)
	}

	// assert empty encryption key is illegal
	params.EncryptionKey = [32]byte{}
	if err := params.Validate(); err == nil || !strings.Contains(err.Error(), "encryption key is empty") {
		t.Fatal("unexpected", err)
	}

	// assert duplicate host keys are illegal
	params.EncryptionKey = frand.Entropy256()
	params.Sectors[2] = params.Sectors[1]
	if err := params.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate host key") {
		t.Fatal("unexpected", err)
	}

	// assert insufficient redundancy is illegal
	params.Sectors = params.Sectors[:1]
	if err := params.Validate(); err == nil || !strings.Contains(err.Error(), "minimum redundancy of 3x is not met") {
		t.Fatal("unexpected", err)
	}

	// assert exceeding max total shards is illegal
	params.Sectors = make([]PinnedSector, MaxTotalShards+1)
	if err := params.Validate(); err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatal("unexpected", err)
	}
}

// TestSlabPinParamsDigest is a unit test for the SlabPinParams.Digest method.
func TestSlabPinParamsDigest(t *testing.T) {
	params := SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors: []PinnedSector{
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
