package slabs_test

import (
	"fmt"
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestSlabPinParamsValidate(t *testing.T) {
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: func() (s []slabs.PinnedSector) {
			for range 30 {
				s = append(s, slabs.PinnedSector{
					Root:    frand.Entropy256(),
					HostKey: frand.Entropy256(),
				})
			}
			return s
		}(),
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
	if err := params.Validate(); err == nil || !strings.Contains(err.Error(), "not enough redundancy") {
		t.Fatal("unexpected", err)
	}

	// assert exceeding max total shards is illegal
	params.Sectors = make([]slabs.PinnedSector, slabs.MaxTotalShards+1)
	if err := params.Validate(); err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatal("unexpected", err)
	}
}

// TestSlabPinParamsDigest is a unit test for the SlabPinParams.Digest method.
func TestSlabPinParamsDigest(t *testing.T) {
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors: []slabs.PinnedSector{
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
	expectedID := slabs.SlabID(hasher.Sum())
	slabID := params.Digest()
	if slabID != expectedID {
		t.Fatalf("expected %v, got %v", expectedID, slabID)
	}
}

func TestValidateECParams(t *testing.T) {
	tests := []struct {
		minShards   int
		totalShards int
		ok          bool
	}{
		{
			minShards:   0,
			totalShards: 6,
			ok:          false,
		},
		{
			minShards:   6,
			totalShards: 0,
			ok:          false,
		},
		{
			minShards:   4,
			totalShards: 2,
			ok:          false,
		},
		{
			minShards:   1,
			totalShards: 3,
			ok:          false,
		},
		{
			minShards:   2,
			totalShards: 6,
			ok:          false,
		},
		{
			minShards:   4,
			totalShards: 8,
			ok:          false,
		},
		{
			minShards:   1,
			totalShards: 10,
			ok:          true,
		},
		{
			minShards:   10,
			totalShards: 30,
			ok:          true,
		},
		{
			minShards:   30,
			totalShards: 60,
			ok:          true,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d-of-%d", test.minShards, test.totalShards), func(t *testing.T) {
			err := slabs.ValidateECParams(test.minShards, test.totalShards)
			if (err == nil) != test.ok {
				t.Fatalf("expected %v, got %v", test.ok, err == nil)
			}
		})
	}
}
