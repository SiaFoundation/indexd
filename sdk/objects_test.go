package sdk

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

// TestObjectID makes sure that we don't accidentally change the way object IDs
// are computed.
func TestObjectID(t *testing.T) {
	so := slabs.SealedObject{
		EncryptedDataKey:     []byte{1, 2, 3},
		EncryptedMetadataKey: []byte{3, 2, 1},
		Slabs: []slabs.SlabSlice{
			{
				EncryptionKey: [32]byte{4, 5, 6},
				MinShards:     1,
				Sectors: []slabs.PinnedSector{
					{
						Root:    [32]byte{7, 8, 9},
						HostKey: [32]byte{10, 11, 12},
					},
				},
				Offset: 131415,
				Length: 161718,
			},
		},
		EncryptedMetadata: []byte{19, 20, 21},
		DataSignature:     types.Signature{22, 23, 24},
		MetadataSignature: types.Signature{24, 23, 22},
		CreatedAt:         time.Unix(25, 26),
		UpdatedAt:         time.Unix(27, 28),
	}
	if so.ID().String() != "92b456fd0320c6595cf40280fafde2e3c549e09f6e7168ebdd963500830f50b5" {
		t.Fatalf("unexpected object ID: %s", so.ID())
	}
}

func TestSealedObjectRoundtrip(t *testing.T) {
	appKey := types.GeneratePrivateKey()

	ss := []slabs.SlabSlice{
		{Offset: 10, Length: 5000, EncryptionKey: frand.Entropy256(), Sectors: []slabs.PinnedSector{}},
		{Offset: 32, Length: 4096, EncryptionKey: frand.Entropy256(), Sectors: []slabs.PinnedSector{}},
	}
	obj := Object{
		dataKey:  frand.Bytes(32),
		slabs:    ss,
		metadata: frand.Bytes(128),
	}

	locked := obj.Seal(appKey)

	data, err := locked.MarshalSia()
	if err != nil {
		t.Fatal(err)
	}

	var decoded SealedObject
	if err := decoded.UnmarshalSia(data); err != nil {
		t.Fatal(err)
	}

	obj2, err := decoded.Open(appKey)
	if err != nil {
		t.Fatal(err)
	}

	obj2.createdAt = obj.createdAt
	obj2.updatedAt = obj.updatedAt
	if !reflect.DeepEqual(obj, obj2) {
		t.Fatalf("object mismatch: expected %+v, got %+v", obj, obj2)
	}
}

func TestObjectEquivalency(t *testing.T) {
	sk := types.GeneratePrivateKey()
	obj := Object{
		dataKey: frand.Bytes(32),
		slabs: func() []slabs.SlabSlice {
			ss := make([]slabs.SlabSlice, 30)
			for i := range ss {
				ss[i] = slabs.SlabSlice{
					EncryptionKey: frand.Entropy256(),
					MinShards:     10,
					Sectors: func() []slabs.PinnedSector {
						sectors := make([]slabs.PinnedSector, 30)
						for j := range sectors {
							sectors[j] = slabs.PinnedSector{
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
			return ss
		}(),
		metadata: json.RawMessage([]byte("{\"hello\": \"world\"}")),
	}
	objectID := obj.ID()
	so := obj.Seal(sk)
	if so.ID() != objectID {
		t.Fatalf("unexpected ID: got %v, want %v", so.ID(), objectID)
	} else if err := so.VerifySignatures(sk.PublicKey()); err != nil {
		t.Fatalf("unexpected error verifying signatures: %v", err)
	}

	pr := so.PinRequest()
	if pr.ID != objectID {
		t.Fatalf("unexpected ID: got %v, want %v", pr.ID, objectID)
	} else if pr.DataSigHash() != so.DataSigHash() {
		t.Fatalf("unexpected data sig hash: got %v, want %v", pr.DataSigHash(), so.DataSigHash())
	} else if pr.MetaSigHash() != so.MetaSigHash() {
		t.Fatalf("unexpected metadata sig hash: got %v, want %v", pr.MetaSigHash(), so.MetaSigHash())
	} else if err := pr.VerifySignatures(sk.PublicKey()); err != nil {
		t.Fatalf("unexpected error verifying signatures: %v", err)
	}
}
