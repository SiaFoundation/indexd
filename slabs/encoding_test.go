package slabs

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestEncodeSlabSlice(t *testing.T) {
	s := SlabSlice{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []PinnedSector{
			{
				Root:    frand.Entropy256(),
				HostKey: frand.Entropy256(),
			},
		},
		Offset: 200,
		Length: 300,
	}

	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	s.EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	var s2 SlabSlice
	dec := types.NewBufDecoder(buf.Bytes())
	s2.DecodeFrom(dec)
	if err := dec.Err(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(s, s2) {
		t.Fatalf("decoded slab slice does not match original: got %+v, want %+v", s2, s)
	}
}

func TestEncodeSealedObject(t *testing.T) {
	so := SealedObject{
		Version:          1,
		EncryptedDataKey: frand.Bytes(72),
		Slabs: []SlabSlice{{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []PinnedSector{{
				Root:    frand.Entropy256(),
				HostKey: frand.Entropy256(),
			}},
			Offset: 200,
			Length: 300,
		}},
		DataSignature:        types.Signature(frand.Bytes(64)),
		EncryptedMetadataKey: frand.Bytes(72),
		EncryptedMetadata:    frand.Bytes(50),
		MetadataSignature:    types.Signature(frand.Bytes(64)),
		CreatedAt:            time.Unix(1700000000, 0).UTC(),
		UpdatedAt:            time.Unix(1700000123, 0).UTC(),
	}

	b, err := so.MarshalSia()
	if err != nil {
		t.Fatal(err)
	}

	var so2 SealedObject
	if err := so2.UnmarshalSia(b); err != nil {
		t.Fatal(err)
	} else if !so2.CreatedAt.Equal(so.CreatedAt) || !so2.UpdatedAt.Equal(so.UpdatedAt) {
		t.Fatalf("decoded timestamps do not match: got %v/%v, want %v/%v", so2.CreatedAt, so2.UpdatedAt, so.CreatedAt, so.UpdatedAt)
	}

	// ReadTime returns the instant in local time; compare the rest of the
	// fields after normalizing the (already-checked) timestamps.
	so2.CreatedAt, so2.UpdatedAt = so.CreatedAt, so.UpdatedAt
	if !reflect.DeepEqual(so, so2) {
		t.Fatalf("decoded object does not match original: got %+v, want %+v", so2, so)
	}
}
