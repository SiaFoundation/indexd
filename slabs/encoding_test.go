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

func TestEncodePinnedSlabs(t *testing.T) {
	slabs := PinnedSlabs{
		{
			ID:            SlabID(frand.Entropy256()),
			Version:       1,
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []PinnedSector{
				{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
				{Root: frand.Entropy256()}, // lost sector
			},
		},
		{
			ID:            SlabID(frand.Entropy256()),
			EncryptionKey: frand.Entropy256(),
			MinShards:     2,
			Sectors: []PinnedSector{
				{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
				{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
			},
		},
	}

	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	slabs.EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	var decoded PinnedSlabs
	dec := types.NewBufDecoder(buf.Bytes())
	decoded.DecodeFrom(dec)
	if err := dec.Err(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(slabs, decoded) {
		t.Fatalf("decoded slab batch does not match original: got %+v, want %+v", decoded, slabs)
	}
}

func TestEncodeObjectEvents(t *testing.T) {
	events := ObjectEvents{
		{
			Key:       frand.Entropy256(),
			UpdatedAt: time.Unix(1700000000, 123456789).UTC(),
			Object: &SealedObject{
				EncryptedDataKey: frand.Bytes(72),
				Slabs: []SlabSlice{{
					Version:       1,
					EncryptionKey: frand.Entropy256(),
					MinShards:     1,
					Sectors: []PinnedSector{
						{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
						{Root: frand.Entropy256()}, // lost sector
					},
					Offset: 10,
					Length: 100,
				}},
				DataSignature:        types.Signature(frand.Bytes(64)),
				EncryptedMetadataKey: frand.Bytes(72),
				EncryptedMetadata:    frand.Bytes(100),
				MetadataSignature:    types.Signature(frand.Bytes(64)),
				CreatedAt:            time.Unix(1600000000, 1).UTC(),
				UpdatedAt:            time.Unix(1700000000, 123456789).UTC(),
			},
		},
		{
			Key:       frand.Entropy256(),
			UpdatedAt: time.Unix(1700000001, 0).UTC(),
			Object: &SealedObject{ // optional fields absent
				EncryptedDataKey: frand.Bytes(72),
				Slabs: []SlabSlice{{
					EncryptionKey: frand.Entropy256(),
					MinShards:     1,
					Sectors:       []PinnedSector{{Root: frand.Entropy256(), HostKey: frand.Entropy256()}},
					Length:        1,
				}},
				DataSignature: types.Signature(frand.Bytes(64)),
				CreatedAt:     time.Unix(1600000001, 0).UTC(),
				UpdatedAt:     time.Unix(1700000001, 0).UTC(),
			},
		},
		{
			Key:       frand.Entropy256(),
			Deleted:   true,
			UpdatedAt: time.Unix(1700000002, 999999999).UTC(),
		},
	}

	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	events.EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	var decoded ObjectEvents
	dec := types.NewBufDecoder(buf.Bytes())
	decoded.DecodeFrom(dec)
	if err := dec.Err(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(events, decoded) {
		t.Fatalf("decoded events do not match original: got %+v, want %+v", decoded, events)
	}
}

func TestEncodeObjectEventReferences(t *testing.T) {
	events := ObjectEventReferences{
		{
			Key:       frand.Entropy256(),
			UpdatedAt: time.Unix(1700000000, 123456789).UTC(),
			Object: &SealedObjectReference{
				EncryptedDataKey: frand.Bytes(72),
				Slabs: []ObjectSlab{
					{ID: SlabID(frand.Entropy256()), Offset: 200, Length: 300},
					{ID: SlabID(frand.Entropy256()), Length: 1 << 22},
				},
				DataSignature:        types.Signature(frand.Bytes(64)),
				EncryptedMetadataKey: frand.Bytes(72),
				EncryptedMetadata:    frand.Bytes(100),
				MetadataSignature:    types.Signature(frand.Bytes(64)),
				CreatedAt:            time.Unix(1600000000, 1).UTC(),
				UpdatedAt:            time.Unix(1700000000, 123456789).UTC(),
			},
		},
		{
			Key:       frand.Entropy256(),
			UpdatedAt: time.Unix(1700000001, 0).UTC(),
			Object: &SealedObjectReference{ // optional fields absent
				EncryptedDataKey: frand.Bytes(72),
				Slabs:            []ObjectSlab{{ID: SlabID(frand.Entropy256()), Length: 1}},
				DataSignature:    types.Signature(frand.Bytes(64)),
			},
		},
		{
			Key:       frand.Entropy256(),
			Deleted:   true,
			UpdatedAt: time.Unix(1700000002, 999999999).UTC(),
		},
	}

	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	events.EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	var decoded ObjectEventReferences
	dec := types.NewBufDecoder(buf.Bytes())
	decoded.DecodeFrom(dec)
	if err := dec.Err(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(events, decoded) {
		t.Fatalf("decoded events do not match original: got %+v, want %+v", decoded, events)
	}
}
