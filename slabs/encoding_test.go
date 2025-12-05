package slabs

import (
	"bytes"
	"reflect"
	"testing"

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
