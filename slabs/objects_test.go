package slabs

import (
	"reflect"
	"testing"
	"time"

	"lukechampine.com/frand"
)

func TestObjectEncoding(t *testing.T) {
	now := time.Now().Round(time.Second)
	metadata := make([]byte, frand.Uint64n(128))
	frand.Read(metadata)

	var obj Object
	frand.Read(obj.Key[:])
	obj.Slabs = []SlabSlice{
		{SlabID: SlabID(frand.Entropy256()), Offset: 10, Length: 5000},
		{SlabID: SlabID(frand.Entropy256()), Offset: 32, Length: 4096},
	}
	obj.Meta = metadata
	obj.CreatedAt = now
	obj.UpdatedAt = now

	b, err := obj.MarshalSia()
	if err != nil {
		t.Fatal(err)
	}

	var decoded Object
	if err := decoded.UnmarshalSia(b); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(obj, decoded) {
		t.Fatalf("mismatch after marshaling and unmarshaling: expected %v, got %v", obj, decoded)
	}
}
