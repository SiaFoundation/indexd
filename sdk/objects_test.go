package sdk

import (
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestSealedObjectRoundtrip(t *testing.T) {
	appKey := types.GeneratePrivateKey()

	ss := []slabs.SlabSlice{
		{SlabID: slabs.SlabID(frand.Entropy256()), Offset: 10, Length: 5000},
		{SlabID: slabs.SlabID(frand.Entropy256()), Offset: 32, Length: 4096},
	}
	masterKey := frand.Bytes(32)
	obj := Object{
		masterKey: masterKey,
		slabs:     ss,
		metadata:  frand.Bytes(128),
	}

	locked := obj.Seal(appKey)

	data, err := locked.MarshalSia()
	if err != nil {
		t.Fatal(err)
	}

	var decoded slabs.SealedObject
	if err := decoded.UnmarshalSia(data); err != nil {
		t.Fatal(err)
	}

	obj2, err := objectFromSealedObject(decoded, appKey)
	if err != nil {
		t.Fatal(err)
	}

	obj2.createdAt = obj.createdAt
	obj2.updatedAt = obj.updatedAt
	if !reflect.DeepEqual(obj, obj2) {
		t.Fatalf("object mismatch: expected %+v, got %+v", obj, obj2)
	}
}
