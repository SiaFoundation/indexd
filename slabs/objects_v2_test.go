package slabs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

func TestObjectDigestVectors(t *testing.T) {
	// Fixed vectors generated independently with BLAKE2b-256 and a recursive
	// Merkle tree cover empty, balanced, and unbalanced trees.
	for _, tt := range []struct {
		n      int
		v1, v2 string
	}{
		{0, "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8", "4a39182aed68033cc399d010226ded35737c672257c3d1f246f70a2df6ac4616"},
		{1, "27beef4e522274d15e5812e45f985457602f253c159d9a31c08e7122e82f8ba1", "d186960aa6848d694ab541e6fbc5182be11120a0b3a2b34981584e5ada7f9d93"},
		{2, "b6e15a65a7788e5a08249a27f6cc859a38b3768167ea6e987697e2a406491221", "4cd30fb94a66dc81a72d65943f6b3ca11cad439ebe485e331ef435b9302ab658"},
		{3, "155bb43c781f14f2d1e254cb2d68d15683b5346143e3d40f0b85433b1f7c3aaa", "106568d0f203569e731cccd2d477e84d9656280299efb8e77d4945f6c47790b8"},
		{4, "dc31771f15dd358bdc1bb76c0190d529ba8ff4f893add6be8425ef50f0880431", "5a46c0eb75fad2e685834e59e0549e1bf671bad035516ac48ba881ea6de986b0"},
		{5, "a7c3b6923db58f27ec0921b9a2e4b811d9f023d4b9dcfb147cd1cacad28990fd", "d6b4fd23c98d0b949d32bf7c9283e015549a70bc2bb46e904c3ad4e0c61d72f7"},
		{7, "8972bfe2ac0f8317deb0f74896b95d27ee0701a73b3170ca9daacc774072c9d4", "158c5b335faf35c9b759818679a7dc86b842f6d21c819e9ada725f7ff5cf599f"},
	} {
		t.Run(fmt.Sprint(tt.n), func(t *testing.T) {
			slabs := make([]ObjectSlab, tt.n)
			for i := range slabs {
				slabs[i] = ObjectSlab{ID: SlabID{byte(i + 1)}, Offset: uint32((i + 1) * 7), Length: uint32((i + 1) * 11)}
			}
			if got := pinnedObjectID(slices.Values(slabs)); got.String() != tt.v1 {
				t.Fatalf("V1: got %s, want %s", got, tt.v1)
			} else if got := v2PinnedObjectID(slices.Values(slabs)); got.String() != tt.v2 {
				t.Fatalf("V2: got %s, want %s", got, tt.v2)
			}
		})
	}
}

func testSignedObject(t *testing.T, version string) (SealedObject, types.PrivateKey) {
	t.Helper()
	sk := types.GeneratePrivateKey()
	obj := SealedObject{
		EncryptedDataKey:     bytes.Repeat([]byte{1}, EncryptionKeySize),
		EncryptedMetadataKey: bytes.Repeat([]byte{2}, EncryptionKeySize),
		EncryptedMetadata:    []byte("metadata"),
		Slabs: []SlabSlice{
			{EncryptionKey: EncryptionKey{1}, MinShards: 1, Sectors: []PinnedSector{{Root: types.Hash256{1}}}, Offset: 7, Length: 11},
			{Version: 1, EncryptionKey: EncryptionKey{2}, MinShards: 1, Sectors: []PinnedSector{{Root: types.Hash256{2}}}, Offset: 13, Length: 23},
			{EncryptionKey: EncryptionKey{3}, MinShards: 1, Sectors: []PinnedSector{{Root: types.Hash256{3}}}, Offset: 31, Length: 47},
		},
		CreatedAt: time.Unix(123, 0),
		UpdatedAt: time.Unix(456, 0),
	}
	if version == "v2" {
		obj.Sign(sk)
	} else {
		id := ObjectDigest(obj.Slabs)
		obj.DataSignature = sk.SignHash(id.DataSigHash(obj.EncryptedDataKey))
		obj.MetadataSignature = sk.SignHash(id.MetaSigHash(obj.EncryptedMetadataKey, obj.EncryptedMetadata))
	}
	return obj, sk
}

func TestObjectRequestUpdates(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			obj, sk := testSignedObject(t, version)
			id, err := obj.SignedID(sk.PublicKey())
			if err != nil {
				t.Fatal(err)
			}
			update := obj.PinRequestForID(id)
			update.EncryptedMetadata = []byte("updated metadata")
			update.Sign(sk)
			if update.ID != id {
				t.Fatal("metadata update changed the ID")
			} else if err := update.Validate(sk.PublicKey()); err != nil {
				t.Fatal(err)
			}

			obj.Slabs[0].Length++
			changed := obj.PinRequest()
			changed.Sign(sk)
			if changed.ID != V2ObjectDigest(obj.Slabs) || changed.ID == id {
				t.Fatal("changed slabs did not produce a new V2 ID")
			} else if err := changed.Validate(sk.PublicKey()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestObjectIDVersions(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			obj, sk := testSignedObject(t, version)
			id, err := obj.SignedID(sk.PublicKey())
			if err != nil {
				t.Fatal(err)
			}
			want := ObjectDigest(obj.Slabs)
			if version == "v2" {
				want = obj.ID()
			}
			if id != want {
				t.Fatalf("resolved %s, want %s", id, want)
			}
			pr := obj.PinRequestForID(id)
			if err := pr.Validate(sk.PublicKey()); err != nil {
				t.Fatal(err)
			} else if err := obj.VerifySignatures(sk.PublicKey()); err != nil {
				t.Fatal(err)
			} else if pr.ID != id {
				t.Fatal("pinning changed the object ID")
			}
			defaultRequest := obj.PinRequest()
			if defaultRequest.ID != obj.ID() {
				t.Fatal("default pin request does not use V2")
			} else if defaultRequest.DataSignature != obj.DataSignature || defaultRequest.MetadataSignature != obj.MetadataSignature {
				t.Fatal("default pin request changed the signatures")
			}
			if err := defaultRequest.Validate(sk.PublicKey()); version == "v1" && err == nil || version == "v2" && err != nil {
				t.Fatalf("default pin request for %s signatures: %v", version, err)
			}
			if got := obj.WithoutSlabs().WithSlabs(obj.Slabs); !reflect.DeepEqual(*got, obj) {
				t.Fatal("reassembling an object changed it")
			}
			if ObjectDigest(obj.Slabs) != pinnedObjectID(slices.Values(pr.Slabs)) || obj.ID() != v2PinnedObjectID(slices.Values(pr.Slabs)) {
				t.Fatal("full and pinned object digests differ")
			}
			for _, tt := range []struct {
				name   string
				mutate func(*SealedObject)
			}{
				{"order", func(o *SealedObject) { o.Slabs[0], o.Slabs[1] = o.Slabs[1], o.Slabs[0] }},
				{"offset", func(o *SealedObject) { o.Slabs[0].Offset++ }},
				{"length", func(o *SealedObject) { o.Slabs[0].Length++ }},
				{"slab", func(o *SealedObject) { o.Slabs[0].EncryptionKey[0]++ }},
				{"count", func(o *SealedObject) { o.Slabs = o.Slabs[:2] }},
				{"data key", func(o *SealedObject) { o.EncryptedDataKey = bytes.Repeat([]byte{3}, EncryptionKeySize) }},
				{"metadata", func(o *SealedObject) { o.EncryptedMetadata = []byte("tampered") }},
				{"metadata key", func(o *SealedObject) { o.EncryptedMetadataKey = bytes.Repeat([]byte{4}, EncryptionKeySize) }},
			} {
				t.Run(tt.name, func(t *testing.T) {
					bad := obj
					bad.Slabs = slices.Clone(obj.Slabs)
					tt.mutate(&bad)
					if err := bad.VerifySignatures(sk.PublicKey()); err == nil {
						t.Fatal("accepted tampered object")
					}
					req := bad.PinRequestForID(id)
					if err := req.Validate(sk.PublicKey()); err == nil {
						t.Fatal("accepted tampered pin request")
					}
				})
			}
			// Both valid signatures must commit to the same ID, not one each.
			bad := obj
			bad.DataSignature = sk.SignHash(ObjectDigest(obj.Slabs).DataSigHash(obj.EncryptedDataKey))
			bad.MetadataSignature = sk.SignHash(obj.ID().MetaSigHash(obj.EncryptedMetadataKey, obj.EncryptedMetadata))
			if bad.VerifyDataSignature(sk.PublicKey()) != nil || bad.VerifyMetadataSignature(sk.PublicKey()) != nil {
				t.Fatal("expected each signature to be valid individually")
			}
			if _, err := bad.SignedID(sk.PublicKey()); err == nil {
				t.Fatal("accepted signatures for different IDs")
			}
			if bad.VerifySignatures(sk.PublicKey()) == nil {
				t.Fatal("accepted mixed signature versions")
			}
			for _, candidate := range []ObjectID{ObjectDigest(obj.Slabs), obj.ID()} {
				req := bad.PinRequestForID(candidate)
				if req.Validate(sk.PublicKey()) == nil {
					t.Fatal("accepted mixed-version pin request")
				}
			}
			bad = obj
			wrongID := ObjectID{42}
			bad.DataSignature = sk.SignHash(wrongID.DataSigHash(obj.EncryptedDataKey))
			bad.MetadataSignature = sk.SignHash(wrongID.MetaSigHash(obj.EncryptedMetadataKey, obj.EncryptedMetadata))
			req := bad.PinRequestForID(wrongID)
			if bad.VerifySignatures(sk.PublicKey()) == nil || req.Validate(sk.PublicKey()) == nil {
				t.Fatal("accepted a signed ID unrelated to the slabs")
			}
			if _, err := obj.SignedID(types.GeneratePrivateKey().PublicKey()); err == nil {
				t.Fatal("accepted wrong signer")
			}
		})
	}
}

func TestObjectIDEncoding(t *testing.T) {
	id := ObjectID{1, 2, 3}
	encoded, err := json.Marshal(id)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(types.Hash256(id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, legacy) {
		t.Fatalf("changed ID JSON encoding: %s != %s", encoded, legacy)
	}
	var decoded ObjectID
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != id {
		t.Fatalf("ID round trip: %v, %v", decoded, err)
	}
	for _, invalid := range []string{`"00"`, `"xyz"`, `[1,2,3]`} {
		if err := json.Unmarshal([]byte(invalid), &decoded); err == nil {
			t.Fatalf("accepted invalid ID %s", invalid)
		}
	}
}
