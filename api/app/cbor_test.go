package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
	"lukechampine.com/frand"
)

func TestEncodeResponseNegotiation(t *testing.T) {
	uploadedAt := time.Now().UTC().Truncate(time.Nanosecond)
	slab := slabs.PinnedSlab{
		ID:            slabs.SlabID(frand.Entropy256()),
		Version:       1,
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors: []slabs.PinnedSector{{
			Root:       frand.Entropy256(),
			HostKey:    types.GeneratePrivateKey().PublicKey(),
			UploadedAt: &uploadedAt,
		}},
	}

	tests := []struct {
		name   string
		accept string
	}{
		{"no accept header", ""},
		{"json", applicationJSON},
		{"cbor", applicationCBOR},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accept := tt.accept
			req := httptest.NewRequest(http.MethodGet, "/slabs/"+slab.ID.String(), nil)
			if accept != "" {
				req.Header.Set(acceptHeader, accept)
			}
			rec := httptest.NewRecorder()
			encodeResponse(jape.Context{ResponseWriter: rec, Request: req}, slab)

			resp := rec.Result()
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			} else if vary := resp.Header.Values(varyHeader); !slices.Contains(vary, acceptHeader) {
				t.Fatalf("expected the response to vary on %s, got %v", acceptHeader, vary)
			}

			var decoded slabs.PinnedSlab
			if accept == applicationCBOR {
				if ct := resp.Header.Get(contentTypeHeader); ct != applicationCBOR {
					t.Fatalf("expected %s, got %s", applicationCBOR, ct)
				} else if err := decodeCBOR(resp.Body, &decoded); err != nil {
					t.Fatal(err)
				}
			} else if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, slab) {
				t.Fatalf("expected %+v, got %+v", slab, decoded)
			}
		})
	}
}

func TestAcceptsCBOR(t *testing.T) {
	tests := []struct {
		name   string
		accept string
		want   bool
	}{
		{"empty", "", false},
		{"json", applicationJSON, false},
		{"cbor", applicationCBOR, true},
		{"cbor with quality and space", "  application/cbor;q=1 ", true},
		{"cbor uppercase", "APPLICATION/CBOR", true},
		{"cbor after json", "application/json;q=0.5, application/cbor", true},
		{"cbor before wildcard", "application/cbor, */*;q=0.8", true},
		{"wildcard", "*/*", false},
		{"other types", "text/html, application/xml;q=0.9", false},
		{"cbor prefix", "application/cbor-seq", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := acceptsCBOR(tt.accept); got != tt.want {
				t.Fatalf("acceptsCBOR(%q) = %v, want %v", tt.accept, got, tt.want)
			}
		})
	}
}

func TestCBORObjectEventRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	key := types.Hash256(frand.Entropy256())
	events := []slabs.ObjectEvent{
		{
			Key:       key,
			UpdatedAt: now,
			Object: &slabs.SealedObject{
				EncryptedDataKey: frand.Bytes(48),
				Slabs: []slabs.SlabSlice{{
					Version:       1,
					EncryptionKey: frand.Entropy256(),
					MinShards:     10,
					Sectors: []slabs.PinnedSector{{
						Root:    frand.Entropy256(),
						HostKey: types.GeneratePrivateKey().PublicKey(),
					}},
					Offset: 5,
					Length: 100,
				}},
				DataSignature:        types.Signature(frand.Bytes(64)),
				EncryptedMetadataKey: frand.Bytes(48),
				EncryptedMetadata:    frand.Bytes(128),
				MetadataSignature:    types.Signature(frand.Bytes(64)),
				CreatedAt:            now.Add(-time.Hour),
				UpdatedAt:            now,
			},
		},
		{Key: types.Hash256(frand.Entropy256()), Deleted: true, UpdatedAt: now},
	}

	buf, err := encodeCBOR(events)
	if err != nil {
		t.Fatal(err)
	}

	// hashes and keys must encode as byte strings rather than arrays of ints
	if !bytes.Contains(buf, append([]byte{0x58, 0x20}, key[:]...)) {
		t.Fatal("expected the object key to be encoded as a 32 byte string")
	}

	var decoded []slabs.ObjectEvent
	if err := decodeCBOR(bytes.NewReader(buf), &decoded); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(decoded, events) {
		t.Fatalf("expected %+v, got %+v", events, decoded)
	}
}
