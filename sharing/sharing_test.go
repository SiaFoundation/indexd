package sharing_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sharing"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestKeyRequestSignature(t *testing.T) {
	privateKey := types.GeneratePrivateKey()
	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
	req := sharing.KeyRequest{
		Nonce:       sharing.Nonce(frand.Entropy256()),
		Description: "share",
		ExpiresAt:   &expiresAt,
	}
	req.Sign(privateKey)

	if req.PublicKey != privateKey.PublicKey() {
		t.Fatalf("expected public key %v, got %v", privateKey.PublicKey(), req.PublicKey)
	} else if err := req.VerifySignature(); err != nil {
		t.Fatal(err)
	}

	buf, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var decoded sharing.KeyRequest
	if err := json.Unmarshal(buf, &decoded); err != nil {
		t.Fatal(err)
	} else if err := decoded.VerifySignature(); err != nil {
		t.Fatalf("signature did not survive JSON round trip: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*sharing.KeyRequest)
	}{
		{
			name: "public key",
			mutate: func(req *sharing.KeyRequest) {
				req.PublicKey = types.GeneratePrivateKey().PublicKey()
			},
		},
		{
			name: "nonce",
			mutate: func(req *sharing.KeyRequest) {
				req.Nonce[0]++
			},
		},
		{
			name: "description",
			mutate: func(req *sharing.KeyRequest) {
				req.Description = "tampered"
			},
		},
		{
			name: "expiration",
			mutate: func(req *sharing.KeyRequest) {
				t := req.ExpiresAt.Add(time.Second)
				req.ExpiresAt = &t
			},
		},
		{
			name: "signature",
			mutate: func(req *sharing.KeyRequest) {
				req.Signature = types.Signature{}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := req
			test.mutate(&mutated)
			if err := mutated.VerifySignature(); !errors.Is(err, sharing.ErrInvalidRequest) {
				t.Fatalf("expected ErrInvalidRequest, got %v", err)
			}
		})
	}
}

// TestDeriveSharingKeyGolden pins the sharing key derivation and the create
// request's sig hash to fixed vectors. The Rust SDK reimplements both from
// scratch and asserts these same constants in
// sia_storage/src/sharing.rs test_derive_and_sig_hash_golden, so any change
// here that does not break that test is a silent wire format split: a key
// derived by one implementation would not be the key the other's recipient
// imports, and a request signed by one would not verify for the other.
func TestDeriveSharingKeyGolden(t *testing.T) {
	const (
		wantSeed      = "a4175b19800301a44ae89269d95b4bde7b68a964a150e1b4888d3b639237f0a0"
		wantPublicKey = "ed25519:e225d520cb027055aa6ee1b870edfd290758964b25915cc78d9da7d24e036c6b"
		wantSigHash   = "49bdc0b8aab2919a5a58a74d3f6d5f151c7d16ac1742a4e4133ad6834f071238"
	)

	appKey := types.NewPrivateKeyFromSeed(bytes.Repeat([]byte{1}, 32))
	nonce := sharing.Nonce(bytes.Repeat([]byte{2}, sharing.NonceSize))

	sharingKey := sharing.DeriveSharingKey(appKey, nonce)
	// NewPrivateKeyFromSeed returns seed||publicKey, so the leading 32 bytes are
	// the seed handed to the recipient as the whole credential.
	if got := hex.EncodeToString(sharingKey[:32]); got != wantSeed {
		t.Fatalf("expected seed %s, got %s", wantSeed, got)
	} else if got := sharingKey.PublicKey().String(); got != wantPublicKey {
		t.Fatalf("expected public key %s, got %s", wantPublicKey, got)
	}

	expiresAt := time.Unix(1893456000, 0).UTC()
	req := sharing.KeyRequest{
		PublicKey:   sharingKey.PublicKey(),
		Nonce:       nonce,
		Description: "photos",
		ExpiresAt:   &expiresAt,
	}
	if got := req.SigHash().String(); got != wantSigHash {
		t.Fatalf("expected sig hash %s, got %s", wantSigHash, got)
	}

	req.Sign(sharingKey)
	if err := req.VerifySignature(); err != nil {
		t.Fatal(err)
	}
}

// TestNonceHexRoundTrip pins the nonce's wire form. It is the HKDF salt, so a
// recipient that decodes it differently derives a different key, and the Rust
// SDK (de)serializes it as a plain hex string with no prefix.
func TestNonceHexRoundTrip(t *testing.T) {
	golden := []struct {
		name string
		fill byte
		hex  string
	}{
		// matches the Rust SDK's test_nonce_hex_roundtrip
		{name: "0x02", fill: 0x02, hex: "02"},
		// hex digits that have a case, so lowercase is actually pinned
		{name: "0xab", fill: 0xab, hex: "ab"},
	}
	for _, test := range golden {
		t.Run(test.name, func(t *testing.T) {
			nonce := sharing.Nonce(bytes.Repeat([]byte{test.fill}, sharing.NonceSize))
			want := `"` + strings.Repeat(test.hex, sharing.NonceSize) + `"`

			buf, err := json.Marshal(nonce)
			if err != nil {
				t.Fatal(err)
			} else if string(buf) != want {
				t.Fatalf("expected %s, got %s", want, buf)
			}

			var decoded sharing.Nonce
			if err := json.Unmarshal(buf, &decoded); err != nil {
				t.Fatal(err)
			} else if decoded != nonce {
				t.Fatalf("expected nonce %x, got %x", nonce, decoded)
			}
		})
	}

	random := sharing.Nonce(frand.Entropy256())
	buf, err := json.Marshal(random)
	if err != nil {
		t.Fatal(err)
	}
	var roundTripped sharing.Nonce
	if err := json.Unmarshal(buf, &roundTripped); err != nil {
		t.Fatal(err)
	} else if roundTripped != random {
		t.Fatalf("expected nonce %x, got %x", random, roundTripped)
	}

	invalid := []struct {
		name string
		json string
	}{
		{name: "empty", json: `""`},
		{name: "too short", json: `"` + strings.Repeat("02", sharing.NonceSize-1) + `"`},
		{name: "too long", json: `"` + strings.Repeat("02", sharing.NonceSize+1) + `"`},
		{name: "not hex", json: `"zz` + strings.Repeat("02", sharing.NonceSize-1) + `"`},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			var decoded sharing.Nonce
			if err := json.Unmarshal([]byte(test.json), &decoded); err == nil {
				t.Fatalf("expected error unmarshaling %s", test.json)
			}
		})
	}
}

// TestSharedObjectRequestSigHashParity asserts that the shared object sig
// hashes in this package agree with the ones in slabs. The two are defined
// independently and share no code, yet a client attaches an object by copying
// the fields of a slabs.SealedObject sealed under the sharing key rather than
// re-signing them, so a divergence would make every attachment fail
// verification server side with no other test catching it.
func TestSharedObjectRequestSigHashParity(t *testing.T) {
	tests := []struct {
		name     string
		metadata bool
	}{
		{name: "with metadata", metadata: true},
		{name: "without metadata", metadata: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sharingKey := sharing.DeriveSharingKey(types.GeneratePrivateKey(), sharing.Nonce(frand.Entropy256()))

			sealed := slabs.SealedObject{
				EncryptedDataKey: frand.Bytes(sharing.EncryptionKeySize),
				Slabs: []slabs.SlabSlice{{
					EncryptionKey: frand.Entropy256(),
					MinShards:     10,
					Sectors: []slabs.PinnedSector{
						{Root: frand.Entropy256(), HostKey: frand.Entropy256()},
					},
					Length: 1 << 20,
				}},
			}
			if test.metadata {
				sealed.EncryptedMetadataKey = frand.Bytes(sharing.EncryptionKeySize)
				sealed.EncryptedMetadata = frand.Bytes(128)
			}
			// Sign covers the metadata hash even when both metadata fields are
			// nil, so the request must carry that signature rather than a zero
			// value.
			sealed.Sign(sharingKey)

			req := sharing.SharedObjectRequest{
				ObjectID:             sealed.ID(),
				EncryptedDataKey:     sealed.EncryptedDataKey,
				DataSignature:        sealed.DataSignature,
				EncryptedMetadataKey: sealed.EncryptedMetadataKey,
				EncryptedMetadata:    sealed.EncryptedMetadata,
				MetadataSignature:    sealed.MetadataSignature,
			}
			if err := req.VerifySignatures(sharingKey.PublicKey()); err != nil {
				t.Fatalf("sealed object signatures rejected by sharing: %v", err)
			}

			// and the reverse direction, so neither definition can drift alone
			req.Sign(sharingKey)
			sealed.DataSignature = req.DataSignature
			sealed.MetadataSignature = req.MetadataSignature
			if err := sealed.VerifySignatures(sharingKey.PublicKey()); err != nil {
				t.Fatalf("sharing signatures rejected by sealed object: %v", err)
			}
		})
	}
}
