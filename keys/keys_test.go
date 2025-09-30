package keys

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"testing"
)

func TestDerivePrivateKey(t *testing.T) {
	sk := decodeHex(t, "b3383d90bf82c4c9bfa445a9d6090977dc9333d302309bd002ba9407c42674b4581010a63b0682cc447cd970fdb8b7d634e1e6fec0631a310e53069c7e1b61d9")
	expected := decodeHex(t, "5d83d7281a0f5741684c57f95689b2710e1f2a1b32d67a6c6c036c18b61380467a785e0fc02b806005ec2a56c0bd5648539e2ec3093cbd74a0af7ac52ce45912")
	key := DerivePrivateKey(sk, "foo")

	if len(key) != ed25519.PrivateKeySize {
		t.Fatalf("unexpected key length: got %d, want %d", len(key), ed25519.PrivateKeySize)
	}
	if bytes.Equal(key, make([]byte, ed25519.PrivateKeySize)) {
		t.Fatal("expected non-zero key, got all zeros")
	}
	if bytes.Equal(key, DerivePrivateKey(sk, "bar")) {
		t.Fatal("expected different keys for different purposes")
	}
	if !bytes.Equal(key, DerivePrivateKey(sk, "foo")) {
		t.Fatal("expected same key for same purpose")
	}
	if !bytes.Equal(key, expected) {
		t.Fatal("derived key mismatch")
	}
}

func decodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("failed to decode hex string: %v", err)
	}
	return b
}
