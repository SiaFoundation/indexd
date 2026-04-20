package admin

import (
	"testing"

	"go.sia.tech/core/types"
)

func TestDecodeHostKey(t *testing.T) {
	pk := types.GeneratePrivateKey().PublicKey()
	rawHex := pk.String()[len("ed25519:"):]

	// raw hex without prefix
	decoded, err := decodeHostKey(rawHex)
	if err != nil {
		t.Fatal(err)
	} else if decoded != pk {
		t.Fatalf("expected %v, got %v", pk, decoded)
	}

	// full prefixed key
	decoded, err = decodeHostKey(pk.String())
	if err != nil {
		t.Fatal(err)
	} else if decoded != pk {
		t.Fatalf("expected %v, got %v", pk, decoded)
	}

	// invalid input
	if _, err := decodeHostKey("not-a-key"); err == nil {
		t.Fatal("expected error for invalid input")
	}

	// too short hex
	if _, err := decodeHostKey(rawHex[:32]); err == nil {
		t.Fatal("expected error for truncated key")
	}
}
