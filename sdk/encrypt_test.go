package sdk

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// TestDeriveAppKeyGolden tests that deriving an app key from
// a known mnemonic, app ID, and shared secret produces
// the expected app key. This is to ensure compatibility
// with other implementations.
func TestDeriveAppKeyGolden(t *testing.T) {
	const (
		mnemonic        = "glare own entire dish exact open theme family harsh room scrap rose"
		appIDStr        = "0e90d697f5045a6593f1c43ebf79a369e2bc72cc5c7b6282f3b5aeb0de6e4005"
		sharedSecretStr = "cf02d945fe4bfe614d823dc13c19aa8501699e656d0f7915490c3056d5c97dc6"
		expectedAppKey  = "t1Bh80uzrqsjKwZx2i0DR8VHNDoAJrtVNcKR2WT9CaE"
	)

	var appID, sharedSecret types.Hash256
	if err := appID.UnmarshalText([]byte(appIDStr)); err != nil {
		t.Fatal(err)
	} else if err := sharedSecret.UnmarshalText([]byte(sharedSecretStr)); err != nil {
		t.Fatal(err)
	}

	appKey, err := deriveAppKey(mnemonic, appID, sharedSecret)
	if err != nil {
		t.Fatal(err)
	}
	if appKey.String() != expectedAppKey {
		t.Fatalf("derived app key mismatch: expected %q, got %q", expectedAppKey, appKey)
	}
}

func TestEncryptRoundtrip(t *testing.T) {
	var data [4096]byte
	frand.Read(data[:])

	var key [32]byte
	frand.Read(key[:])

	for _, offset := range []uint64{0, 16, 31, 63, 64, 96, 128, 2048, 4096, maxBytesPerNonce - 127, maxBytesPerNonce - 128, maxBytesPerNonce - 63, maxBytesPerNonce - 64, maxBytesPerNonce, 2 * maxBytesPerNonce} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			r := encrypt(&key, bytes.NewReader(data[:]), offset)

			read, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}

			var buf bytes.Buffer
			decrypted := decrypt(&key, &buf, offset)
			if _, err := decrypted.Write(read); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data[:], buf.Bytes()) {
				t.Fatalf("data mismatch: expected %v, got %v", data[:], buf.Bytes())
			}
		})
	}
}
