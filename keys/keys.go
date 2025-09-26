package keys

import (
	"io"

	"go.sia.tech/core/blake2b"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/hkdf"
)

// DerivePrivateKey derives a key from the given private key and purpose using HKDF.
func DerivePrivateKey(key types.PrivateKey, purpose string) types.PrivateKey {
	buf := Derive(key[:], purpose, 32)
	defer clear(buf)
	return types.NewPrivateKeyFromSeed(buf)
}

// Derive derives a key from the given key and purpose using HKDF.
// It will return exactly n bytes.
func Derive(key []byte, purpose string, n int) []byte {
	buf := make([]byte, n)
	hkdf := hkdf.New(blake2b.New256, key, []byte(purpose), nil)
	if _, err := io.ReadFull(hkdf, buf); err != nil {
		panic(err) // never happens
	}
	return buf
}
