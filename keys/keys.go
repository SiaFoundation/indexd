package keys

import (
	"io"

	"go.sia.tech/core/blake2b"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/hkdf"
)

// DerivePrivateKey derives a new ed25519 private key from the given private key and purpose using HKDF.
func DerivePrivateKey(key types.PrivateKey, purpose string) types.PrivateKey {
	buf := Derive(key[:], []byte(purpose), nil, 32)
	defer clear(buf)
	return types.NewPrivateKeyFromSeed(buf)
}

// Derive derives a generic key from the given key and purpose using HKDF.
// It will return exactly n bytes.
func Derive(key []byte, salt []byte, domain []byte, n int) []byte {
	buf := make([]byte, n)
	hkdf := hkdf.New(blake2b.New256, key, salt, domain)
	if _, err := io.ReadFull(hkdf, buf); err != nil {
		panic(err) // never happens
	}
	return buf
}
