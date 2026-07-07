package testutils

import (
	"testing"

	"github.com/klauspost/reedsolomon"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

// NewTestShards creates an erasure-coded, encrypted set of shards along with
// the encryption key and the shards' sector roots.
func NewTestShards(t testing.TB, dataShards, parityShards int) ([32]byte, [][]byte, []types.Hash256) {
	t.Helper()

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		t.Fatal(err)
	}

	shards := make([][]byte, dataShards+parityShards)
	for i := range shards {
		if i < dataShards {
			shards[i] = frand.Bytes(proto.SectorSize)
		} else {
			shards[i] = make([]byte, proto.SectorSize)
		}
	}

	err = enc.Encode(shards)
	if err != nil {
		t.Fatalf("failed to encode shards: %v", err)
	}

	var encryptionKey [32]byte
	frand.Read(encryptionKey[:])
	nonce := make([]byte, 24)
	for i := range shards {
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])
	}

	var roots []types.Hash256
	for _, shard := range shards {
		roots = append(roots, proto.SectorRoot((*[proto.SectorSize]byte)(shard)))
	}

	return encryptionKey, shards, roots
}
