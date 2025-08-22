package sdk

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"lukechampine.com/frand"
)

func TestEncryptRoundtrip(t *testing.T) {
	var data [4096]byte
	frand.Read(data[:])

	var key [32]byte
	frand.Read(key[:])

	for _, offset := range []uint64{0, 64, 128, 2048, 4096, maxBytesPerNonce - 128, maxBytesPerNonce - 64, maxBytesPerNonce, 2 * maxBytesPerNonce} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			r, err := encrypt(&key, bytes.NewReader(data[:]), offset)
			if err != nil {
				t.Fatal(err)
			}

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
