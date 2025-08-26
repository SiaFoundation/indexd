package sdk

import (
	"crypto/cipher"
	"encoding/binary"
	"io"
	"math"

	"golang.org/x/crypto/chacha20"
)

type rekeyStream struct {
	key  []byte
	c    *chacha20.Cipher
	skip int

	counter uint64
	nonce   uint64
}

const (
	// maximum amount of data we can encrypt with a single nonce because
	// counter is a uint32 and each tick is 64 bytes
	maxBytesPerNonce = 64 * math.MaxUint32
)

func (rs *rekeyStream) XORKeyStream(dst, src []byte) {
	if len(src) == 0 {
		return
	}

	if rs.skip > 0 {
		// determine how many bytes we can process from the first block.
		n := min(64-rs.skip, len(src))

		// generate the full 64-byte keystream for the initial block.
		var keyStream [64]byte
		rs.c.XORKeyStream(keyStream[:], keyStream[:])

		// XOR the relevant part of the keystream with the source data.
		for i := 0; i < n; i++ {
			dst[i] = src[i] ^ keyStream[rs.skip+i]
		}

		// update state and slice pointers for the rest of the operation
		rs.counter += uint64(n)
		src = src[n:]
		dst = dst[n:]
		// only run once
		rs.skip = 0
	}
	if len(src) == 0 {
		return
	}

	rs.counter += uint64(len(src))
	if rs.counter < maxBytesPerNonce {
		rs.c.XORKeyStream(dst, src)
		return
	}

	// counter overflow; xor remaining bytes, then increment nonce and xor again
	rem := maxBytesPerNonce - (rs.counter - uint64(len(src)))
	rs.c.XORKeyStream(dst[:rem], src[:rem])
	src = src[rem:]
	dst = dst[rem:]

	// reset counter and re-key the cipher with an incremented nonce
	rs.counter = uint64(len(src))
	rs.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], rs.nonce)
	rs.c, _ = chacha20.NewUnauthenticatedCipher(rs.key, nonce)

	rs.c.XORKeyStream(dst, src)
}

func nonce(offset uint64) (nonce [24]byte, nonce64 uint64) {
	nonce64 = offset / maxBytesPerNonce
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	return
}

// encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func encrypt(key *[32]byte, r io.Reader, offset uint64) cipher.StreamReader {
	n, n64 := nonce(offset)
	offset %= maxBytesPerNonce
	skip := int(offset % 64)

	c, _ := chacha20.NewUnauthenticatedCipher(key[:], n[:])
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: key[:], c: c, counter: offset, nonce: n64, skip: skip}
	return cipher.StreamReader{S: rs, R: r}
}

// decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func decrypt(key *[32]byte, w io.Writer, offset uint64) cipher.StreamWriter {
	n, n64 := nonce(offset)
	offset %= maxBytesPerNonce
	skip := int(offset % 64)

	c, _ := chacha20.NewUnauthenticatedCipher(key[:], n[:])
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: key[:], c: c, counter: offset, nonce: n64, skip: skip}
	return cipher.StreamWriter{S: rs, W: w}
}
