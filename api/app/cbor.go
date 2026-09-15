package app

import (
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

// maxCBORResponseSize bounds how much of a CBOR response the client buffers
// before decoding it.
const maxCBORResponseSize = 256 << 20

// cborEncMode is the CBOR encoding shared by the handlers and the client. Field
// names come from the json struct tags, fixed size byte arrays encode as byte
// strings and timestamps as RFC 3339 strings in UTC.
var cborEncMode = func() cbor.EncMode {
	em, err := cbor.EncOptions{Time: cbor.TimeRFC3339NanoUTC}.EncMode()
	if err != nil {
		panic(err) // never happens
	}
	return em
}()

// encodeCBOR encodes v as CBOR.
func encodeCBOR(v any) ([]byte, error) {
	return cborEncMode.Marshal(v)
}

// decodeCBOR decodes a single CBOR value from r into v. The value is buffered
// rather than streamed because the streaming decoder rescans it from the head
// after every read, which is quadratic in the response size.
func decodeCBOR(r io.Reader, v any) error {
	buf, err := io.ReadAll(io.LimitReader(r, maxCBORResponseSize+1))
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	} else if len(buf) > maxCBORResponseSize {
		return fmt.Errorf("response exceeds the %d byte limit", maxCBORResponseSize)
	}
	return cbor.Unmarshal(buf, v)
}
