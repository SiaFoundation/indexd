package app

import (
	"github.com/fxamacker/cbor/v2"
)

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
