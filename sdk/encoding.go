package sdk

import (
	"bytes"

	"go.sia.tech/core/types"
)

// EncodeTo implements types.EncoderTo.
func (s Slab) EncodeTo(e *types.Encoder) {
	e.Write(s.ID[:])
	e.WriteUint64(uint64(s.Offset)<<32 | uint64(s.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (s *Slab) DecodeFrom(d *types.Decoder) {
	d.Read(s.ID[:])

	combined := d.ReadUint64()
	s.Offset = uint32(combined >> 32)
	s.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (obj Object) EncodeTo(e *types.Encoder) {
	e.WriteBool(obj.Key != nil)
	if obj.Key != nil {
		e.Write((*obj.Key)[:])
	}
	types.EncodeSlice(e, obj.Slabs)
}

// DecodeFrom implements types.DecoderFrom.
func (obj *Object) DecodeFrom(d *types.Decoder) {
	if d.ReadBool() {
		obj.Key = new([32]byte)
		d.Read((*obj.Key)[:])
	}
	types.DecodeSlice(d, &obj.Slabs)
}

// MarshalSia is a convenience method to encode the object metadata into bytes
// using the Sia encoding. This is equivalent to:
// var buf bytes.Buffer
// e := types.NewEncoder(&buf)
// obj.EncodeTo(e)
// e.Flush()
// buf now contains encoded Object
func (obj *Object) MarshalSia() ([]byte, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	obj.EncodeTo(e)
	e.Flush()
	return buf.Bytes(), nil
}

// UnmarshalSia is a convenience method to decode the Sia-encoded bytes into an
// object metadata type. This is equivalent to:
// d := types.NewBufDecoder(bv)
// obj.DecodeFrom(d)
// return d.Err()
func (obj *Object) UnmarshalSia(b []byte) error {
	d := types.NewBufDecoder(b)
	obj.DecodeFrom(d)
	return d.Err()
}
