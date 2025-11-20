package slabs

import (
	"bytes"

	"go.sia.tech/core/types"
)

// EncodeTo implements types.EncoderTo.
func (s SlabID) EncodeTo(e *types.Encoder) {
	e.Write(s[:])
}

// DecodeFrom implements types.DecoderFrom.
func (s *SlabID) DecodeFrom(d *types.Decoder) {
	d.Read(s[:])
}

// EncodeTo implements types.EncoderTo.
func (ps PinnedSector) EncodeTo(e *types.Encoder) {
	ps.Root.EncodeTo(e)
	ps.HostKey.EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom.
func (ps *PinnedSector) DecodeFrom(d *types.Decoder) {
	ps.Root.DecodeFrom(d)
	ps.HostKey.DecodeFrom(d)
}

// EncodeTo implements types.EncoderTo.
func (ps PinnedSlab) EncodeTo(e *types.Encoder) {
	ps.ID.EncodeTo(e)
	e.Write(ps.EncryptionKey[:])
	e.WriteUint64(uint64(ps.MinShards))
	types.EncodeSlice(e, ps.Sectors)
}

// DecodeFrom implements types.DecoderFrom.
func (ps *PinnedSlab) DecodeFrom(d *types.Decoder) {
	ps.ID.DecodeFrom(d)
	d.Read(ps.EncryptionKey[:])
	ps.MinShards = uint(d.ReadUint64())
	types.DecodeSlice(d, &ps.Sectors)
}

// EncodeTo implements types.EncoderTo.
func (s SlabSlice) EncodeTo(e *types.Encoder) {
	e.Write(s.SlabID[:])
	e.WriteUint64(uint64(s.Offset)<<32 | uint64(s.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (s *SlabSlice) DecodeFrom(d *types.Decoder) {
	d.Read(s.SlabID[:])

	combined := d.ReadUint64()
	s.Offset = uint32(combined >> 32)
	s.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (ps PinnedSlabSlice) EncodeTo(e *types.Encoder) {
	ps.PinnedSlab.EncodeTo(e)
	e.WriteUint64(uint64(ps.Offset)<<32 | uint64(ps.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (ps *PinnedSlabSlice) DecodeFrom(d *types.Decoder) {
	ps.PinnedSlab.DecodeFrom(d)

	combined := d.ReadUint64()
	ps.Offset = uint32(combined >> 32)
	ps.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (so SealedObject) EncodeTo(e *types.Encoder) {
	e.WriteBytes(so.EncryptedMasterKey)
	types.EncodeSlice(e, so.Slabs)
	e.WriteBytes(so.EncryptedMetadata)
	so.Signature.EncodeTo(e)
	e.WriteTime(so.CreatedAt)
	e.WriteTime(so.UpdatedAt)
}

// DecodeFrom implements types.DecoderFrom.
func (so *SealedObject) DecodeFrom(d *types.Decoder) {
	so.EncryptedMasterKey = d.ReadBytes()
	types.DecodeSlice(d, &so.Slabs)
	so.EncryptedMetadata = d.ReadBytes()
	so.Signature.DecodeFrom(d)
	so.CreatedAt = d.ReadTime()
	so.UpdatedAt = d.ReadTime()
}

// MarshalSia is a convenience method to encode the object metadata into bytes
// using the Sia encoding. This is equivalent to:
// var buf bytes.Buffer
// e := types.NewEncoder(&buf)
// obj.EncodeTo(e)
// e.Flush()
// buf now contains encoded Object
func (so *SealedObject) MarshalSia() ([]byte, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	so.EncodeTo(e)
	e.Flush()
	return buf.Bytes(), nil
}

// UnmarshalSia is a convenience method to decode the Sia-encoded bytes into an
// object metadata type. This is equivalent to:
// d := types.NewBufDecoder(bv)
// obj.DecodeFrom(d)
// return d.Err()
func (so *SealedObject) UnmarshalSia(b []byte) error {
	d := types.NewBufDecoder(b)
	so.DecodeFrom(d)
	return d.Err()
}
