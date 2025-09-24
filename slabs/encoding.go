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
func (lo LockedObject) EncodeTo(e *types.Encoder) {
	lo.ID.EncodeTo(e)
	e.WriteBytes(lo.EncryptedMasterKey)
	types.EncodeSlice(e, lo.Slabs)
	e.WriteBytes(lo.EncryptedMetadata)
	e.WriteTime(lo.CreatedAt)
	e.WriteTime(lo.UpdatedAt)
}

// DecodeFrom implements types.DecoderFrom.
func (lo *LockedObject) DecodeFrom(d *types.Decoder) {
	lo.ID.DecodeFrom(d)
	lo.EncryptedMasterKey = d.ReadBytes()
	types.DecodeSlice(d, &lo.Slabs)
	lo.EncryptedMetadata = d.ReadBytes()
	lo.CreatedAt = d.ReadTime()
	lo.UpdatedAt = d.ReadTime()
}

// MarshalSia is a convenience method to encode the object metadata into bytes
// using the Sia encoding. This is equivalent to:
// var buf bytes.Buffer
// e := types.NewEncoder(&buf)
// obj.EncodeTo(e)
// e.Flush()
// buf now contains encoded Object
func (lo *LockedObject) MarshalSia() ([]byte, error) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	lo.EncodeTo(e)
	e.Flush()
	return buf.Bytes(), nil
}

// UnmarshalSia is a convenience method to decode the Sia-encoded bytes into an
// object metadata type. This is equivalent to:
// d := types.NewBufDecoder(bv)
// obj.DecodeFrom(d)
// return d.Err()
func (lo *LockedObject) UnmarshalSia(b []byte) error {
	d := types.NewBufDecoder(b)
	lo.DecodeFrom(d)
	return d.Err()
}
