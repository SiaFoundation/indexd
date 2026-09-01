package slabs

import (
	"bytes"
	"time"

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
	e.WriteUint8(ps.Version)
	e.Write(ps.EncryptionKey[:])
	e.WriteUint64(uint64(ps.MinShards))
	types.EncodeSlice(e, ps.Sectors)
}

// DecodeFrom implements types.DecoderFrom.
func (ps *PinnedSlab) DecodeFrom(d *types.Decoder) {
	ps.ID.DecodeFrom(d)
	ps.Version = d.ReadUint8()
	d.Read(ps.EncryptionKey[:])
	ps.MinShards = uint(d.ReadUint64())
	types.DecodeSlice(d, &ps.Sectors)
}

// EncodeTo implements types.EncoderTo.
func (ps PinnedSlabs) EncodeTo(e *types.Encoder) {
	types.EncodeSlice(e, ps)
}

// DecodeFrom implements types.DecoderFrom.
func (ps *PinnedSlabs) DecodeFrom(d *types.Decoder) {
	types.DecodeSlice(d, (*[]PinnedSlab)(ps))
}

// EncodeTo implements types.EncoderTo.
func (os ObjectSlab) EncodeTo(e *types.Encoder) {
	os.ID.EncodeTo(e)
	e.WriteUint64(uint64(os.Offset)<<32 | uint64(os.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (os *ObjectSlab) DecodeFrom(d *types.Decoder) {
	os.ID.DecodeFrom(d)
	combined := d.ReadUint64()
	os.Offset = uint32(combined >> 32)
	os.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (so SealedObjectReference) EncodeTo(e *types.Encoder) {
	e.WriteBytes(so.EncryptedDataKey)
	types.EncodeSlice(e, so.Slabs)
	so.DataSignature.EncodeTo(e)
	e.WriteBytes(so.EncryptedMetadataKey)
	e.WriteBytes(so.EncryptedMetadata)
	so.MetadataSignature.EncodeTo(e)
	encodeTime(e, so.CreatedAt)
	encodeTime(e, so.UpdatedAt)
}

// DecodeFrom implements types.DecoderFrom.
func (so *SealedObjectReference) DecodeFrom(d *types.Decoder) {
	so.EncryptedDataKey = d.ReadBytes()
	types.DecodeSlice(d, &so.Slabs)
	so.DataSignature.DecodeFrom(d)
	so.EncryptedMetadataKey = decodeOptionalBytes(d)
	so.EncryptedMetadata = decodeOptionalBytes(d)
	so.MetadataSignature.DecodeFrom(d)
	so.CreatedAt = decodeTime(d)
	so.UpdatedAt = decodeTime(d)
}

// EncodeTo implements types.EncoderTo.
func (oe ObjectEventReference) EncodeTo(e *types.Encoder) {
	oe.Key.EncodeTo(e)
	e.WriteBool(oe.Deleted)
	encodeTime(e, oe.UpdatedAt)
	types.EncodePtr(e, oe.Object)
}

// DecodeFrom implements types.DecoderFrom.
func (oe *ObjectEventReference) DecodeFrom(d *types.Decoder) {
	oe.Key.DecodeFrom(d)
	oe.Deleted = d.ReadBool()
	oe.UpdatedAt = decodeTime(d)
	types.DecodePtr(d, &oe.Object)
}

// EncodeTo implements types.EncoderTo.
func (oes ObjectEventReferences) EncodeTo(e *types.Encoder) {
	types.EncodeSlice(e, oes)
}

// DecodeFrom implements types.DecoderFrom.
func (oes *ObjectEventReferences) DecodeFrom(d *types.Decoder) {
	types.DecodeSlice(d, (*[]ObjectEventReference)(oes))
}

// encodeTime keeps the sub-second part that Encoder.WriteTime drops so decoded
// timestamps match their JSON encoding.
func encodeTime(e *types.Encoder, t time.Time) {
	e.WriteUint64(uint64(t.Unix()))
	e.WriteUint64(uint64(t.Nanosecond()))
}

func decodeTime(d *types.Decoder) time.Time {
	return time.Unix(int64(d.ReadUint64()), int64(d.ReadUint64())).UTC()
}

// decodeOptionalBytes returns nil for an empty field so it matches the omitted
// JSON form.
func decodeOptionalBytes(d *types.Decoder) []byte {
	if b := d.ReadBytes(); len(b) > 0 {
		return b
	}
	return nil
}

// EncodeTo implements types.EncoderTo.
func (s SlabSlice) EncodeTo(e *types.Encoder) {
	e.WriteUint8(s.Version)
	e.Write(s.EncryptionKey[:])
	e.WriteUint8(uint8(s.MinShards))
	types.EncodeSlice(e, s.Sectors)
	e.WriteUint64(uint64(s.Offset)<<32 | uint64(s.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (s *SlabSlice) DecodeFrom(d *types.Decoder) {
	s.Version = d.ReadUint8()
	d.Read(s.EncryptionKey[:])
	s.MinShards = uint(d.ReadUint8())
	types.DecodeSlice(d, &s.Sectors)
	combined := d.ReadUint64()
	s.Offset = uint32(combined >> 32)
	s.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (so SealedObject) EncodeTo(e *types.Encoder) {
	e.WriteBytes(so.EncryptedDataKey)
	types.EncodeSlice(e, so.Slabs)
	so.DataSignature.EncodeTo(e)
	e.WriteBytes(so.EncryptedMetadataKey)
	e.WriteBytes(so.EncryptedMetadata)
	so.MetadataSignature.EncodeTo(e)
	encodeTime(e, so.CreatedAt)
	encodeTime(e, so.UpdatedAt)
}

// DecodeFrom implements types.DecoderFrom.
func (so *SealedObject) DecodeFrom(d *types.Decoder) {
	so.EncryptedDataKey = d.ReadBytes()
	types.DecodeSlice(d, &so.Slabs)
	so.DataSignature.DecodeFrom(d)
	so.EncryptedMetadataKey = decodeOptionalBytes(d)
	so.EncryptedMetadata = decodeOptionalBytes(d)
	so.MetadataSignature.DecodeFrom(d)
	so.CreatedAt = decodeTime(d)
	so.UpdatedAt = decodeTime(d)
}

// EncodeTo implements types.EncoderTo.
func (oe ObjectEvent) EncodeTo(e *types.Encoder) {
	oe.Key.EncodeTo(e)
	e.WriteBool(oe.Deleted)
	encodeTime(e, oe.UpdatedAt)
	types.EncodePtr(e, oe.Object)
}

// DecodeFrom implements types.DecoderFrom.
func (oe *ObjectEvent) DecodeFrom(d *types.Decoder) {
	oe.Key.DecodeFrom(d)
	oe.Deleted = d.ReadBool()
	oe.UpdatedAt = decodeTime(d)
	types.DecodePtr(d, &oe.Object)
}

// EncodeTo implements types.EncoderTo.
func (oes ObjectEvents) EncodeTo(e *types.Encoder) {
	types.EncodeSlice(e, oes)
}

// DecodeFrom implements types.DecoderFrom.
func (oes *ObjectEvents) DecodeFrom(d *types.Decoder) {
	types.DecodeSlice(d, (*[]ObjectEvent)(oes))
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
