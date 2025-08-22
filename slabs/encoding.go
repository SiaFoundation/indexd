package slabs

import (
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
