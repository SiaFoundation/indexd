package slabs

import (
	"go.sia.tech/core/types"
)

// EncodeTo implements types.EncoderTo.
func (x SlabID) EncodeTo(e *types.Encoder) {
	e.Write(x[:])
}

// DecodeFrom implements types.DecoderFrom.
func (x *SlabID) DecodeFrom(d *types.Decoder) {
	d.Read(x[:])
}

// EncodeTo implements types.EncoderTo.
func (x PinnedSector) EncodeTo(e *types.Encoder) {
	x.Root.EncodeTo(e)
	x.HostKey.EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom.
func (x *PinnedSector) DecodeFrom(d *types.Decoder) {
	x.Root.DecodeFrom(d)
	x.HostKey.DecodeFrom(d)
}

// EncodeTo implements types.EncoderTo.
func (x PinnedSlab) EncodeTo(e *types.Encoder) {
	x.ID.EncodeTo(e)
	e.Write(x.EncryptionKey[:])
	e.WriteUint64(uint64(x.MinShards))
	types.EncodeSlice(e, x.Sectors)
}

// DecodeFrom implements types.DecoderFrom.
func (x *PinnedSlab) DecodeFrom(d *types.Decoder) {
	x.ID.DecodeFrom(d)
	d.Read(x.EncryptionKey[:])
	x.MinShards = uint(d.ReadUint64())
	types.DecodeSlice(d, &x.Sectors)
}
