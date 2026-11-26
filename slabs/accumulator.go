package slabs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/bits"

	"go.sia.tech/core/blake2b"
	"go.sia.tech/core/types"
)

// An ObjectAccumulator incrementally computes a V2 ObjectID from slab slices.
type ObjectAccumulator struct {
	Trees [32]types.Hash256 // 4,294,967,295 slabs
	Slabs uint64
}

func (acc *ObjectAccumulator) hasTreeAtHeight(height int) bool {
	return acc.Slabs&(1<<height) != 0
}

func (acc *ObjectAccumulator) subtreeRoot(slab ObjectSlab, height int) types.Hash256 {
	var buf [41]byte
	copy(buf[1:33], slab.ID[:])
	binary.LittleEndian.PutUint64(buf[33:], uint64(slab.Offset)<<32|uint64(slab.Length))
	h := blake2b.Sum256(buf[:])
	for i := range height {
		h = blake2b.SumPair(acc.Trees[i], h)
	}
	return h
}

// encodingMask selects the trees needed for the current root and one pop.
func (acc *ObjectAccumulator) encodingMask() uint64 {
	mask := acc.Slabs
	if mask > 0 {
		mask |= mask - 1
	}
	return mask
}

// AddSlab appends a new slab to the accumulator
func (acc *ObjectAccumulator) AddSlab(slab ObjectSlab) {
	height := bits.TrailingZeros64(^acc.Slabs)
	acc.Trees[height] = acc.subtreeRoot(slab, height)
	acc.Slabs++
}

// ReplaceSlab replaces the most recently added slab slice.
//
// Returns an error if existing does not match.
func (acc *ObjectAccumulator) ReplaceSlab(existing, replacement ObjectSlab) error {
	if acc.Slabs == 0 {
		return errors.New("cannot replace slab in empty accumulator")
	}
	height := bits.TrailingZeros64(acc.Slabs)
	if acc.subtreeRoot(existing, height) != acc.Trees[height] {
		return errors.New("existing slab does not match last slab")
	} else if existing != replacement {
		acc.Trees[height] = acc.subtreeRoot(replacement, height)
	}
	return nil
}

// ID returns the V2 object ID of the accumulated slab slices.
func (acc *ObjectAccumulator) ID() ObjectID {
	var root [32]byte
	if i := bits.TrailingZeros64(acc.Slabs); i != 64 {
		root = acc.Trees[i]
		for i++; i < len(acc.Trees); i++ {
			if acc.hasTreeAtHeight(i) {
				root = blake2b.SumPair(acc.Trees[i], root)
			}
		}
	}
	h := types.NewHasher()
	h.WriteDistinguisher("indexd/object/v2")
	h.E.WriteUint64(acc.Slabs)
	h.E.Write(root[:])
	return ObjectID(h.Sum())
}

// EncodeTo implements types.EncoderTo.
func (acc ObjectAccumulator) EncodeTo(e *types.Encoder) {
	e.WriteUint64(acc.Slabs)
	mask := acc.encodingMask()
	for i, root := range acc.Trees {
		if mask&(1<<i) != 0 {
			types.Hash256(root).EncodeTo(e)
		}
	}
}

// DecodeFrom implements types.DecoderFrom.
func (acc *ObjectAccumulator) DecodeFrom(d *types.Decoder) {
	var decoded ObjectAccumulator
	decoded.Slabs = d.ReadUint64()
	if bits.Len64(decoded.Slabs) > len(decoded.Trees) {
		d.SetErr(errors.New("invalid accumulator leaf count"))
		return
	}
	mask := decoded.encodingMask()
	for i := range decoded.Trees {
		if mask&(1<<i) != 0 {
			(*types.Hash256)(&decoded.Trees[i]).DecodeFrom(d)
		}
	}
	if d.Err() == nil {
		*acc = decoded
	}
}

// MarshalJSON implements json.Marshaler.
func (acc ObjectAccumulator) MarshalJSON() ([]byte, error) {
	v := struct {
		Slabs uint64          `json:"slabs"`
		Trees []types.Hash256 `json:"trees"`
	}{acc.Slabs, []types.Hash256{}}
	mask := acc.encodingMask()
	for i, root := range acc.Trees {
		if mask&(1<<i) != 0 {
			v.Trees = append(v.Trees, types.Hash256(root))
		}
	}
	return json.Marshal(v)
}

// UnmarshalJSON implements json.Unmarshaler.
func (acc *ObjectAccumulator) UnmarshalJSON(b []byte) error {
	var v struct {
		Slabs uint64          `json:"slabs"`
		Trees []types.Hash256 `json:"trees"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	decoded := ObjectAccumulator{Slabs: v.Slabs}
	if bits.Len64(decoded.Slabs) > len(decoded.Trees) || len(v.Trees) != bits.OnesCount64(decoded.encodingMask()) {
		return errors.New("invalid accumulator encoding")
	}
	mask := decoded.encodingMask()
	for i := range decoded.Trees {
		if mask&(1<<i) != 0 {
			decoded.Trees[i] = v.Trees[0]
			v.Trees = v.Trees[1:]
		}
	}
	*acc = decoded
	return nil
}
