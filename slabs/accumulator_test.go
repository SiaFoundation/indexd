package slabs

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"go.sia.tech/core/blake2b"
	"go.sia.tech/core/types"
)

func encodeObjectAccumulator(t testing.TB, acc ObjectAccumulator) []byte {
	t.Helper()
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	acc.EncodeTo(e)
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func roundTripObjectAccumulator(t testing.TB, acc ObjectAccumulator, format string) ObjectAccumulator {
	t.Helper()
	mask := acc.Slabs
	if mask != 0 {
		mask |= mask - 1
	}
	// Decode into a reused receiver to ensure omitted tree slots are cleared.
	got := ObjectAccumulator{Slabs: math.MaxUint32}
	for i := range got.Trees {
		got.Trees[i][0] = 255
	}
	if format == "binary" {
		b := encodeObjectAccumulator(t, acc)
		if len(b) != 8+32*bits.OnesCount64(mask) {
			t.Fatalf("unexpected encoded size: %d", len(b))
		}
		d := types.NewBufDecoder(b)
		got.DecodeFrom(d)
		if err := d.Err(); err != nil {
			t.Fatal(err)
		}
	} else {
		b, err := json.Marshal(acc)
		if err != nil {
			t.Fatal(err)
		}
		var wire struct {
			Slabs uint64          `json:"slabs"`
			Trees []types.Hash256 `json:"trees"`
		}
		if err := json.Unmarshal(b, &wire); err != nil {
			t.Fatal(err)
		} else if wire.Slabs != acc.Slabs || len(wire.Trees) != bits.OnesCount64(mask) {
			t.Fatalf("unexpected JSON encoding: %s", b)
		}
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
	}
	if got.Slabs != acc.Slabs || got.ID() != acc.ID() {
		t.Fatal("round trip changed the accumulator")
	}
	for i, root := range got.Trees {
		if mask&(1<<i) != 0 {
			if root != acc.Trees[i] {
				t.Fatalf("lost tree at height %d", i)
			}
		} else if root != ([32]byte{}) {
			t.Fatalf("retained stale tree at height %d", i)
		}
	}
	return got
}

func TestObjectAccumulatorEncoding(t *testing.T) {
	for _, format := range []string{"binary", "json"} {
		t.Run(format, func(t *testing.T) {
			var acc ObjectAccumulator
			var reference blake2b.Accumulator
			roundTripObjectAccumulator(t, acc, format)
			for n := 1; n <= 257; n++ {
				beforeLast := reference
				slab := ObjectSlab{ID: SlabID(blake2b.Sum256(fmt.Append(nil, n))), Offset: uint32(n * 7), Length: uint32(n * 11)}
				acc.AddSlab(slab)
				reference.AddLeaf(referenceSlabLeaf(slab))
				t.Run(fmt.Sprint(n), func(t *testing.T) {
					got := roundTripObjectAccumulator(t, acc, format)
					if got.ID() != referenceObjectID(reference) {
						t.Fatal("root differs from core accumulator")
					}
					existing := slab
					for replacement := range 3 {
						slab := ObjectSlab{ID: SlabID(blake2b.Sum256(fmt.Appendf(nil, "replacement %d", replacement))), Offset: uint32(replacement * 3), Length: uint32(replacement*17 + 1)}
						if err := got.ReplaceSlab(existing, slab); err != nil {
							t.Fatal(err)
						}
						want := beforeLast
						want.AddLeaf(referenceSlabLeaf(slab))
						if got.Slabs != uint64(n) || got.ID() != referenceObjectID(want) {
							t.Fatal("replacement differs from rebuilt accumulator")
						}
						before := got
						if err := got.ReplaceSlab(existing, slab); err == nil || got != before {
							t.Fatal("stale existing slab was accepted or changed the accumulator")
						}
						existing = slab
						got = roundTripObjectAccumulator(t, got, format)
					}
					// Appending still works after replacement and serialization.
					want := beforeLast
					want.AddLeaf(referenceSlabLeaf(ObjectSlab{ID: SlabID(blake2b.Sum256([]byte("replacement 2"))), Offset: 6, Length: 35}))
					slab := ObjectSlab{ID: SlabID(blake2b.Sum256([]byte("appended"))), Offset: 10, Length: 23}
					got.AddSlab(slab)
					want.AddLeaf(referenceSlabLeaf(slab))
					if got.Slabs != want.NumLeaves || got.ID() != referenceObjectID(want) {
						t.Fatal("append after replacement differs from core")
					}
				})
			}
		})
	}
}

func TestObjectAccumulatorDecodeInvalid(t *testing.T) {
	var original ObjectAccumulator
	original.AddSlab(ObjectSlab{ID: SlabID{1}, Offset: 7, Length: 11})
	for _, n := range []uint64{1 << 32, math.MaxUint64} {
		var buf bytes.Buffer
		e := types.NewEncoder(&buf)
		e.WriteUint64(n)
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		got := original
		d := types.NewBufDecoder(buf.Bytes())
		got.DecodeFrom(d)
		if d.Err() == nil || got != original {
			t.Fatal("invalid leaf count was accepted or changed the receiver")
		}
	}
	var acc ObjectAccumulator
	for i := range 8 {
		acc.AddSlab(ObjectSlab{ID: SlabID{byte(i + 1)}, Offset: uint32(i * 7), Length: uint32(i * 11)})
	}
	encoded := encodeObjectAccumulator(t, acc)
	for length := range len(encoded) {
		got := original
		d := types.NewBufDecoder(encoded[:length])
		got.DecodeFrom(d)
		if d.Err() == nil || got != original {
			t.Fatalf("truncated encoding of length %d was accepted or changed the receiver", length)
		}
	}
	for _, b := range []string{
		`{"slabs":4294967296,"trees":[]}`,
		`{"slabs":18446744073709551615,"trees":[]}`,
		`{"slabs":1,"trees":[]}`,
		`{"slabs":0,"trees":["0000000000000000000000000000000000000000000000000000000000000000"]}`,
		`{"slabs":8,"trees":["0000000000000000000000000000000000000000000000000000000000000000"]}`,
		`{"slabs":1,"trees":["bad hash"]}`,
		`{"slabs":`,
	} {
		got := original
		if err := json.Unmarshal([]byte(b), &got); err == nil || got != original {
			t.Fatalf("invalid JSON was accepted or changed the receiver: %s", b)
		}
	}
}

func TestReplaceSlabEmpty(t *testing.T) {
	var acc ObjectAccumulator
	if err := acc.ReplaceSlab(ObjectSlab{}, ObjectSlab{ID: SlabID{1}}); err == nil {
		t.Fatal("expected replacing an empty accumulator to fail")
	} else if acc != (ObjectAccumulator{}) {
		t.Fatal("replacing an empty accumulator changed its state")
	}
}

func TestReplaceSlabVerification(t *testing.T) {
	var acc ObjectAccumulator
	replacement := ObjectSlab{ID: SlabID{255}, Offset: 17, Length: 23}
	for n := 1; n <= 257; n++ {
		existing := ObjectSlab{ID: SlabID(blake2b.Sum256(fmt.Append(nil, n))), Offset: uint32(n * 7), Length: uint32(n * 11)}
		acc.AddSlab(existing)
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			for _, field := range []string{"id", "offset", "length"} {
				t.Run(field, func(t *testing.T) {
					wrong := existing
					switch field {
					case "id":
						wrong.ID[0] ^= 1
					case "offset":
						wrong.Offset++
					case "length":
						wrong.Length++
					}
					got := acc
					if err := got.ReplaceSlab(wrong, replacement); err == nil || got != acc {
						t.Fatal("incorrect existing slab was accepted or changed the accumulator")
					}
				})
			}
			got := acc
			if err := got.ReplaceSlab(existing, existing); err != nil {
				t.Fatal(err)
			} else if got != acc {
				t.Fatal("replacing a slab with itself changed the accumulator")
			}
			for height := 0; height < bits.TrailingZeros64(acc.Slabs); height++ {
				got := acc
				got.Trees[height][0] ^= 1
				if got.ID() != acc.ID() {
					t.Fatal("inactive tree affected the current object ID")
				}
				before := got
				if err := got.ReplaceSlab(existing, replacement); err == nil || got != before {
					t.Fatalf("corrupted retained tree at height %d was accepted or changed the accumulator", height)
				}
			}
		})
	}
}

func TestReplaceSlabLargeCount(t *testing.T) {
	existing := ObjectSlab{ID: SlabID{1}, Offset: 7, Length: 11}
	replacement := ObjectSlab{ID: SlabID{2}, Offset: 17, Length: 23}
	for _, n := range []uint64{1<<31 - 1, 1 << 31, 1<<31 + 1, math.MaxUint32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			acc := ObjectAccumulator{Slabs: n - 1}
			reference := blake2b.Accumulator{NumLeaves: n - 1}
			for i := range acc.Trees {
				acc.Trees[i] = blake2b.Sum256(fmt.Append(nil, i))
				reference.Trees[i] = acc.Trees[i]
			}
			acc.AddSlab(existing)
			reference.AddLeaf(referenceSlabLeaf(replacement))
			for _, format := range []string{"binary", "json"} {
				got := roundTripObjectAccumulator(t, acc, format)
				if err := got.ReplaceSlab(existing, replacement); err != nil {
					t.Fatal(err)
				} else if got.Slabs != n || got.ID() != referenceObjectID(reference) {
					t.Fatal("replacement differs from core accumulator")
				}
			}
		})
	}
}

func referenceSlabLeaf(slab ObjectSlab) [32]byte {
	var leaf [41]byte
	copy(leaf[1:], slab.ID[:])
	binary.LittleEndian.PutUint64(leaf[33:], uint64(slab.Offset)<<32|uint64(slab.Length))
	return blake2b.Sum256(leaf[:])
}

func referenceObjectID(acc blake2b.Accumulator) ObjectID {
	encoded := []byte("sia/indexd/object/v2|")
	encoded = binary.LittleEndian.AppendUint64(encoded, acc.NumLeaves)
	root := acc.Root()
	return ObjectID(blake2b.Sum256(append(encoded, root[:]...)))
}
