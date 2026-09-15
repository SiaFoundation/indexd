---
default: minor
---

# Support CBOR for slab and object listing responses

`GET /slabs/:slabid`, `GET /objects` and `GET /objects/:key/slabs` now return
CBOR when the request's `Accept` header names `application/cbor`, and JSON
otherwise. The Sia binary encoding previously served for
`Accept: application/octet-stream` has been replaced, so `slabs.PinnedSlab` no
longer implements `types.EncoderTo` and `types.DecoderFrom`.
