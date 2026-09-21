---
default: major
---

# Negotiate CBOR responses on every app endpoint

Every application API endpoint that returns a body now returns CBOR when the
request's `Accept` header names `application/cbor`, and JSON otherwise. The
negotiated responses set `Vary: Accept`. The Sia binary encoding previously
served for `Accept: application/octet-stream` has been replaced, so
`slabs.PinnedSlab` no longer implements `types.EncoderTo` and
`types.DecoderFrom`.
