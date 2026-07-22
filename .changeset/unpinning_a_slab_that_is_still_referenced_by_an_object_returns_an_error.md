---
default: minor
---

# Unpinning a slab that is still referenced by an object returns an error

`DELETE /slabs/:slabid` now fails with `409 Conflict` if one of the account's
objects still references the slab. Previously the slab was unpinned anyway,
freeing up the account's quota while the object kept the slab alive.
