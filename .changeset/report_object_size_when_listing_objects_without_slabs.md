---
default: minor
---

# Report object size when listing objects without slabs

Object events and objects listed without their slabs (`includeslabs=false` on
`GET /objects`, `GET /sharing/:key/objects`, and `GET /shared/objects`) now
include the object's logical `size` in bytes, so callers no longer need to page
through every slab slice to learn how large an object is. The size is stored
when the object is pinned and existing objects are backfilled by a database
migration.

`SealedObjectWithoutSlabs` gains a `Size` field, and `SealedObject` and
`PinObjectRequest` gain a `Size()` method that sums their slab slice lengths.
