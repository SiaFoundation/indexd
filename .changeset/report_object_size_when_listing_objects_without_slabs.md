---
default: minor
---

# Report object size when listing objects without slabs

Object events from `GET /objects?includeslabs=false` and objects attached to a
sharing key from `GET /sharing/{key}/objects?includeslabs=false` and
`GET /shared/objects?includeslabs=false` now include each object's logical
`size` in bytes, so callers no longer need to page through every slab slice to
learn how large an object is. The size is stored on the object when it is
pinned, and existing objects are backfilled by a database migration.
