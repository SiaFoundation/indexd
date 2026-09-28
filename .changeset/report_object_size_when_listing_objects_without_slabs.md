---
default: minor
---

# Report object size when listing objects without slabs

`GET /objects?includeslabs=false` now includes each object's `size` in bytes, so
callers no longer need to fetch every slab slice to learn how large an object is.
