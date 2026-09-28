---
default: minor
---

# Report the logical size of objects in object listings without slab slices

`GET /objects?includeslabs=false` now includes each object's logical `size` in
bytes, so callers no longer need to page through every slab slice to learn how
large an object is.
