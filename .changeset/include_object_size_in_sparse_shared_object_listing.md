---
default: minor
---

# Include each object's size in the sparse shared object listing

`GET /shared/objects?includeslabs=false` and `GET /sharing/:key/objects?includeslabs=false` now return a `size` field with the object's length in bytes, so a client can show file sizes without fetching every object's slabs.
