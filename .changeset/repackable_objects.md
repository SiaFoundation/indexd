---
default: minor
---

# Add GET /repackable/objects

Lists small objects stored in a single slab that is more than half empty so they can be repacked together. It follows the same cutoff as `POST /slabs/prune` and returns at most `limit` objects, or one slab of data at the `datashards` and `parityshards` erasure coding parameters passed in, whichever is smaller.
