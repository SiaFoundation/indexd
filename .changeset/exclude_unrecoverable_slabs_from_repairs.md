---
default: minor
---

# Exclude unrecoverable slabs from repairs

A shard that doesn't hash to its pinned root after being reconstructed from its
peers can never be migrated, so the slab it belongs to is now marked
unrecoverable with a reason and taken out of the repair rotation instead of
being retried forever. Such a mismatch no longer interrupts the migration
either: the slab's remaining shards finish migrating first.

Two metrics were added to track slab repair health:
`indexd_num_unrecoverable_slabs` and `indexd_num_stuck_slabs`, the latter
counting slabs that failed more than one consecutive repair attempt and are
still being retried. Both are maintained by a trigger and reported by
`GET /stats/sectors`.
