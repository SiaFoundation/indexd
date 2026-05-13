---
default: minor
---

# Make slab migration concurrency configurable

Added a new `slabs.migrationWorkers` config field that controls the number of slabs migrated in parallel by the slab manager.
