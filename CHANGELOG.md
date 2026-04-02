
## 0.1.1 (2026-04-02)

### Features

- Update Go to 1.26.0.

### Fixes

- Add debug routes for deleting a slab's objects and pruning orphaned slabs.
- Expose Account method on the SDK
- Fetch all hosts in updateHosts rather than the first 100.
- Fix subscriber sync loop exiting early after processing only the first batch
- Improve integrity check throughput by replacing batch-and-wait with a worker pool and adding a per-host timeout to prevent slow hosts from blocking progress.
- Lower host scanning interval to 5 minutes.
- Update coreutils to v0.21.1
