---
default: patch
---

# Speed up contract pruning

Pruning now loads a snapshot of each contract's roots before pruning it and only looks up roots missing from the snapshot, using indices scoped to the contract instead of the global sector root index. The snapshot doesn't count towards the per-contract prune timeout and at most 10 hosts are pruned in parallel to reduce disk I/O.
