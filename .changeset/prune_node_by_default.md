---
default: minor
---

# Prune node by default

The consensus prune target now defaults to a week of blocks (1008) when it is
not set in the config file. Setting `consensus.pruneTarget` to 0 explicitly
disables pruning.
