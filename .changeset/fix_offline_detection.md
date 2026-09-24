---
default: patch
---

# Fix offline detection

Determine indexer connectivity by probing known external sites instead of relying
on syncer peers, which can remain connected while offline. Cache connectivity
results for 30 seconds to avoid excessive probes during host scans.
