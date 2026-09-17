---
default: patch
---

# Mark a slab as unrecoverable when recovering it for migration has failed 10 times in a row

Slabs now leave the repair rotation after ten consecutive shard-recovery failures. A successful recovery resets the failure count, even if uploading the recovered shards fails.
