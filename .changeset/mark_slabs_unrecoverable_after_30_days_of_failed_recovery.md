---
default: patch
---

# Mark slabs unrecoverable after 30 days of failed recovery

A slab is marked unrecoverable once its shards have failed to recover for 30
days while fewer than MinShards of its sectors are stored on a host. A
successful recovery resets the window, even if uploading the recovered shards
fails.
