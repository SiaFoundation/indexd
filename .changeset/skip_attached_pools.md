---
default: patch
---

# Skip fully attached pools when looking for pending pool attachments

Funding no longer rescans every account of every pool on every host each cycle, only pools that gained accounts since they were last fully attached.
