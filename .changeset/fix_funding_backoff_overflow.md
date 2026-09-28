---
default: patch
---

# Fix funding backoff overflow

Fixed the account and pool funding backoff overflowing after ~28 consecutive failures, which caused funding to spin on a single failing host and stall funding for all other hosts.
