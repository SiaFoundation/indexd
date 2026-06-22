---
default: minor
---

# Add network read and write throughput estimates to the client.

The client now exposes `ReadEstimate` and `WriteEstimate`, returning the expected duration to transfer a given number of bytes from the network-wide observed throughput. Both fall back to a default rate before any bulk transfers have been sampled.
