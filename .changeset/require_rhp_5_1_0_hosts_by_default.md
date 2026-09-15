---
default: patch
---

# Require RHP 5.1.0 hosts by default

A new deployment initializes `MinProtocolVersion` at 5.1.0 rather than 5.0.2, so
a host needs balance pool support to pass the default usability check. Existing
deployments keep the minimum already stored in their settings.
