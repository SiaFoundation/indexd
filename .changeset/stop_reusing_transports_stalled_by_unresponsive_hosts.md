---
default: patch
---

# Stop reusing transports stalled by unresponsive hosts

When an RPC reaches a transport and then runs out its deadline, the client now
removes that exact transport from its cache. Existing streams receive a grace
period to finish, while the next RPC to the host establishes a fresh
connection. Host demotion remains with callers, which know whether an expired
deadline represents host failure.
