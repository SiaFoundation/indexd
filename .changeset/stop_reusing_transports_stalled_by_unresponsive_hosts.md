---
default: patch
---

# Stop reusing transports stalled by unresponsive hosts

When an RPC reaches a transport and then runs out its deadline, the client now
removes that exact transport from its cache. Existing RPCs keep it open until
they finish, while the next RPC to the host establishes a fresh connection. The
old transport closes when its final RPC releases it. Host demotion remains with
callers, which know whether an expired deadline represents host failure.
