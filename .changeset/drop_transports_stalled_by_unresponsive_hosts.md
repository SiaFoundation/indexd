---
default: patch
---

# Drop transports stalled by unresponsive hosts

An RPC whose deadline expires while it is in flight now stops its connection
from being handed out to new RPCs and closes it after a grace period. Previously
the connection kept being reused until the kernel abandoned the socket.
