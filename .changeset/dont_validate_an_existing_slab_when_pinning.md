---
default: patch
---

# Only count sectors a pin binds towards the bad hosts limit

Pinning a slab no longer fails with `ErrBadHosts` when every sector is already
bound to a host. A pin that binds no sectors records no new placement, so a
slab can be pinned by any account after its hosts go bad. Binding a sector to
a bad host is still rejected.
