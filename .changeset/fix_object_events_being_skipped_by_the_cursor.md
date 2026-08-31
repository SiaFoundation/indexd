---
default: patch
---

# Fix object events being skipped by the cursor

An event now takes its position in the stream from a background publisher rather
than from the transaction that wrote it, so a slow commit can no longer land
behind a cursor that has already moved on. Clients need no update, though an
event becomes visible up to two seconds after the write instead of within the
same second.
