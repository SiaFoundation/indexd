---
default: patch
---

# Fix object events being skipped by the cursor

Fixes a bug where a client could miss an object event. `GET /objects` now
withholds events until the transactions that could still write into them
settle. Clients need no update, but the database role has to be granted
`pg_read_all_stats`.
