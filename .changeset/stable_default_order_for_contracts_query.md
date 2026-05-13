---
default: patch
---

# Default to a stable order in the contracts query

Paginated callers of `Store.Contracts` that didn't pass a sort option were issuing `LIMIT/OFFSET` without an `ORDER BY`, which is non-deterministic in PostgreSQL and could cause rows to be silently skipped or duplicated between batches. The contracts query now defaults to ordering by `c.contract_id ASC` when no sort is specified.
