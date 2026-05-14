## 0.2.2 (2026-05-13)

### Features

#### Make slab migration concurrency configurable

Added a new `slabs.migrationWorkers` config field that controls the number of slabs migrated in parallel by the slab manager.

### Fixes

#### Default to a stable order in the contracts query

Paginated callers of `Store.Contracts` that didn't pass a sort option were issuing `LIMIT/OFFSET` without an `ORDER BY`, which is non-deterministic in PostgreSQL and could cause rows to be silently skipped or duplicated between batches. The contracts query now defaults to ordering by `c.contract_id ASC` when no sort is specified.

## 0.2.1 (2026-05-07)

### Fixes

- Don't reset transport for invalid proof errors.
- Reduce funding interval to 5m
- Truncate object event timestamps in database to seconds

## 0.2.0 (2026-04-27)

### Breaking Changes

- The SDK has been moved to its own package `go.sia.tech/siastorage`

### Features

- Add a Prometheus metrics endpoint to the admin API.
- Add warmup connections to the client.
- Download geoip database on demand rather than embedding it.

### Fixes

- Adjust max fund limit to exclude uploads when account remaining storage is 0
- Call managers from admin API instead of the store
- Don't consider hosts on "bad" QUIC ports usable
- Fix a bug where contracts weren't renewed due to invalid signatures.
- Fixed auth check succeeding for soft deleted accounts.
- Increment default MinProtocolVersion to 5.0.2.
- Only consider a host good when scanning if they are reachable on both Siamux and Quic
- Reduce account fund interval to 15 minutes. This reduces the initial fund for new accounts and reduces the amount of time a high-usage account has to wait to be refilled.
- Update lastUsed field for Accounts every time an account authenticates with the indexer.
- Use deltas for stats to reduce contention
