## 0.4.4 (2026-08-21)

### Features

#### Add support for blocking objects

Adds an object blocklist for content moderation, managed through the new admin
endpoints `GET /objects/blocklist` and `GET`/`PUT`/`DELETE
/objects/blocklist/{objectkey}`. Blocked objects are filtered out in the
database, so they are hidden from `GET /objects`, `GET /sharing/{key}/objects`
and `GET /shared/objects` without disturbing pagination. Fetching one by key
returns `451 Unavailable For Legal Reasons`, as does pinning it or attaching it
to a sharing key — an object key is derived from its slabs, so a blocked key
covers that content for every account, including content that has not been
pinned yet.

Blocking never unpins data. Unblocking restores access and refreshes the
object's event timestamp so clients that already paged past it pick it up again.

### Fixes

- Batch object queries for better performance.
- Fix a bug where an approved request could be reused to register multiple apps.

## 0.4.3 (2026-08-17)

### Features

- Add manual admin host import and scan endpoint

### Fixes

- Improved performance of object events pagination queries.
- Rank hosts that stall until the deadline by the implied throughput
- Speed up pruning

## 0.4.2 (2026-08-06)

### Features

- Delete slabs of an object after deleting the object if the slabs become unreferenced

#### Added support for sharing keys

Sharing keys let an app grant read-only access to a specific set of objects without requiring the viewer to sign up or log in. Each key is a scoped, read-only credential derived from the app's own key and limited to the objects explicitly attached to it.

SDKs can access objects using the `/shared` endpoints and a valid sharing key

#### Unpinning a slab that is still referenced by an object returns an error

`DELETE /slabs/:slabid` now fails with `409 Conflict` if one of the account's
objects still references the slab. Previously the slab was unpinned anyway,
freeing up the account's quota while the object kept the slab alive.

### Fixes

- Check tip height of host against local height in AppendSectors.
- Don't consider sectors lost when they are pinned to a contract beyond its proof height.
- Fix objects_events.updated_at not truncating default.
- Skip redundant host settings signature verification on cache hits

## 0.4.1 (2026-07-23)

### Features

- Add `remote` subcommand for spinning up a migration-only remote indexer which can help the main indexer migrate faster.
- Added tracking for active connect key metrics.

#### Added support for pre-authorized keys

Pre-authorized keys let developers connect apps without requiring users to complete an interactive approval flow first.

### Fixes

- Bound host connects in the RHP4 client with a 10 second connect timeout
- Clamp value of failureRate at sane epsilon to make sure hosts go back to 0 failures eventually
- Extend ContractsStats and HostStats with information about locked allowance
- Treat corrupt sectors as lost
- Unblock usable hosts that were automatically blocked due to being unusable
- Update default cutoff when pruning slabs to 72 hours

## 0.4.0 (2026-07-08)

### Breaking Changes

#### Added support for slab versioning

Slab versioning lets us change the encoding scheme of slabs to add new features or change functionality

### Fixes

- Updated coreutils to v0.23.3

## 0.3.2 (2026-07-04)

### Fixes

- Updated coreutils to v0.23.1 for new sector read error message.

## 0.3.0 (2026-06-30)

### Breaking Changes

#### Add basic pool support.

Fund one pool per connect key instead of individual accounts. Accounts attached to a pool draw from its shared balance.

### Features

- Added `reconnecting` field to auth connect status so apps can tell if a user is returning or connecting for the first time.

#### Add network read and write throughput estimates to the client.

The client now exposes `ReadEstimate` and `WriteEstimate`, returning the expected duration to transfer a given number of bytes from the network-wide observed throughput. Both fall back to a default rate before any bulk transfers have been sampled.

### Fixes

- Chunk downloads for slab migrations rather than downloading full sectors to spread downloads out over more hosts.
- Chunk RecordIntegrityCheck UPDATEs to bound per-statement latency and shorten row-lock windows.
- Don't renew a contract with capacity 0 if there is another active contract with capacity 0 already
- Ensure that pruning slabs/sectors in parallel doesn't leave orphaned slabs/sectors
- Rework the unhealthy slabs query to avoid full table scans on large databases.
- Register additional sharing account for each user
- Take into account inflight uploads and downloads when scheduling hosts.

#### Return a typed HTTPError from the app client on non-2xx responses.

The app client previously returned `errors.New("")` when an upstream proxy returned a non-2xx status code with an empty body, making failures impossible to diagnose. It now returns an `*HTTPError` carrying both the status code and body, formatted as `HTTP <code>: <message>`. Callers can `errors.As` on it to branch retry behavior on the status code.

## 0.2.3 (2026-06-04)

### Features

- Add cutoff argument to PruneSlabs to only prune slabs that have been pinned for some amount of time.
- Remove ErrAbortedRPC.

### Fixes

- Repinning a slab now rebinds host sectors that have been lost.
- Use `ErrorCodeClientError` to prevent transport reset for all client errors.
- Expose AddFailedRPC on the client
- Fetch unhealthy slabs for migration in separate goroutine to keep pipeline saturated.
- Slab migrations no longer move sectors whose contracts are healthy but excluded from appends, e.g. because they are in the renew window, at max size, or low on allowance/collateral.
- Support compressed Geo IP database downloads from CDN.
- Update mux to v1.5.1.

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
