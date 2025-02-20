# Data Migration

## Abstract

Data migration is the process of keeping slab data alive on the network. If a
slab is missing pieces, we need to download the minimum number of shards to
restore the missing ones and then upload the missing shards to new hosts. This
process consists mainly of 2 parts. The first one is determining the health of a
slab while the second one is the actual repair loop which periodically fetches
unhealthy slabs from the database and repairs them.

### Health

The health of a slab is determined by the percentage of available parity shards.
That makes it easy to compare the health of slabs of various redundancy
settings. A slab with fewer redundancy shards will drop in health faster when a
host goes bad than one with many and will therefore be repaired sooner.

Computing the health is achieved with the following algorithm which is
periodically applied to all slabs that have a health_valid_until that is in the
past:

1. Retrieve all rows from `host_sectors` that are either not pinned yet
(`contract_id` is `NULL`) or have a good contract (`is_good` is `TRUE`).
2. Make sure they are sorted by their index within the slab and their
`uploaded_at` time in descending order (to prioritize the more recent ones).
3. Loop over the sectors and increment a counter for each sector that:
    - Has a `slab_index` for which we counted a good sector yet.
    - Has a `host_id` for which we haven't counted a sector yet.
    - Has an IP subnet for which we haven't counted a sector yet.
4. Compute the health of the slab using the formula:
    `health = 100 * (good_shards - min_shards / (total_shards - min_shards))`
5. Update the health of the slab in the database:
    - If it is < 0 set it to -1 so that all lost slabs have the same health.
    - If it is > 100, search for the bug that is causing it because it should never happen.

With all of the above in place, the database also exposes a `slabs, nextRefresh := UnhealthySlabs(limit)` method which returns up to `limit` slabs for which the following holds true:
- The slabs are sorted from least failed repair attempts to most failed repair attempts.
- The slabs are also sorted from most unhealthy to least unhealthy.

The point of the former is to prioritize slabs that have not been repaired yet
over slabs that failed to repair. As soon as a slab is repaired successfully,
the counter is reset to 0.

The point of `nextRefresh` is to provide a marker for the repair loop. If
`UnhealthySlabs` returns fewer than `limit` slabs, the repair loop knows to
sleep until `nextRefresh` since there won't be any new slabs to repair until the
health is updated again.

### Repair Loop

With the health code in place, the repair loop is a relatively simple one
process and looks like this:

1. Fetch all good hosts we can upload to from the database.
2. Fetch up to 10 slabs for repair and launch a goroutine for each slab.
3. Determine which shards are missing or stored on bad hosts by using the same
algorithm as the health check.
4. Fetch the account balance and fund the account if necessary for the repair.
5. Download the slab and reconstruct the missing shards (NOTE: compare the
reconstructed shard to the sector root we think it is supposed to hash to. See
[Lost Slabs and Bad Metadata](#lost-slabs-and-bad-metadata)).
6. Upload the missing shards one after another, reevaluating which hosts to use
by continuously applying the rules of the health check (NOTE: uploading a
missing shard to a host affects the hosts that we can upload the remaining
missing shards to).
7. Wait for the goroutines to finish.
8. Update the slabs in the database (NOTE: even if a full repair failed, some
shards might need to be updated).
9. Continue with 1 after the 10 slabs are processed if there is more work or
wait until the next health refresh.

This simplified repair loop has many advantages over `renterd`'s current approach:
- It doesn't require contract locking due to RHP4.
- We don't need to have a separate account funding loop for the indexer's own accounts.
- We don't need the complexity of overdrive and speed estimates.
- We can use a simple exponential backoff strategy to avoid temporarily unavailable hosts.

### Lost Slabs and Bad Metadata

Since slabs are added by users and we have to trust that whatever encryption
keys, sector roots, and redundancy settings add up, we have no guarantee that
the information adds up before we actually start to pin and repair the slab. So
we might need a way to mark a slab as inconsistent and to exclude it from the
health loop. e.g. if decryption and erasure coding don't produce the expected
root making us unable to repair the slab.

### Discussion

One last concern we have is hosts for which uploads succeed but pinning fails.
It seems like it would be a good idea for the `HostManager` to not hand out
hosts to the repair loop that we actively fail to pin to at the moment. Which is
why I think the aforementioned backoff mechanism should be shared by the repair
and pinning code within the `HostManager`. After all, we don't really mind
pausing repairs to a host for a bit if it means we don't end up uploading to a
host that we will have to migrate away from soon.
