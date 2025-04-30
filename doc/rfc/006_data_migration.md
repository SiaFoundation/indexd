# Data Migration

## Abstract

Data migration is the process of keeping slab data alive on the network. If a
slab is missing pieces, we need to download the minimum number of shards to
restore the missing ones and then upload the missing shards to new hosts. This
process consists mainly of 2 parts. The first one is determining the health of a
slab while the second one is the actual repair loop which periodically fetches
unhealthy slabs from the database and repairs them.

### Health

The health of a slab is determined by its likelihood of becoming unrecoverable.
In `renterd` this was a relatively hard and expensive to compute metric since it
accounts for sectors that are stored redundantly on multiple hosts. The database
schema of `indexd` is more straightforward. Since we only ever keep a single row
per sector in the `sectors` table of our database, we can simply determine the
number of shards until failure by counting the result of joining `slabs.id` with
`sectors.slab_id` and `contracts.id` with `sectors.contract_sectors_map_id`
(through the `contract_sectors_map` join table) and filtering for
`sectors.host_id != NULL AND contract.is_good == TRUE`. Subtracting
`slabs.min_shards` from that number gives us the remaining shards until failure.

Fetching slabs for repair is then as simple as querying the health of all slabs,
ordered by the number of shards until failure and limiting the results to a
number just enough to saturate our repair loop such as `10`.

### Repair Loop

With the health code in place, the repair loop is a relatively simple one
process and looks like this:

1. Fetch all good hosts we can upload to from the database.
2. Fetch up to 10 slabs for repair and launch a goroutine for each slab.
3. Determine which shards are missing or stored on bad contracts. Then find
replacements for them taking into account the IP subnets of hosts that already
store shards.
4. Fetch the account balance and fund the account if necessary for the repair.
5. Download the slab and reconstruct the missing shards (NOTE: compare the
reconstructed shard to the sector root we think it is supposed to hash to. See
[Lost Slabs and Bad Metadata](#lost-slabs-and-bad-metadata)).
6. Upload the missing shards one after another, reevaluating which hosts to use
by continuously applying the IP net check (NOTE: uploading a
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
