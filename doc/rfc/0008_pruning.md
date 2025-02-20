# Pruning

## Abstract

Pruning is the process of removing data from contracts that the indexer is no
longer interested in storing. Due to the introduction of RHP4 and the new
`Capacity` field of file contracts, we can now reuse storage that is already
paid for by first pruning the data of a contract and then adding data to it.

### Contract Size vs Capacity

Before the v2 hardfork, file contracts had a `Size` field which reflected the
amount of data contained within a specific contract. Pruning was possible, but
lowered the `Size` of a contract and when adding more data to it afterwards, the
storage for that data had to be paid for again.

RHP4 added a new `Capacity` field which can be thought of as the actually paid
for data of a contract. A contract starts with `Capacity = 0` and `Size = 0`.
After uploading 100 sectors both the `Capacity` and `Size` fields are set to the
byte equivalent of 100 sectors. But if we prune 50 sectors from the contract,
the `Size` field changes to 50 sectors and the `Capacity` field remains at 100
sectors. So the next time we upload 50 sectors to the contract, we don't have to
pay the storage price anymore and instead only pay the bandwidth cost. After
that upload, both the `Size` and `Capacity` fields are set to 100 sectors again.

### Pruning Process

Similar to pinning, the pruning process is part of the contract maintenance (see
[Contract Maintenance](004_contract_maintenance.md)). That's because we require
exclusive control over a contract to do so.

To avoid overwhelming the host, pruning sectors is limited to 1TiB worth of data
per batch. To avoid overwhelming the indexer, we limit pruning a contract to
once per day using a `last_prune` field on the contract within the database.

To determine whether we need to prune or not, we can keep a `prunable_data` flag
on the contract which we increment whenever we delete from the `host_sectors`
table and set to `0` whenever we end up finishing pruning a contract. So even if
it goes out of sync, it will eventually be in sync again after getting pruned.

The actual pruning of a contract is performed as follows:
1. Fetch all sector roots from the `host_sectors` table in the database where the
`contract_id` matches the contract to prune and put them in a map (TODO: probably need to benchmark this and maybe batch it)
2. Fetch up to 1TiB worth of sector roots from the contract at offset 0 (up to 80MiB worth of transmitted data)
3. Collect all sector roots that are within the 1TiB batch but not in the roots fetched from the database
4. Prune the collected sectors and increment the offset by 1TiB
5. Repeat steps 2-4 until the new offset > the new contract size

NOTE: The process is not perfect and might miss some sectors. That's because
pruning a sector swaps the sector to prune with the last one from the contract.
So the swapped-in sectors are not within the range of our next lookup. We
consider this acceptable as it simplifies the code. In the worst case, when a
10TiB contract needs to be fully pruned, it takes us 4 iterations to do so.
