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
[Contract Maintenance](003_contract_maintenance.md)). That's because we require
exclusive control over a contract to do so.

To avoid overwhelming the host, pruning sectors is limited to 1TiB worth of data
per batch. To avoid overwhelming the indexer, we limit pruning a contract to
once per day using a `next_prune` field on the contract within the database.

To determine whether we need to prune or not, we can keep a `prunable_data` flag
on the contract which we increment whenever we delete from the `host_sectors`
table and set to `0` whenever we end up finishing pruning a contract. So even if
it goes out of sync, it will eventually be in sync again after getting pruned.

The actual pruning of a contract is performed as follows:
1. Loop over batches of up to 1TiB worth of roots
2. Fetch the batch of roots from the host
3. Diff them against the database to figure out which ones to prune
4. Use RPCFreeSector to prune the indices returned by the database
5. Repeat from 1 until offset is greater than the contract's size

NOTE: The process is not perfect and might miss some sectors. That's because
pruning a sector swaps the sector to prune with the last one from the contract.
So the swapped-in sectors are not within the range of our next lookup. We
consider this acceptable as it simplifies the code. In the worst case, when a
10TiB contract needs to be fully pruned, it takes us 4 iterations to do so.
