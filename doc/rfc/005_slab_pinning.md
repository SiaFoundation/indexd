# Slab Pinning

## Abstract

## API

For user apps, the API to pin uploaded slabs is pretty straightforward. All we
need is a `POST /slabs/pin` endpoint and its `DELETE` counterpart.

### Request

The payload for the `POST` request should look like this:

```json
{
  "slabs": [...]
}
```

Where every slab is an object of the following form:

```json
{
  "Key": "<32-random-bytes>", // key used to encrypt shards after erasure-coding
  "MinShards": 10, // number of data pieces used for uploading
  "Shards": [...] // matches number of data pieces + parity pieces
}
```

The `Key` is necessary because the `indexer` can't repair slabs without the
ability to decrypt shards. However, the first layer of encryption is applied by
the client, so the corresponding key is never sent to the `indexer`.

Each shard within a slab is an object of the following form:

```json
{
  "HostKey": "<public-key-of-host>", // public key of the host the shard is uploaded to
  "Root": "<sector-root>" // merkle root of the sector the shard is uploaded to
}
```

### Response

The response to the request contains one item for each slab:

```json
{
  "slabs": [
    {
      "ID": "<slab-id>", // unique identifier of the slab
    }
    ...
  ]
}
```

The `ID` is special because it is derived from the roots of the shards and key.
This means that two slabs with the same shards are effectively the same. The
reason for including the key is to prevent two slabs from being treated as the
same if one of them was purposefully created with the wrong key. One nice
side-effect creating the `ID` this way is that slabs shared between accounts
will have the same `ID`.

If everything goes well, the slabs are now in the `indexer`'s database and
uniquely identified via their `ID`s. The shards are also persisted as "unpinned
sectors" with a timestamp indicating when they drop out of temporary storage.
However, the slabs are not yet pinned to contracts. That happens asynchronously.

### Slab Scope

Slabs are scoped to the account key that pinned them. So one account can't unpin
another account's slabs or even fetch that slab's metadata.

## Sector Pinning

Pinning sectors happens as part of periodic maintenance that the indexer
performs on contracts (see [Contract Maintenance](003_contract_maintenance.md)).
The process looks like this:

For each host we have a contract with we do the following:
1. Fetch a batch of unpinned sectors (at most 1<<40) sorted by expiration time
2. Attempt to append the sectors to any of the contracts we have with the host
3. Update the database
  a. For successful appends: Update the sector<->host link to contain the contract ID the sector was pinned to
  b. For "missing sectors" errors: Remove the sectors from the database

## Slab Health and unpinned Sectors

Unpinned sectors count towards the health of a slab. That's because the repair
code will reuse the same pinning loop used for user slabs. e.g. if a slab needs
repairs on 5 sectors, the repair loop uploads 5 sectors to temp storage and
notifies the pinning loop. Until the pinning is done, the repair loop shouldn't
revisit the same slab.
