# Gouging Checks

## Abstract

In a decentralized, permissionless network like Sia, we need to be careful about
malicious actors. From the indexer's perspective this means, that we have to
assume hosts might try and trick us into spending too much money or even
draining our contracts completely. To do so, we perform a set of checks that we
refer to as the `gouging checks`. This RFC outlines these checks and when they
need to be performed to effectively prevent abuse.

### Performed Checks

The actual gouging checks are performed on the `Prices` section of a host's
settings. The following table lists the various settings a host can define as
well as the countermeasure performed to avoid losing money when a host suddenly
changes them.

| Setting           | Assertion |
|-------------------|----------------|
| ContractPrice     | < 1SC |
| Collateral        | > configured MinCollateral && >= 2x StoragePrice|
| StoragePrice      | < configured MaxStoragePrice |
| IngressPrice      | < configured MaxIngressPrice |
| EgressPrice       | < configured MaxEgressPrice |
| FreeSectorPrice   | <= 1SC / TB |
| TipHeight         | not more than 144 blocks in the past |

### When to perform the checks

Gouging checks are performed whenever we scan a host. Meaning every time we
either run a scheduled scan of a host or need up-to-date prices. That way, the
indexer only ever needs to perform them in a single spot.
