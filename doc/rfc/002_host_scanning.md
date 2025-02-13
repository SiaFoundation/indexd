# Host Scanning

## Abstract

The foundation of renting on the Sia network are the contracts that renters form
with hosts to pay them for their provided services. Since the Sia network is
completely permissionless, hosts can come and go as they please. This means, we
need to be careful about what hosts to pick for forming contracts which requires
scanning them and tracking some metrics about them.

### Host Scanning

To select hosts, the Indexer needs to know about its settings. This is done by
performing host scans. Scanning a host includes the following steps:

- Fetch the host's settings via the RHP4 settings RPC
  - Upon success, add the time that has passed since the last scan to the host's uptime
  - Upon failure, add the time that has passed since the last scan to the host's downtime
  - Increment the number of successful, failed and total scans in the store
  - Update the next scan time to be in 24 ± 6 hours to spread scans out

- Perform the following checks on the host to determine whether it's good. If
any of the following checks fail, the host is considered "bad":
  - More than 1 week of uptime and a 90%+ uptime overall
  - MaxDuration is greater than configured contract period
  - MaxCollateral per contract of at least 1TB worth of data
  - Perform [Gouging Checks](003_gouging_checks.md)
  - Protocol version of at least 1.0.0
  - Prices should be valid for at least 1 hour
  - Accepts contracts

- Update database
  - Store breakdown of checks in the database
  - Store host settings in the database
  - Store IP subnet(s) of the host in the database (to compute health of slabs)

#### Scheduled Scans

Hosts should be scanned periodically to accurately determine their uptime. To do
so we use the following approach.

- If a host hasn't been scanned yet, scan it
- If the scan was a success, schedule another scan for 24 ± 6 hours from now
- If the scan failed use an exponential backoff to schedule the next scan in 8,
16, 32, 64, 128 and hours with 128 hours being the max

#### Unscheduled Scans

Hosts that we have contracts with are potentially interacted with more
frequently for various operations for which we need valid prices, which in turn
requires us to fetch valid settings from hosts. The following operations trigger
a scan as a byproduct:

- Forming, Renewing and Refreshing a contract always fetches a new price table
- Fetching new prices for:
  - Refilling an account
  - Pinning sectors
  - Migrating data

### Host Pruning

To avoid scanning hosts that have disappeared forever, hosts are deleted from
the database when the following conditions are met:

- We don't have a contract with the host (let it expire first)
- The host has been offline for more than 12 months
- The host has at least 10 consecutive failed scans
