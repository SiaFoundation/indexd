# Price Pinning

## Abstract

Data on Sia is protected via file contracts which require a storage proof at the
end of their lifetime. That storage proof requires the host to proof existence
of a single, random 64 byte segment of the contract. While these proofs are good
at making sure a host hasn't lost all data of a contract due to a disk array
failing it comes with a few pitfalls.

- hosts can't inform renters of losing data
- hosts might just lose part of a contract's data and get lucky about the data they have to create the proof for
- hosts don't need to post a proof for renewed contracts

In fact, there is no incentive for hosts to inform a renter. The more data they
receive the smaller the chance of having to provide a proof for the already lost
data.

This is where integrity checks come into play.

### Integrity Check

An integrity check is effectively an RPC that a renter can execute on a host for
a given sector. The renter picks a segment that the host has to report a proof
for and the host (hopefully) produces a proof. If it can't, we know that it
probably lost the data.

Similar to pinning and pruning data, integrity checks are performed in a loop
that fetches all sectors that are due for an integrity check as indicated by the
`next_integrity_check` column of the `sectors` table. However, it's not
performed by the contract maintenance since we don't need a contract for the
check. The actual steps look like this:

1. Fetch up to 10 pinned sector per contract per host from the database
2. For each one, call `RPCVerifySector` after making sure the account is funded.
We process contracts in parallel but wait for the whole batch to finish before
launching a new one. Apply a sane timeout of 60 seconds per batch and remove
timed out hosts from subsequent batches of this iteration. Also, ignore hosts
for subsequent batches if they time out or fail to prevent slowing down the
other integrity checks or failing in a loop on the same sectors over and over
again.
  a. If the proof was successful, update the time of the next check to 1 week from now
  b. If the host reports the sector as lost, move on to 3.
  b. If the proof failed for any other reason after a successful dial and sending of the initial request,
  set the next check time 6 hours from now and increment the count of failed checks.
3. If the sector has failed its check 3 times in a row or has reported it as
lost, set `host_id` and `contract_id` on the `sectors` table to `NULL` and
increment the `lost_sectors` count on the `hosts` table. This will lead to the
sector being picked up by the data migration code (see [Data
Migration](007_data_migration.md)).

### Host penalty

Using the `lost_sectors` field on a host we can theoretically penalize hosts.
This is a potentially severe measure to automate, we should resort to manual
user intervention. To make it clear to the user that some sort of action should
be taken, we alert them in case a host has lost more than `10GB` worth of
sectors. They can then decide whether they want to "forgive" the host or ban it.
