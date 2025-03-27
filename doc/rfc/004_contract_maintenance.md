# Contract maintenance

## Abstract

For data to remain on the network, contracts needs to be formed and renewed. The
contract maintenance process automates this by performing a range of checks on
hosts and existing contracts to determine whether the contracts we have are
still sufficient. This RFC outlines the necessary steps performed by the
contract maintenance code.

### Contract Archiving

Before we look into forming new contracts, we archive contracts that are either
expired or have been renewed. This step is pretty straightforward but still
crucial as contracts that are no longer needed can block new contracts from
being formed.

### Contract Formations

The goal of the contract formation process is to keep around a default of at
least 50 contracts that meet the following requirements:

- The corresponding host is considered "good" (see [Host Scanning](002_host_scanning.md))
- The corresponding host doesn't share the same IP subnet as another host we have a contract with (if they do, they count as one)
- The contract has less than 10TB of data in it and hasn't reached its MaxCollateral (if it has more and the host is good, we form another contract with the same host)
- The corresponding host has at least 10GB of free space
- The contract is not less than half a renew window away from expiring

To achieve that, we perform the following steps:
1. Fetch all good hosts
2. Randomly pick one of them
3. Scan the host
4. Make sure forming a contract actually increases our number of good contracts
5. Form a contract with the host
6. Repeat from step 2 until the desired number of contracts is reached

Initially, we fund contracts with 10GB (upload+download+storage) of allowance
and 10GB of collateral. e.g. if 10SC equals 100GB of data, we add 100GB worth of
collateral.

### Contract Renewals

Contract renewals are similar to contract formations but the requirements are
slightly different. We don't care about the number of contracts and instead
renew a contract if:

- The host is considered "good" (see [Host Scanning](002_host_scanning.md))
- The contract has data in it
- The window height of the contract is less blocks away than the renew window

Assuming these conditions are met we try to renew the contract without
increasing the funds or collateral within the contract. That is what the refresh
is for.

### Contract Refreshes

Refreshing a contract is similar to renewing it but without extending its
expiration height. There are two triggers for refreshing a contract:

1. The contract is out of funds -> remaining funds are less than 10% of the
initial allowance
2. The contract is out of collateral -> remaining collateral is less than 10% of
the total collateral

If the reason for the refresh is 1., we reset the funds to the initial allowance
of the contract plus 20% without ever going below the initial funding amount of
10SC.

If the reason for the refresh is 2., the allowance remains the same and we
convert the remaining allowance to a collateral amount the same way we do when
forming contracts.

### Bad Contracts

Bad contracts are contracts that don't contribute to the health of a slab. A contract is considered bad if:

- The host is considered "bad" (see [Host Scanning](002_host_scanning.md))
- The contract fails to renew and has reached the second half of the renew window

### Funding, Pinning and Pruning

Apart from forming and renewing contracts, the contract maintenance is also
responsible for funding accounts, pinning data to contracts and pruning data
that is no longer needed.

All of these tasks can only be performed sequentially on a single contract, but
we can perform it in parallel for multiple contracts. So we use the following
loop to do so:

1. Create a semaphore/channel that limits our goroutines to 50
2. Loop over all hosts with contracts and launch a goroutine to perform account
funding on all accounts that require it
3. Wait for goroutines to finish
4. Do the same thing again but for a batch (1TiB) of pinning limited to 60s per
host (see [Slab Pinning](006_slab_pinning.md)) 3. Wait for goroutines to finish
5. Do the same again but this time for pruning using the same 60s timeout
6. Wait for goroutines to finish
7. Repeat from step 1 until there is no more work to perform

The biggest advantage of this approach is its simplicity. Where `renterd`
required distributed locking since contracts were used by multiple entities,
every contract in the `indexer` is only modified within the maintenance loop. So
we don't need any contract locking at all.
