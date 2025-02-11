# Contract maintenance

## Abstract

TODO

### Contract Archiving

Before we look into forming new contracts, we archive contracts that are either
expired or have been renewed.

### Contract Formations

The goal of the contract formation process is to keep around a default of at
least 50 contracts that meet the following requirements:

- The corresponding host is considered "good" (see [Host Scanning](001_host_scanning.md))
- The corresponding host doesn't share the same IP subnet as another host we have a contract with (if they do, they count as one)
- The contract has less than 10TB of data in it (if it has more and the host is good, we form another contract with the same host)
- The corresponding host has at least 10GB of free space
- The contract is neither out of collateral nor out of funds
- The contract is not less than half a renew window away from expiring

To achieve that number, we perform the following steps:
1. Fetch all good hosts
2. Randomly pick one of them
3. Scan the host
4. Make sure forming a contract actually increases our number of good contracts
5. Form a contract with the host
6. Repeat from step 2 until the desired number of contracts is reached

Initially we fund contracts with 10SC for both allowance and collateral (TODO: edge case 0 storage cost)

### Contract Renewals

Contract renewals are similar to contract formations but the requirements are
slightly different. We don't care about the number of contracts and instead
renew a contract if:

- The host is considered "good" (see [Host Scanning](001_host_scanning.md))
- The contract has data in it

Assuming these conditions are met we try to renew the contract without
increasing the funds or collateral within the contract. That is what the refresh
is for.

### Contract Refreshes

TODO: condition for refresh

### Bad Contracts

Bad contracts are contracts that don't contribute to the health of a slab. A contract is considered bad if:

- The host is considered "bad" (see [Host Scanning](001_host_scanning.md))
- The contract fails to renew and has reached the second half of the renew window
