# Account Management

## Abstract

This RFC describes the user account management for the `indexer`. Users are
expected to create one account per app they want to use with the indexer. An account is a ed25519 keypair and
serves the following purposes:

1. Authentication: Apps sign requests with the corresponding private key of the account as an alternative to Basic Auth
2. Slab Isolation: Slabs are associated with an account and can only be removed by the account owner to prevent one app accessing data from another app
3. Payment: The public key of the account is funded by the `indexer` on all hosts the `indexer` has good contracts with

### Authentication

See [API Authentication](011_api_authentication.md)

### Account Funding

Unlike `renterd`, the `indexer` doesn't locally track the expected balance of
accounts to know when to fund them. Instead, it uses a ratelimit-based approach.
Accounts are funded with at most 1SC per hour by the contract maintenance (see
[contract maintenance](004_contract_maintenance.md). To do so, the following
state will be stored for every account and host:

| **Field** | **Description** |
|-------|-------------|
| Account | The public key of the account |
| TimeToFund | The time when the account will be funded again |
| Host | The host the account is funded on |

The `indexer` will then perform the following steps for each host it has a contract with:
1. Fetch all accounts for that host where `TimeToFund` is in the past
2. In batches of up to 1000, fund the accounts with the replenish RPC to restore
their balances to 1SC again. If the remaining funds in the contract are too low,
fund fewer accounts.
3. For each account in the batch:
  3a. Upon success, update `TimeToFund` to 1 hour in the future
  3b. Upon failure, log the error and update `TimeToFund` using an exponential backoff (1, 2, 4, 8, 16, 32, 64 and 128 minutes)
4. Sleep until the lowest `TimeToFund` for an account on this host is reached or until the next account is registered

**Malicious Hosts**

Using the replenish RPC, account funding is a pretty efficient operation that
won't waste any funds assuming a host is honest.

For malicious hosts, the worst case scenario is that the host makes us fund
every account with 1SC per hour. Leading to a total spending of 24SC a day or
720SC a month per account.

A potentially effective countermeasure would be some simple statistical
analysis. By tracking how much money we pour into each of our hosts on average,
we can block hosts that are outliers from having their accounts funded.
