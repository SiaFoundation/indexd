# Price Pinning

## Abstract

Price pinning allows the indexer to pin their price limits (see [gouging
checks](003_gouging_checks.md)) to values in fiat currency. Sia's token,
Siacoin, is volatile and for most people it is more convenient to deal with
currencies such as USD or EUR which are a lot more stable.

This RFC outlines the two main components that make up price pinning. The
explorer and the pin manager.

### Explorer

The explorer exposes an API for fetching the conversion rates of Siacoin to a
given fiat currency. By default, the explorer will use `https://api.siascan.com`
as its source but an alternative source can be configured as long as they adhere
to the following interface:

```
GET <url>/exchange-rate/siacoin/<currency>
```

The resulting response should only contain the exchange rate as a float.

### Pin Manager

The `PinManager` uses the `Explorer` to periodically compare the currently
persisted rate from the database to the media rate over the last 6 hours worth
of data resulting in the following algorithm:

Every 5 minutes, the `PinManager` will fetch the current rate from the `Explorer` and then
1. add it to its internal array of `rates`
2. if it has more than 6 hours worth of rates (`len(rates) > 6 hours / 5 minutes`), drop the oldest one
3. calculate the median of the `rates` and compare it to the current one
4. if it has changed by more than 2%, update the gouging settings to the pinned values

The 2% threshold is based on what is often considered the ideal inflation rate.
So going above that seems like a good indicator for enough inflation to adjust
prices.
