---
default: patch
---

# Fix admin API spec mismatches

The admin OpenAPI spec now matches what the API sends and accepts. Protocol versions are strings like `v1.2.3`, the hosts blocklist request uses `hostKeys` and its Go example passes reasons as a slice, the lost sectors reset route is `/host/{hostkey}/lostsectors/reset`, `/state` includes `syncHeight`, hosts include `accountFunding`, `totalSpent`, and `stuckSince` instead of `fundsSpent` and report `recentUptime` as a number, account stats include `pinnedData` and `pinnedSize`, and detailed host stats include `protocolVersion` and `release`.
