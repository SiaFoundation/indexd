---
default: minor
---

# Return 403 when pinning slabs exceeds a storage limit

`POST /slabs` now responds with `403 Forbidden` instead of `500` when the account's quota or the app's storage limit is full, with the stable body `account storage limit exceeded` or `app storage limit exceeded`.
