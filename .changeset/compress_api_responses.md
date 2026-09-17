---
default: minor
---

# Compress API responses

Responses from the application and admin APIs are now compressed when the client
advertises support for it. Both `zstd` and `gzip` are supported; `zstd` is
preferred when a client accepts either.
