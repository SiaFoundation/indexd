---
default: minor
---

# Allow host key parameters without the `ed25519:` prefix.

Host key query and path parameters in the admin API now accept bare 64 character hex strings in addition to the full `ed25519:<hex>` format.
