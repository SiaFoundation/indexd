---
default: patch
---

# Slab migrations no longer move sectors whose contracts are healthy but excluded from appends, e.g. because they are in the renew window, at max size, or low on allowance/collateral.
