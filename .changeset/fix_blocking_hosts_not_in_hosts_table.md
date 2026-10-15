---
default: patch
---

# Fix blocking hosts that are not in the hosts table

Blocking a host that indexd has not seen yet no longer fails when no reasons are given, and blocking it again now merges the new reasons with the existing ones instead of replacing them.
