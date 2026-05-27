---
default: major
---

# Added versioning to objects

Added a version field to `SealedObject` and `PinObjectRequest`. This enables us to change the behavior of objects without breaking compatibility with existing data.