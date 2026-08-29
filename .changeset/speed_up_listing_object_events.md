---
default: patch
---

# Speed up listing object events

#1076 by @chris124567

- Add query parameter (`expandslabs`) to `/objects` endpoint that omits sector info from slabs
- Add optional binary encoding
- Add batch endpoint to get full slab details for many slabs

Close #1069
