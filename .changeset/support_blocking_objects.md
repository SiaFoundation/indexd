---
default: minor
---

# Add support for blocking objects

Adds an object blocklist for content moderation, managed through the new admin
endpoints `GET /objects/blocklist` and `GET`/`PUT`/`DELETE
/objects/blocklist/{objectkey}`. Blocked objects are filtered out in the
database, so they are hidden from `GET /objects`, `GET /sharing/{key}/objects`
and `GET /shared/objects` without disturbing pagination. Fetching one by key
returns `451 Unavailable For Legal Reasons`, as does pinning it or attaching it
to a sharing key — an object key is derived from its slabs, so a blocked key
covers that content for every account, including content that has not been
pinned yet.

Blocking never unpins data. Unblocking restores access and refreshes the
object's event timestamp so clients that already paged past it pick it up again.
