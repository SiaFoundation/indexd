---
default: minor
---

# Added support for sharing keys

Sharing keys let an app grant read-only access to a specific set of objects without requiring the viewer to sign up or log in. Each key is a scoped, read-only credential derived from the app's own key and limited to the objects explicitly attached to it.

SDKs can access objects using the `/shared` endpoints and a valid sharing key