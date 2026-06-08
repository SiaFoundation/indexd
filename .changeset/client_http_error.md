---
default: patch
---

# Return a typed HTTPError from the app client on non-2xx responses.

The app client previously returned `errors.New("")` when an upstream proxy returned a non-2xx status code with an empty body, making failures impossible to diagnose. It now returns an `*HTTPError` carrying both the status code and body, formatted as `HTTP <code>: <message>`. Callers can `errors.As` on it to branch retry behavior on the status code.
