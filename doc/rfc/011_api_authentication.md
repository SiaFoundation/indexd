# API Authentication

## Abstract

The `indexer` serves an http API that allows clients to interact with it. There
are two different types of clients, the admin, who gets full access to the API
including the UI, and users, who can only access the API relevant to pinning
slabs. For additional security, these APIs are served on separate ports.

## Admin API

The admin API is protected via http basic authentication and should never be
exposed on the public internet. Instead, tools such as SSH tunnels, VPNs or
existing acess control systems such as Authelia should be used to avoid
reinventing the wheel in the `indexer`.

For example, SiaHub will probably access the admin API for registering a new
account for an app after autenticating the user. That authentication will be
implemented within SiaHub which will have access to the admin API via some sort
of virtual network over docker, K8s or similar.

## User API

The user API is a bit more tricky. Users, or rather their apps, need to be able
to authenticate via a private key corresponding to a previously registered
public key. This public key serves for identifying the app as well as the
account the `indexer` funds for the user with its hosts.

### Signed URLs

To create a signed URL, follow the steps below:

1. Construct the payload to sign using the following format

```
HOST: <hostname> // e.g. indexer.sia.tech
SiaIdx-ValidUntil: <unix timestamp>
```

- `Host`: Makes sure a request for indexer.sia.tech can't be reused for indexer.thirdparty.tech
- `SiaIdx-ValidUntil`: The time at which the request expires in Unix timestamp format

2. Sign the payload using ED25519

3. Construct the signed URL by attaching the query parameters:
- `SiaIdx-Credential`: The public key used to verify the signature
- `SiaIdx-Signature`: The signature from step 2
- `SiaIdx-ValidUntil`: The time at which the request expires in Unix timestamp format

#### Trade-offs:

Compared to other signed URL schemes, like AWS's V4, this scheme is much
simpler. It assumes that the client always uses https to connect to the server
to make sure the request isn't tampered with and that requests aren't pre-signed
for third parties.
That means we can drop the signing of headers, url path or query parameters
since we don't need to be able to restrict access to certain resources for thid
parties.

If needed, it should be easy enough to include additional functionality by
versioning this scheme and adding more fields to the payload.
