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

The user API is a bit more tricky. Users or rather their apps need to be able to
authenticate via a private key corresponding to a previously registered public
key. This public key serves for identifying the app as well as the account the
`indexer` funds for the user with its hosts.

### Signed URLs

TODO: write section

NOTE: This section is heavily inspired by v4 signatures used by AWS or actually
Google Cloud's documentation on it. For more information see the
[overview](https://cloud.google.com/storage/docs/access-control/signed-urls) and
[manual
signing](https://cloud.google.com/storage/docs/access-control/signing-urls-manually)
pages of their docs.

### Discussion

For extra security it
