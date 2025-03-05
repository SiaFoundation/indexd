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

To create a signed URL, follow the steps below:

1. Construct the canonical request like so

```
HTTP_VERB\n
PATH_TO_RESOURCE\n
CANONICAL_QUERY_STRING\n
CANONICAL_HEADERS\n

SIGNED_HEADERS
```

- `HTTP_VERB`: The HTTP verb like `GET`, `POST`, etc.
- `PATH_TO_RESOURCE`: The path to the resource like `/api/slab/pin`
- `CANONICAL_QUERY_STRING`: The query string sorted by name using a lexicographical sort by code point value, separated by '&' or a \n if there are no query parameters.
- `CANONICAL_HEADERS`: One line per header, lowercase name for each header, sorted like the query strings, duplicate headers collapsed into comma-separated list, folding whitespaces or newlines replaced with single space
- `SIGNED_HEADERS`: List of lowercase header names to be signed, separated by semicolon (TODO: not sure if that is actually necessary considering that we have the CANONICAL_HEADERS)

The following canonical query strings are required:
`X-SiaIdx-Credential` - The public key of the user
`X-SiaIdx-Date` - The date at which the request becomes usable in ISO8601 format
`X-SiaIdx-Expires` - The time after which the request expires in seconds (max 24 hours)
`X-SiaIdx-SignedHeaders` - A semicolon-separated list of headers that are signed

The following canonical headers are required:
`host`: The hostname the request gets sent to like `indexer.sia.tech`
`x-siaidx-` prefixed headers: All potential `indexer`-specific headers to be consumed by the API


2. Construct the string to sign:

NOTE: AWS creates this string from the signing algorithm, the date (same as
`X-SiaIdx-Date`), the credential scope and the hashed request. It is not quite
clear why but it seems like we can simplify this by just using the canonical
request directly for signing.

3. Sign the string

We use ED25519 to sign the string rather than RSA, which is used by AWS.

4. Construct the signed URL using the following concatenation:

```
HOSTNAME + PATH_TO_RESOURCE + '?' + CANONICAL_QUERY_STRING + '&X-SiaIdx-Signature=' + REQUEST_SIGNATURE
```

Example:

```
https://indexer.googleapis.com/slabs/pin?X-SiaIdx-Credential=example&X-SiaIdx-Date=20181026T211942Z&X-SiaIdx-expires=60&X-SiaIdx-Signedheaders=host&X-SiaIdx-Signature=<hex-encoded-sig>
```

NOTE: This section is heavily inspired by v4 signatures used by AWS or actually
Google Cloud's documentation on it. For more information see the
[overview](https://cloud.google.com/storage/docs/access-control/signed-urls),
[canonical
requests](https://cloud.google.com/storage/docs/authentication/canonical-requests)
and [manual
signing](https://cloud.google.com/storage/docs/access-control/signing-urls-manually)
pages of their docs.
