# Metadata Store

## Abstract

One of the advantages of the indexer is allowing users to outsource the
responsibility of forming contracts, repairing data, acquiring Siacoin and
running the service 24/7 themselves to a third party. However, without a way to
back up metadata on top pinning slabs, there is no way for client applications
to recover information about what slabs make up an object or additional metadata
such as the object's content type.

To add that behavior, the indexer offers a generic metadata store which holds
client-side encrypted, tamper-resistant data. To maximize privacy, there is no
relation between these blobs of metadata and slabs that the indexer knows of.
This severely limits the capability of the store as the indexer can't perform
searches on this data, but the client can add any necessary complexity
client-side while the metadata store serves purely as a backup mechanism.

## The Store

The actual store is very straightforward. Apps can perform the following actions on it:

- Store a value for a given 32-byte key (SHA-256 HMAC)
- Retrieve a value for a given 32-byte key
- Fetch all available keys with offset+limit

While not important for the indexer implementation since it can't verify it, the
key is intended to be a fingerprint of the value that only the client can
create. To achieve that, we use an HMAC over the encrypted value.

### HMAC Creation

Clients already use ED25519 keys for authentication. To avoid increasing the
complexity of using indexd, we try to keep the number of keys a client
application needs to manage to one. To achieve that, we derive the key we use
for the HMAC from the ED25519 private key since it's not considered safe to
reuse the same key material for different cryptographic algorithms directly.

For that, we use the key derivation function HKDF which is specifically made for
use-cases where you already have entropy-rich input and quickly want to derive
domain-specific keys. The salt/domain should be the string `MD-HMAC-SECRET`. We
then use the resulting key to create the HMAC over the encrypted value to store
in the metadata store.

### Value Encryption

Similar to the HMAC creation, we derive another key for the encryption of the
value with HKDF as well as a random nonce. This time we use the salt
`MD-ENCRYPTION-SECRET`. From that key we derive another one using a random 32
byte salt. Then we use CHACHA20-Poly1305 to encrypt the plaintext
value and append the salt to it.

The reason for using a random salt rather than a random 12 byte nonce is to
avoid nonce reuse. This will always result in a unique key for each stored value
while reusing the nonce might become a problem when a user reaches billions of
messages created from the same key since the probability is about 𝑛^2/2^96 for n
messages.
If we generate a unique key for each stored value, we can safely encrypt every
value starting at nonce = 0.

### Validation

After fetching the value for a key from the store, validation works as follows:

1. derive the `MD-HMAC-SECRET` and `MD-ENCRYPTION-SECRET` keys
2. validate the value against the HMAC
3. extract the salt from the value and derive the key for decryption
4. decrypt the value
5. use the value

### Limits and Abuse

To prevent abuse by users just uploading arbitrary data to the metadata store,
we enforce sane limits.

If we assume the intended usage of the store is to store object metadata and
that an object consists mostly of 48 bytes of metadata per 40MiB slab, 1MB per
key should allow for a max object size of about 1TB, depending on how compact of
a binary format is chosen.

Since users hosting indexd themselves probably won't need to limit themselves,
the default is set to 1GB of metadata per account, which should be enough for
1PB of uploads per app. Entities building services on top of indexd might want
to set this to lower values on a per-account, per-tier basis to prevent abuse.

In addition to this per-account limit, we also limit the size of a single
key-value pair to 5MB. This should be enough to cover objects of about 5TB in
size which is quite generous and won't allow users to lock up our database with
large insertions.
