# [![Indexd](https://sia.tech/api/media/file/banner-indexd.png)](http://sia.tech)

[![GoDoc](https://godoc.org/go.sia.tech/indexd?status.svg)](https://godoc.org/go.sia.tech/indexd)

An indexer for the [Sia](https://sia.tech) network.

## Overview

`indexd` is a daemon built by the Sia Foundation that enables developers to
build applications that require decentralized storage on top of the Sia storage
network. It manages contracts with storage hosts, makes sure uploaded data stays
available, and exposes an application API that apps can connect to for storing
the metadata of uploaded objects. None of the data uploaded to Sia through
`indexd` is stored on the `indexd` server itself. All data is encrypted and
stored on the Sia network. So even the indexer doesn't have access to the
content of the files being uploaded.

To build your own app, you can use the [Go SDK](sdk/README.md), the [Rust
SDK](https://crates.io/crates/sia_storage) or one of the
[SDK bindings](https://github.com/siafoundation/sia-storage-sdk) for other
languages.

## Building

`indexd` embeds a
[GeoLite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)
database for IP geolocation. This file is stored using [Git
LFS](https://git-lfs.com), so you must have Git LFS installed before cloning the
repository. Otherwise, indexd won't run because the embedded geolocation
database will appear corrupted.

```sh
# set up Git LFS (once per machine after installing Git LFS)
git lfs install

# clone the repository (LFS files are fetched automatically)
git clone https://github.com/SiaFoundation/indexd.git

# if you already cloned without LFS, pull the real files from within the repo
git lfs pull
```

Once the LFS files are in place, build as usual:

```sh
go generate ./...
go build -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w' ./cmd/indexd
```

## Getting Started

Running `indexd` requires a PostgreSQL database. By default, `indexd` looks for
a PostgreSQL server running on `localhost:5432` with a database named `indexd`
and a user named `indexd`. You can change these settings in the config file.

A convenient way to configure `indexd` the first time is to run
```sh
indexd config
```
which will guide you through an interactive configuration process and generate a
config file for you. For more information on configuration options, see the
[Configuration](#configuration) section below.

Once indexd is configured, you can run `indexd` with
```sh
./indexd
```
which will automatically open the admin UI in your default browser if
`autoOpenWebUI` is set to `true` in the config file.

## Instant Syncing (Experimental)

New users can sync instantly using `indexd --instant`. When instant syncing, the
`indexd` node initializes using a Utreexo-based checkpoint and can immediately
validate blocks from that point forward without replaying the whole chain state.
The state is extremely compact and committed in block headers, making this
initialization both quick and secure. Instant syncing also enables pruning of
old blocks.

[Learn more](https://sia.tech/learn/instant-syncing)

**The wallet is required to only have v2 history to use instant syncing.**

## Docker

`indexd` is also available as a Docker image at `ghcr.io/siafoundation/indexd`.

You can use it with a PostgreSQL database running in another container. You can find an example `docker-compose.yml` file [here](docker-compose.yml).

## API

`indexd` exposes two HTTP APIs:

- **Admin API** (`9980`) -- The password-protected API that serves the UI.
  Used for configuring the indexer and managing the wallet, contracts and apps.
- **Application API** (`9982`) -- The public-facing API that third-party
  applications connect to for storing metadata of uploaded objects. Applications
  use URL signing with their internal secret to access it

OpenAPI specifications for both APIs are available in the
[`openapi/`](openapi/) directory.

## Configuration

`indexd` is configured via a YAML config file. Run `indexd config` to
interactively generate one.

### Command-Line Flags

| Flag | Description |
|------|-------------|
| `-api.admin` | Address to serve the admin API on (default `127.0.0.1:9980`) |
| `-api.app` | Address to serve the application API on (default `:9982`) |
| `-instant` | Enable instant sync mode for faster initial sync. This also enables pruning |

### Default Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 9980 | TCP | Admin UI and API |
| 9981 | TCP | Sia peer-to-peer syncer |
| 9982 | TCP | Application API |

### Default Paths

| | Config File | Data Directory |
|---------|-------------|----------------|
| Linux | `/etc/indexd/indexd.yml` | `/var/lib/indexd` |
| macOS | `~/Library/Application Support/indexd/indexd.yml` | `~/Library/Application Support/indexd` |
| Windows | `%APPDATA%/indexd/indexd.yml` | `%APPDATA%/indexd` |
| Docker | `/data/indexd.yml` | `/data` |

### Environment Variables

The following environment variables may be used to override the default
configuration and take precedence over the config file settings.

| Variable | Description |
|----------|-------------|
| `INDEXD_CONFIG_FILE` | Override the config file path |
| `INDEXD_DATA_DIR` | Override the data directory |

### Example Config

```yaml
autoOpenWebUI: true
directory: /var/lib/indexd
debug: false # Enable debug endpoints and logging
recoveryPhrase: <your twelve word recovery phrase> # Your secret recovery phrase
adminAPI:
    address: :9980
    password: <your admin password> # Your admin password for protecting the admin API
applicationAPI:
    address: :9982
    advertiseURL: "https://app.sia.storage" # The publicly accessible URL for the application API
syncer:
    address: :9981
    bootstrap: true
    enableUPnP: false
    peers: [] # A list of peer addresses to connect to additionally to the default bootstrap nodes
consensus:
    network: mainnet # mainnet | zen
    indexBatchSize: 1000
    pruneTarget: 64 # number of blocks to keep when pruning (0 to disable, minimum 6 hours of blocks)
explorer:
    enabled: true
    url: https://api.siascan.com
log:
    stdout:
        enabled: true # enable logging to stdout
        level: info # log level for console logger
        format: human # log format (human, json)
        enableANSI: true # enable ANSI color codes (disabled on Windows)
    file:
        enabled: true # enable logging to file
        level: info # log level for file logger
        format: json # log format (human, json)
        path: /var/log/indexd/indexd.log # the path of the log file
database:
    host: localhost # the hostname or IP address of the PostgreSQL server
    port: 5432 # the port of the PostgreSQL server
    user: indexd # the username for the PostgreSQL server
    password: <your db password> # the password for the PostgreSQL server
    database: indexd # the name of the PostgreSQL database
    sslmode: verify-full # the SSL mode for the PostgreSQL connection (https://www.postgresql.org/docs/current/libpq-ssl.html)
```
