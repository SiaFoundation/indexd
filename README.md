# Indexd

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

```sh
go generate ./...
go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w' ./cmd/indexd
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
config file for you. More more information on configuration options, see the
[Configuration](#configuration) section below.

Once indexd is configured, you can run `indexd` with
```sh
./indexd
```
which will automatically open the admin UI in your default browser if
`autoOpenWebUI` is set to `true` in the config file.

## Docker

`indexd` is also available as a Docker image at `ghcr.io/siafoundation/indexd`.

You can use it with a PostgreSQL database running in another container. Here's
an example `docker-compose.yml` file to get you started. You can set the
`INDEXD_DB_PASSWORD` environment variable in a `.env` file in the same directory
as the `docker-compose.yml` file.

Before starting indexd, to configure the container, run `docker compose run --rm
-it indexd config` to interactively create a config file. By default, the config
uses `verify-full` as the SSL mode for the PostgreSQL connection, which requires
the PostgreSQL server to have a valid SSL certificate. If your PostgreSQL server
doesn't have one, you can change the mode to a less restrictive one like
`prefer` in the advanced settings of the configuration agent.

```yml
services:
  postgres:
    restart: unless-stopped
    shm_size: 16g
    environment:
      POSTGRES_USER: indexd
      POSTGRES_PASSWORD: ${INDEXD_DB_PASSWORD}
      POSTGRES_DB: indexd
    volumes:
      - postgres:/var/lib/postgresql/data

  indexd:
    depends_on:
      - postgres
    image: ghcr.io/siafoundation/indexd:master
    restart: unless-stopped
    ports:
      - 10981:9981/tcp # public syncer
      - 10982:9982/tcp # public apps API
    volumes:
      - indexd:/data
      
volumes:
  indexd:
  postgres:
```

## API

`indexd` exposes two HTTP APIs:

- **Admin API** (`9980`) -- The password - protected API that serves the UI.
  Used for configuring the indexer and managing the wallet, contracts and apps.
- **Application API** (`9982`) -- The public-facing API that third-party
  applications connect to for storing metadata of uploaded objects. Applications
  use URL signing with their internal secret to access it

OpenAPI specifications for both APIs are available in the
[`openapi/`](openapi/) directory.

## Configuration

`indexd` is configured via a YAML config file. Run `indexd config` to
interactively generate one.

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
configuration, but the config file will take precedence over environment
variables if both are set.

| Variable | Description |
|----------|-------------|
| `INDEXD_ADMIN_PASSWORD` | Password for the admin API |
| `INDEXD_CONFIG_FILE` | Override the config file path |
| `INDEXD_DATA_DIR` | Override the data directory |

### Example Config

```yaml
autoOpenWebUI: true
directory: /etc/indexd
debug: false # Enable debug endpoints and logging
recoveryPhrase: your twelve word recovery phrase goes here put it in now please ok # Your secret recovery phrase, you can generate one using the 'indexd seed' command
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
    sslmode: require # the SSL mode for the PostgreSQL connection (https://www.postgresql.org/docs/current/libpq-ssl.html)
```
