# Request for Comments (RFC): Indexer Database Schema

## 1. Introduction

This RFC outlines the database schema for the `indexer`, which is a service
storing and maintaing data on the Sia network. The schema is designed for
efficient querying and data integrity.

PostgreSQL was chosen for its reliability, scalability, and support for advanced
data types. It provides robust indexing options like GIST and B-Tree for
efficient queries, ACID compliance for transactional integrity, and
extensibility for future optimizations. Its ability to handle concurrent access
efficiently makes it a strong choice for high-performance applications.

## 2. Database Schema

### 2.1 Settings

The global settings table stores the database version so we know what (possibly)
migrations to execute on startup, as well as the index of the last scanned
block. Single-row enforcement with CHECK (id = 0): Ensures only one
configuration entry.

```postgresql
CREATE TABLE global_settings (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
    db_version INTEGER NOT NULL, -- used for migrations
    last_scanned_index BYTEA -- chain index of the last scanned block
);
```

### 2.2 Wallet

The wallet stores events as well as the state of the wallet in the form of its
outputs. There's several types of events, the raw event data is stored in the
`event_data` column, which is a `BYTEA` since it doesn't have to be searchable.

Notable columns/types:
- **`NUMERIC(50,0)` for `value`**: Allows precise representation of siacoins.

```postgresql
CREATE TABLE wallet_events (
    id SERIAL PRIMARY KEY,
    chain_index BYTEA NOT NULL,
    maturity_height INTEGER NOT NULL,
    event_id BYTEA UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    event_data BYTEA NOT NULL
);
CREATE INDEX wallet_events_chain_index_idx ON wallet_events(chain_index);

CREATE TABLE wallet_siacoin_elements (
    id SERIAL PRIMARY KEY,
    output_id BYTEA UNIQUE NOT NULL,
    value NUMERIC(50,0) NOT NULL,
    address BYTEA NOT NULL,
    merkle_proof BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    maturity_height INTEGER NOT NULL
);
CREATE INDEX wallet_siacoin_elements_output_id_idx ON wallet_siacoin_elements(output_id);
```

### 2.3 Syncer

The syncer connects to other peers and syncs the Sia blockchain. It keeps a list
of peers alongside some information per peer, as well as a list of peers it
banned.

Notable columns/types:
- **`INET` for `ip_address`**: Efficient storage and validation of IP addresses.
- **`CIDR` for `net_cidr`**: Allows subnet-level banning and optimized lookups.
- **`GIST index on net_cidr`**: Enables fast subnet matching.

```postgresql
CREATE TABLE syncer_peers (
    ip_address INET PRIMARY KEY,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_connect TIMESTAMP WITH TIME ZONE NOT NULL,
    synced_blocks INTEGER NOT NULL DEFAULT 0 CHECK (synced_blocks >= 0),
    sync_duration INTEGER NOT NULL DEFAULT 0 CHECK (sync_duration >= 0)
);

CREATE TABLE syncer_bans (
    net_cidr CIDR PRIMARY KEY,
    expiration TIMESTAMP WITH TIME ZONE NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX syncer_bans_expiration_idx ON syncer_bans (expiration);
CREATE INDEX syncer_bans_net_cidr_idx ON syncer_bans USING gist (net_cidr inet_ops); -- fast subnet matches
```

### 2.4 Hosts

```postgresql
CREATE TABLE hosts {
    id SERIAL PRIMARY KEY,
    public_key BYTEA NOT NULL,

    total_scans INTEGER NOT NULL DEFAULT 0,
    successful_scans INTEGER NOT NULL DEFAULT 0,
    failed_scans INTEGER NOT NULL DEFAULT 0,
    consecutive_failed_scans INTEGER NOT NULL DEFAULT 0,
    last_scan_success BOOLEAN NOT NULL,
    next_scan TIMESTAMP WITH TIME ZONE NOT NULL,
    uptime INTERVAL NOT NULL DEFAULT '0 seconds',
    downtime INTERVAL NOT NULL DEFAULT '0 seconds',
}

CREATE TABLE host_addresses {
    id SERIAL PRIMARY KEY,
    host_id INTEGER PRIMARY KEY
        REFERENCES hosts(id)
        ON DELETE CASCADE,

    net_address TEXT NOT NULL,
    protocol SMALLINT NOT NULL,
}

CREATE TABLE host_resolved_cidrs {
    id SERIAL PRIMARY KEY,
    host_id INTEGER
        REFERENCES hosts(id)
        ON DELETE CASCADE,

    cidr CIDR NOT NULL,
}

CREATE TABLE host_settings {
    host_id INTEGER PRIMARY KEY
        REFERENCES hosts(id)
        ON DELETE CASCADE,

    protocol_version BYTEA NOT NULL,
    release TEXT NOT NULL,
    wallet_address BYTEA NOT NULL,
    accepting_contracts BOOLEAN NOT NULL,
    max_collateral NUMERIC(50,0) NOT NULL,
    max_contract_duration NUMERIC(50,0) NOT NULL,
    remaining_storage BIGINT NOT NULL,
    total_storage BIGINT NOT NULL,

    contract_price NUMERIC(50,0) NOT NULL,
    collateral NUMERIC(50,0) NOT NULL,
    storage_price NUMERIC(50,0) NOT NULL,
    ingress_price NUMERIC(50,0) NOT NULL,
    egress_price NUMERIC(50,0) NOT NULL,
    free_sector_price NUMERIC(50,0) NOT NULL,
    tip_height BIGINT NOT NULL,
    valid_until TIMESTAMP WITH TIME ZONE NOT NULL,
}
´´´
