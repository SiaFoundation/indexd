# Indexer Database Schema

## 1. Abstract

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

    -- chain index of the last scanned block
    scanned_height BIGINT NOT NULL DEFAULT 0 CHECK(scanned_height >= 0),
    scanned_block_id BYTEA NOT NULL DEFAULT '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea CHECK (LENGTH(scanned_block_id) = 32),

    -- contract manager settings
    contracts_maintenance_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    contracts_wanted INTEGER NOT NULL DEFAULT 50 CHECK(contracts_wanted > 0), -- number of contracts to maintain
    contracts_renew_window INTEGER NOT NULL DEFAULT 144 * 7 * 2 CHECK(contracts_renew_window > 0), -- 2 weeks
    contracts_period INTEGER NOT NULL DEFAULT 144 * 7 * 6 CHECK(contracts_period > contracts_renew_window), -- 6 weeks

    -- host manager settings
    hosts_min_protocol_version BYTEA NOT NULL DEFAULT '\x010000', -- used for host checks
    hosts_min_collateral NUMERIC(50,0) NOT NULL DEFAULT 0, -- hastings / byte / block
    hosts_max_storage_price NUMERIC(50,0) NOT NULL DEFAULT 0, -- hastings / byte / block
    hosts_max_ingress_price NUMERIC(50,0) NOT NULL DEFAULT 0, -- hastings / byte
    hosts_max_egress_price NUMERIC(50,0) NOT NULL DEFAULT 0, -- hastings / byte

    -- pin manager settings
    pins_currency VARCHAR(3) NOT NULL DEFAULT '',
    pins_min_collateral DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (pins_min_collateral >= 0),
    pins_max_storage_price DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (pins_max_storage_price >= 0),
    pins_max_ingress_price DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (pins_max_ingress_price >= 0),
    pins_max_egress_price DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (pins_max_egress_price >= 0)
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
CREATE TABLE hosts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32),
    consecutive_failed_scans INTEGER NOT NULL DEFAULT 0,
    recent_uptime DOUBLE PRECISION NOT NULL DEFAULT 0.894 CHECK (recent_uptime > 0 AND recent_uptime < 1),
    last_failed_scan TIMESTAMP WITH TIME ZONE,
    last_successful_scan TIMESTAMP WITH TIME ZONE,
    last_announcement TIMESTAMP WITH TIME ZONE,
    next_scan TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    lost_sectors INTEGER NOT NULL DEFAULT 0,

    settings_protocol_version BYTEA NOT NULL DEFAULT '\x000000'::bytea CHECK (LENGTH(settings_protocol_version) = 3),
    settings_release TEXT NOT NULL DEFAULT '',
    settings_wallet_address BYTEA NOT NULL DEFAULT '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea CHECK (LENGTH(settings_wallet_address) = 32),
    settings_accepting_contracts BOOLEAN NOT NULL DEFAULT FALSE,
    settings_max_collateral NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_max_contract_duration NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_remaining_storage BIGINT NOT NULL DEFAULT 0,
    settings_total_storage BIGINT NOT NULL DEFAULT 0,
    settings_contract_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_collateral NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_storage_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_ingress_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_egress_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_free_sector_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_tip_height BIGINT NOT NULL DEFAULT 0,
    settings_valid_until TIMESTAMP WITH TIME ZONE
)

CREATE TABLE host_addresses (
    id SERIAL PRIMARY KEY,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,

    net_address TEXT NOT NULL,
    protocol SMALLINT NOT NULL
)

CREATE TABLE host_resolved_cidrs (
    id SERIAL PRIMARY KEY,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,

    cidr CIDR NOT NULL
)
```

### 2.4 Contracts

```postgresql
CREATE TABLE contracts (
  id SERIAL PRIMARY KEY,
  host_id INTEGER REFERENCES hosts(id) NOT NULL,
  contract_id BYTEA NOT NULL UNIQUE DEFERRABLE,

  -- lifetime related columns
  formation TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  proof_height BIGINT NOT NULL, -- start of proof window
  expiration_height BIGINT NOT NULL, -- end of proof window
  renewed_from INTEGER REFERENCES contracts(id) UNIQUE DEFERRABLE,
  renewed_to INTEGER REFERENCES contracts(id) UNIQUE DEFERRABLE,
  state SMALLINT NOT NULL DEFAULT 0, -- 0 = 'pending', 1 = 'active', 2 = 'resolved', 3 = 'expired', 4 = 'rejected'

  -- metrics for visualization (not ACID)
  capacity BIGINT NOT NULL DEFAULT 0 CHECK(capacity >= size),
  size BIGINT NOT NULL DEFAULT 0 CHECK(size >= 0),

  -- costs
  contract_price DECIMAL(50, 0) NOT NULL, -- used to display cost of forming contract
  initial_allowance DECIMAL(50, 0) NOT NULL, -- used when refreshing contract to increase budget
  miner_fee DECIMAL(50, 0) NOT NULL, -- miner fee added when forming/renewing contract
  total_collateral DECIMAL(50, 0) NOT NULL, -- total collateral (allocated+unallocated)

  -- contract state
  good BOOLEAN NOT NULL DEFAULT TRUE,

  -- spending (not ACID)
  append_sector_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,
  free_sector_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,
  fund_account_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,
  sector_roots_spending DECIMAL(50, 0) NOT NULL DEFAULT 0
);
CREATE INDEX contracts_state_formation_idx ON contracts(state, formation); -- for rejecting expired contracts

CREATE TABLE contract_elements (
    id SERIAL PRIMARY KEY,
    contract_id INTEGER UNIQUE NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    contract BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    merkle_proof BYTEA NOT NULL
);
```

### 2.5 Accounts

```postgresql
CREATE TABLE accounts (
    id BIGSERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL,
);

CREATE TABLE account_slabs (
    account_id INTEGER REFERENCES accounts(id) NOT NULL,
    slab_id BIGSERIAL REFERENCES slabs(id) NOT NULL,
    CONSTRAINT account_slabs_pk PRIMARY KEY (account_id, slab_id)
);
```

### 2.6 Slabs and Sectors

```postgresql
CREATE TABLE slabs (
    id BIGSERIAL PRIMARY KEY, -- internal db id

    digest BYTEA UNIQUE NOT NULL, -- unique identifier for the slab derived from sector roots
    encryption_key BYTEA NOT NULL,
    last_repair_attempt TIMESTAMP WITH TIME ZONE,
    min_shards SMALLINT NOT NULL CHECK(min_shards > 0)
)

CREATE TABLE sectors (
    id BIGSERIAL PRIMARY KEY,
    sector_root BYTEA UNIQUE NOT NULL

    -- uploading
    host_id INTEGER REFERENCES hosts(id), -- host that stores sector
    contract_id INTEGER REFERENCES contracts(id), -- null if not pinned
    uploaded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW() -- allow sorting by upload time

    -- slab
    slab_id BIGINT REFERENCES slabs(id) NOT NULL,
    slab_index SMALLINT NOT NULL, -- index within corresponding slab to retrieve sectors in right order
    UNIQUE(slab_id, slab_index), -- enforce one sector per index per slab

    -- data integrity
    next_integrity_check TIMESTAMP WITH TIME ZONE,
    consecutive_failed_checks SMALLINT NOT NULL DEFAULT 0
)
-- quick lookup of sectors to pin prioritized by upload time
CREATE INDEX sectors_contract_id_uploaded_at_idx ON host_sectors(contract_id, uploaded_at ASC)

-- index over contract_id and next_integrity_check since we only check pinned sectors
CREATE INDEX sectors_contract_id_next_integrity_check_idx ON host_sectors(contract_id, next_integrity_check ASC)
```
