CREATE TABLE hosts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL,
    total_scans INTEGER NOT NULL DEFAULT 0,
    successful_scans INTEGER NOT NULL DEFAULT 0,
    failed_scans INTEGER NOT NULL DEFAULT 0,
    consecutive_failed_scans INTEGER NOT NULL DEFAULT 0,
    last_announcement TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    last_scan_success BOOLEAN NOT NULL DEFAULT FALSE,
    next_scan TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '0001-01-01 00:00:00+00',
    uptime INTERVAL NOT NULL DEFAULT '0 seconds',
    downtime INTERVAL NOT NULL DEFAULT '0 seconds'
);

CREATE TABLE host_addresses (
    id SERIAL PRIMARY KEY,
    host_id INTEGER REFERENCES hosts(id) ON DELETE CASCADE,
    net_address TEXT NOT NULL,
    protocol SMALLINT NOT NULL
);
CREATE INDEX  host_addresses_host_id_idx ON host_addresses (host_id);

CREATE TABLE syncer_peers (
    ip_address INET PRIMARY KEY,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_connect TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '0001-01-01 00:00:00+00',
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

CREATE TABLE wallet_events (
    id SERIAL PRIMARY KEY,
    chain_index BYTEA NOT NULL,
    maturity_height INTEGER NOT NULL,
    event_id BYTEA UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    event_data BYTEA NOT NULL
);
CREATE INDEX wallet_events_chain_index_idx ON wallet_events(chain_index);
CREATE INDEX wallet_events_maturity_height_id_idx ON wallet_events(maturity_height DESC, id DESC);

CREATE TABLE wallet_siacoin_elements (
    id SERIAL PRIMARY KEY,
    output_id BYTEA UNIQUE NOT NULL,
    value NUMERIC(50,0) NOT NULL,
    address BYTEA NOT NULL,
    merkle_proof BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    maturity_height INTEGER NOT NULL
);

CREATE TABLE global_settings (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
    db_version INTEGER NOT NULL, -- used for migrations
    last_scanned_index BYTEA -- chain index of the last scanned block
);

CREATE TABLE contracts (
  id SERIAL PRIMARY KEY,
  host_id INTEGER REFERENCES hosts(id) NOT NULL,
  contract_id BYTEA NOT NULL UNIQUE DEFERRABLE,

  -- lifetime related columns
  formation_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
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

  -- contract state
  good BOOLEAN NOT NULL DEFAULT TRUE,

  -- spending (not ACID)
  append_sector_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,
  free_sector_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,
  fund_account_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,
  sector_roots_spending DECIMAL(50, 0) NOT NULL DEFAULT 0
);

CREATE TABLE contract_elements (
    id SERIAL PRIMARY KEY,
    contract_id INTEGER UNIQUE NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    contract BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    merkle_proof BYTEA NOT NULL
);
