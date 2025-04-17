CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32)
);

CREATE TABLE hosts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32),
    consecutive_failed_scans INTEGER NOT NULL DEFAULT 0,
    recent_uptime DOUBLE PRECISION NOT NULL DEFAULT 0.894 CHECK (recent_uptime > 0 AND recent_uptime < 1),
    last_failed_scan TIMESTAMP WITH TIME ZONE,
    last_successful_scan TIMESTAMP WITH TIME ZONE,
    last_announcement TIMESTAMP WITH TIME ZONE NOT NULL,
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
);
CREATE INDEX idx_hosts_next_scan_idx ON hosts(next_scan);

CREATE TABLE account_hosts (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    next_fund TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    consecutive_failed_funds INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT account_hosts_pk PRIMARY KEY (account_id, host_id)
);
CREATE INDEX account_hosts_host_id_next_fund_idx ON account_hosts (host_id, next_fund);

CREATE TABLE hosts_blocklist (
    public_key BYTEA PRIMARY KEY CHECK (LENGTH(public_key) = 32),
    added TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    reason TEXT NOT NULL
);
CREATE INDEX hosts_blocklist_reason_idx ON hosts_blocklist (reason);

CREATE TABLE host_addresses (
    id SERIAL PRIMARY KEY,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    net_address TEXT NOT NULL,
    protocol SMALLINT NOT NULL
);
CREATE INDEX host_addresses_host_id_idx ON host_addresses (host_id);

CREATE TABLE host_resolved_cidrs (
    id SERIAL PRIMARY KEY,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    cidr CIDR NOT NULL
);
CREATE INDEX host_resolved_cidrs_host_id_idx ON host_resolved_cidrs (host_id);

CREATE TABLE syncer_peers (
    ip_address INET PRIMARY KEY,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_connect TIMESTAMP WITH TIME ZONE,
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
    chain_index BYTEA NOT NULL CHECK (LENGTH(chain_index) = 8+32),
    maturity_height INTEGER NOT NULL,
    event_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(event_id) = 32),
    event_type TEXT NOT NULL,
    event_data BYTEA NOT NULL
);
CREATE INDEX wallet_events_chain_index_idx ON wallet_events(chain_index);
CREATE INDEX wallet_events_maturity_height_id_idx ON wallet_events(maturity_height DESC, id DESC);

CREATE TABLE wallet_siacoin_elements (
    id SERIAL PRIMARY KEY,
    output_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(output_id) = 32),
    value NUMERIC(50,0) NOT NULL,
    address BYTEA NOT NULL CHECK (LENGTH(address) = 32),
    merkle_proof BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    maturity_height INTEGER NOT NULL
);

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

CREATE TABLE contracts (
  id SERIAL PRIMARY KEY,
  host_id INTEGER REFERENCES hosts(id) NOT NULL,
  contract_id BYTEA UNIQUE DEFERRABLE NOT NULL CHECK (LENGTH(contract_id) = 32),

  -- lifetime related columns
  formation TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  proof_height BIGINT NOT NULL, -- start of proof window
  expiration_height BIGINT NOT NULL, -- end of proof window
  renewed_from INTEGER REFERENCES contracts(id) UNIQUE DEFERRABLE,
  renewed_to INTEGER REFERENCES contracts(id) UNIQUE DEFERRABLE,
  revision_number INTEGER NOT NULL DEFAULT 0 CHECK(revision_number >= 0),
  state SMALLINT NOT NULL DEFAULT 0, -- 0 = 'pending', 1 = 'active', 2 = 'resolved', 3 = 'expired', 4 = 'rejected'

  -- metrics for visualization (not ACID)
  capacity BIGINT NOT NULL DEFAULT 0 CHECK(capacity >= size),
  size BIGINT NOT NULL DEFAULT 0 CHECK(size >= 0),

  -- costs
  contract_price DECIMAL(50, 0) NOT NULL, -- used to display cost of forming contract
  initial_allowance DECIMAL(50, 0) NOT NULL, -- used when refreshing contract to increase budget
  remaining_allowance DECIMAL(50, 0) NOT NULL DEFAULT 0 CHECK(remaining_allowance >= 0), -- remaining allowance
  miner_fee DECIMAL(50, 0) NOT NULL, -- miner fee added when forming/renewing contract
  used_collateral DECIMAL(50, 0) NOT NULL DEFAULT 0 CHECK(used_collateral <= total_collateral), -- collateral (allocated)
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
CREATE INDEX contracts_host_id_remaining_allowance_idx ON contracts (host_id, remaining_allowance DESC) WHERE good = true AND remaining_allowance > 0 AND state <= 1; -- for fetching contracts for funding

CREATE TABLE contract_elements (
    id SERIAL PRIMARY KEY,
    contract_id INTEGER UNIQUE NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    contract BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    merkle_proof BYTEA NOT NULL
);

CREATE TABLE slabs (
    id BIGSERIAL PRIMARY KEY, -- internal db id

    digest BYTEA UNIQUE NOT NULL CHECK(LENGTH(digest) = 32), -- unique identifier for the slab derived from sector roots

    encryption_key BYTEA NOT NULL,
    last_repair_attempt TIMESTAMP WITH TIME ZONE,
    min_shards SMALLINT NOT NULL CHECK(min_shards > 0)
);
CREATE INDEX slabs_digest_idx ON slabs(digest);

CREATE TABLE account_slabs (
    account_id INTEGER REFERENCES accounts(id) NOT NULL, -- account that owns slab
    slab_id BIGSERIAL REFERENCES slabs(id) NOT NULL,
    PRIMARY KEY (account_id, slab_id)
);

CREATE TABLE sectors (
    id BIGSERIAL PRIMARY KEY,
    sector_root BYTEA NOT NULL,

    -- uploading
    host_id INTEGER REFERENCES hosts(id), -- host that stores sector
    contract_id INTEGER REFERENCES contracts(id) DEFAULT NULL, -- null if not pinned
    uploaded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- allow sorting by upload time

    -- slab
    slab_id BIGINT REFERENCES slabs(id) NOT NULL,
    slab_index SMALLINT NOT NULL, -- index within corresponding slab to retrieve sectors in right order

    -- data integrity
    next_integrity_check TIMESTAMP WITH TIME ZONE NOT NULL,
    consecutive_failed_checks SMALLINT NOT NULL DEFAULT 0
);
-- quick lookup of sectors to pin prioritized by upload time
CREATE INDEX sectors_contract_id_uploaded_at_idx ON sectors(contract_id, uploaded_at ASC);

-- speed up fetching sectors for slab ordered by their position within the slab
CREATE UNIQUE INDEX sectors_slab_id_slab_idx ON sectors(slab_id, slab_index ASC);

-- foreign key constraint keys
CREATE INDEX sectors_host_id_idx ON sectors(host_id);
CREATE INDEX sectors_contract_id_idx ON sectors(contract_id);

-- speed up integrity check query
CREATE INDEX sectors_next_integrity_check_idx ON sectors(next_integrity_check ASC);
CREATE INDEX sectors_host_id_next_integrity_check_idx ON sectors(host_id, next_integrity_check ASC);