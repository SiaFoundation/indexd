CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32)
);

CREATE TABLE hosts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32),
    consecutive_failed_scans INTEGER NOT NULL DEFAULT 0,
    recent_uptime DOUBLE PRECISION NOT NULL DEFAULT 0.9 CHECK (recent_uptime > 0 AND recent_uptime < 1),
    last_integrity_check TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
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
    settings_max_contract_duration BIGINT NOT NULL DEFAULT 0,
    settings_remaining_storage BIGINT NOT NULL DEFAULT 0,
    settings_total_storage BIGINT NOT NULL DEFAULT 0,
    settings_contract_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_collateral NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_storage_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_ingress_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_egress_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_free_sector_price NUMERIC(50,0) NOT NULL DEFAULT 0,
    settings_tip_height BIGINT NOT NULL DEFAULT 0,
    settings_valid_until TIMESTAMP WITH TIME ZONE,
    settings_signature BYTEA NOT NULL DEFAULT '\x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'::bytea CHECK (LENGTH(settings_signature) = 64)
);
CREATE INDEX hosts_next_scan_idx ON hosts(next_scan);

CREATE INDEX hosts_last_integrity_check_idx ON hosts(last_integrity_check ASC);

CREATE TABLE account_hosts (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    next_fund TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    consecutive_failed_funds INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT account_hosts_pk PRIMARY KEY (account_id, host_id)
);
CREATE INDEX account_hosts_host_id_next_fund_idx ON account_hosts (host_id, next_fund);

CREATE TABLE service_accounts (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    balance NUMERIC(50,0) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    CONSTRAINT service_accounts_pk PRIMARY KEY (account_id, host_id)
);

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
    ip_address INET NOT NULL,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_connect TIMESTAMP WITH TIME ZONE,
    synced_blocks INTEGER NOT NULL DEFAULT 0 CHECK (synced_blocks >= 0),
    sync_duration INTEGER NOT NULL DEFAULT 0 CHECK (sync_duration >= 0),
    PRIMARY KEY (ip_address, port)
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

CREATE TABLE wallet_locked_utxos (
    id SERIAL PRIMARY KEY,
    output_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(output_id) = 32),
    unlock_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX wallet_locked_utxos_unlock_at_idx ON wallet_locked_utxos(unlock_at);

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
    hosts_min_protocol_version BYTEA, -- used for host checks
    hosts_min_collateral NUMERIC(50,0), -- hastings / byte / block
    hosts_max_storage_price NUMERIC(50,0), -- hastings / byte / block
    hosts_max_ingress_price NUMERIC(50,0), -- hastings / byte
    hosts_max_egress_price NUMERIC(50,0), -- hastings / byte

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
  contract_id BYTEA NOT NULL UNIQUE CHECK (LENGTH(contract_id) = 32),

  -- lifetime related columns
  formation TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  proof_height BIGINT NOT NULL, -- start of proof window
  expiration_height BIGINT NOT NULL, -- end of proof window
  renewed_from BYTEA UNIQUE REFERENCES contracts(contract_id),
  renewed_to BYTEA UNIQUE REFERENCES contracts(contract_id),
  revision_number INTEGER NOT NULL DEFAULT 0 CHECK(revision_number >= 0),
  state SMALLINT NOT NULL DEFAULT 0, -- 0 = 'pending', 1 = 'active', 2 = 'resolved', 3 = 'expired', 4 = 'rejected'

  -- revision broadcast related columns
  last_broadcast_attempt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

  -- contract pruning
  next_prune TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW() + INTERVAL '1 day',

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
  sector_roots_spending DECIMAL(50, 0) NOT NULL DEFAULT 0,

  -- raw contract revision data
  raw_revision BYTEA NOT NULL
);
CREATE INDEX contracts_state_formation_idx ON contracts(state, formation); -- for rejecting expired contracts
CREATE INDEX contracts_state_good_idx ON contracts(state) WHERE state <= 1 AND good; -- for filtering contracts
CREATE INDEX contracts_last_broadcast_attempt_contract_id_idx ON contracts (last_broadcast_attempt ASC, contract_id) WHERE renewed_to IS NULL; -- for fetching contracts for broadcasting
CREATE INDEX contracts_host_id_remaining_allowance_contract_id_idx ON contracts (host_id, remaining_allowance DESC, contract_id) WHERE good = true AND remaining_allowance > 0; -- for fetching contracts for funding
CREATE INDEX contracts_capacity_size_contract_id_idx ON contracts (capacity DESC, size DESC, contract_id) WHERE good = true AND remaining_allowance > 0; -- for fetching contracts for pinning

-- foreign key constraint index
CREATE INDEX contracts_host_id_idx ON contracts(host_id);

CREATE TABLE contract_sectors_map (
    id SERIAL PRIMARY KEY,
    contract_id BYTEA UNIQUE REFERENCES contracts(contract_id) NOT NULL
);
CREATE INDEX contract_sectors_map_contract_id_idx ON contract_sectors_map(contract_id);

CREATE TABLE contract_elements (
    id SERIAL PRIMARY KEY,
    contract_id BYTEA NOT NULL UNIQUE REFERENCES contracts(contract_id) ON DELETE CASCADE,
    contract BYTEA NOT NULL,
    leaf_index INTEGER NOT NULL,
    merkle_proof BYTEA NOT NULL
);

CREATE TABLE slabs (
    id BIGSERIAL PRIMARY KEY, -- internal db id

    digest BYTEA UNIQUE NOT NULL CHECK(LENGTH(digest) = 32), -- unique identifier for the slab derived from sector roots

    encryption_key BYTEA NOT NULL,
    last_repair_attempt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    min_shards SMALLINT NOT NULL CHECK(min_shards > 0)
);
CREATE INDEX slabs_digest_idx ON slabs(digest);

-- speeds up lookup of unhealthy slabs
CREATE INDEX slabs_id_last_repair_attempt_idx ON slabs(last_repair_attempt ASC);

CREATE TABLE account_slabs (
    account_id INTEGER REFERENCES accounts(id) NOT NULL, -- account that owns slab
    slab_id BIGSERIAL REFERENCES slabs(id) NOT NULL,
    PRIMARY KEY (account_id, slab_id)
);

CREATE TABLE sectors (
    id BIGSERIAL PRIMARY KEY,
    sector_root BYTEA UNIQUE NOT NULL,

    -- uploading
    -- NOTE: contract_sectors_map_id should always be NULL when host_id is NULL
    host_id INTEGER REFERENCES hosts(id), -- host that stores sector
    contract_sectors_map_id INTEGER REFERENCES contract_sectors_map(id) DEFAULT NULL CHECK((host_id IS NULL AND contract_sectors_map_id IS NULL) OR host_id IS NOT NULL), -- null if not pinned
    uploaded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- allow sorting by upload time

    -- data integrity
    next_integrity_check TIMESTAMP WITH TIME ZONE NOT NULL,
    consecutive_failed_checks SMALLINT NOT NULL DEFAULT 0
);

-- quick lookup of sectors that failed the integrity checks too many times
CREATE INDEX sectors_consecutive_failed_checks_idx ON sectors(host_id, consecutive_failed_checks) WHERE consecutive_failed_checks > 0;

-- quick lookup of sectors to pin prioritized by upload time
CREATE INDEX sectors_contract_sectors_map_id_uploaded_at_idx ON sectors(contract_sectors_map_id, uploaded_at ASC);

-- speed up lookup of unpinned sectors
CREATE INDEX sectors_host_id_uploaded_at_idx ON sectors(host_id, uploaded_at ASC) WHERE contract_sectors_map_id IS NULL;

-- speed up prunable roots check
CREATE INDEX sectors_contract_sectors_map_id_sector_root_idx ON sectors(contract_sectors_map_id, sector_root);

-- foreign key constraint keys
CREATE INDEX sectors_host_id_idx ON sectors(host_id);
-- CREATE INDEX sectors_contract_sectors_map_id_idx ON sectors(contract_sectors_map_id); -- covered by sectors_contract_sectors_map_id_uploaded_at_idx

-- speed up integrity check query
CREATE INDEX sectors_next_integrity_check_idx ON sectors(next_integrity_check ASC);
CREATE INDEX sectors_host_id_next_integrity_check_idx ON sectors(host_id, next_integrity_check ASC);

-- speed up hosts for pinning query
CREATE INDEX sectors_host_id_null_contract_map_idx ON sectors(host_id) WHERE contract_sectors_map_id IS NULL;

-- speed up querying sectors of a host by root
CREATE INDEX sectors_host_id_sector_root_idx ON sectors(host_id, sector_root);

CREATE TABLE slab_sectors (
    slab_id BIGINT REFERENCES slabs(id) ON DELETE CASCADE,
    sector_id BIGINT REFERENCES sectors(id) ON DELETE CASCADE,
    slab_index SMALLINT NOT NULL, -- index within corresponding slab to retrieve sectors in right order
    PRIMARY KEY (slab_id, sector_id)
);

-- speeds up lookup of unhealthy slabs
CREATE INDEX slab_sectors_sector_id_idx ON slab_sectors(sector_id);

-- speed up fetching sectors for slab ordered by their position within the slab
CREATE UNIQUE INDEX slab_sectors_slab_id_slab_index_idx ON slab_sectors(slab_id, slab_index ASC);
