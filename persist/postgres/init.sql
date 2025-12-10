CREATE TABLE app_connect_keys (
    id SERIAL PRIMARY KEY,
    user_secret BYTEA UNIQUE NOT NULL CHECK (LENGTH(user_secret) = 32),
    app_key TEXT UNIQUE NOT NULL,
    use_description TEXT NOT NULL,
    remaining_uses INTEGER NOT NULL,
    total_uses INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE,
    pinned_data BIGINT NOT NULL DEFAULT 0 CHECK (pinned_data >= 0), -- total pinned data in bytes
    max_pinned_data BIGINT NOT NULL CHECK (max_pinned_data >= 0)
);

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32),
    connect_key_id INTEGER REFERENCES app_connect_keys(id),

    pinned_data BIGINT NOT NULL DEFAULT 0 CHECK (pinned_data >= 0), -- total pinned data in bytes
    max_pinned_data BIGINT NOT NULL CHECK (max_pinned_data >= 0), -- max pinned data in bytes
    app_id BYTEA NOT NULL DEFAULT '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea CHECK (LENGTH(app_id) = 32), -- app identifier
    description TEXT NOT NULL DEFAULT '',
    logo_url TEXT NOT NULL DEFAULT '',
    service_url TEXT NOT NULL DEFAULT '',
    last_used TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX accounts_last_used_idx ON accounts(last_used);
CREATE INDEX accounts_deleted_at_idx ON accounts(deleted_at);
CREATE INDEX accounts_connect_key_id_idx ON accounts(connect_key_id);

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
    country_code TEXT NOT NULL DEFAULT '',
    location POINT NOT NULL DEFAULT POINT(0.0, 0.0),
    lost_sectors INTEGER NOT NULL DEFAULT 0,

    scans INTEGER NOT NULL DEFAULT 0 CHECK (scans >= 0),
    scans_failed INTEGER NOT NULL DEFAULT 0 CHECK (scans_failed >= 0),

    usage_account_funding NUMERIC(50,0) NOT NULL DEFAULT 0,
    usage_total_spent NUMERIC(50,0) NOT NULL DEFAULT 0,

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
CREATE INDEX hosts_lost_sectors_idx ON hosts(lost_sectors);
CREATE INDEX hosts_usage_total_spent_idx ON hosts(usage_total_spent DESC);

-- speed up querying by country
CREATE INDEX hosts_country_code_idx ON hosts(country_code);

-- speed up ordering by distance to a location
CREATE INDEX hosts_location_gist_idx ON hosts USING GIST (location);

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
    reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
);
CREATE INDEX hosts_blocklist_reasons_gin_idx ON hosts_blocklist USING GIN(reasons);

CREATE TABLE host_addresses (
    id SERIAL PRIMARY KEY,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    net_address TEXT NOT NULL,
    protocol SMALLINT NOT NULL
);
CREATE INDEX host_addresses_host_id_idx ON host_addresses (host_id);

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

CREATE TABLE wallet_broadcasted_sets (
    id SERIAL PRIMARY KEY,
    chain_index BYTEA NOT NULL CHECK (LENGTH(chain_index) = 8+32),
    set_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(set_id) = 32),
    transactions BYTEA NOT NULL,
    broadcasted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

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

    -- wallet info
    wallet_hash BYTEA CHECK(wallet_hash IS NULL OR LENGTH(wallet_hash) = 32), -- used to prevent wallet seed changes

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

-- foreign key constraint index
CREATE INDEX contracts_host_id_idx ON contracts(host_id);

-- fetching contracts statistics
CREATE INDEX contracts_active_host_size_idx ON contracts(proof_height, host_id) INCLUDE (good, capacity, size) WHERE state IN (0,1) AND renewed_to IS NULL;

 -- listing contracts
CREATE INDEX contracts_host_id_active_good_idx ON contracts(host_id) WHERE state IN (0,1) AND renewed_to IS NULL AND good;
CREATE INDEX contracts_host_id_active_bad_idx ON contracts(host_id) WHERE state IN (0,1) AND renewed_to IS NULL AND NOT good;
CREATE INDEX contracts_host_id_inactive_good_idx ON contracts (host_id) WHERE state IN (2,3,4) AND good;
CREATE INDEX contracts_host_id_inactive_bad_idx ON contracts (host_id) WHERE state IN (2,3,4) AND NOT good;

-- contracts for broadcasting
CREATE INDEX contracts_last_broadcast_attempt_active_idx ON contracts (last_broadcast_attempt ASC, contract_id) WHERE state IN (0,1) AND renewed_to IS NULL;

-- contract elements for broadcasting
CREATE INDEX contracts_expiration_height_contract_id_idx ON contracts (expiration_height, contract_id) WHERE state = 1 AND renewed_to IS NULL;

-- contracts for funding
CREATE INDEX contracts_host_id_remaining_allowance_active_idx ON contracts (host_id, remaining_allowance DESC, contract_id) WHERE state IN (0,1) AND renewed_to IS NULL AND good AND remaining_allowance > 0;

-- contracts for pruning
CREATE INDEX contracts_size_contract_id_idx ON contracts (host_id, size DESC, contract_id) INCLUDE(next_prune) WHERE state IN (0,1) AND renewed_to IS NULL AND good AND remaining_allowance > 0;

-- hosts for pruning
CREATE INDEX contracts_next_prune_host_id_idx ON contracts (next_prune, host_id) WHERE state IN (0,1) AND renewed_to IS NULL AND good;

-- rejecting contracts
CREATE INDEX contracts_formation_pending_idx ON contracts(formation) WHERE state = 0;

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

    pinned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- allow sorting by pinned time
    digest BYTEA UNIQUE NOT NULL CHECK(LENGTH(digest) = 32), -- unique identifier for the slab derived from sector roots

    encryption_key BYTEA NOT NULL,
    min_shards SMALLINT NOT NULL CHECK(min_shards > 0),

    consecutive_failed_repairs SMALLINT NOT NULL DEFAULT 0 CHECK (consecutive_failed_repairs >= 0),
    next_repair_attempt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()

);
CREATE INDEX slabs_digest_idx ON slabs(digest);
CREATE INDEX slabs_pinned_at_idx ON slabs(pinned_at ASC);

-- speeds up lookup of unhealthy slabs
CREATE INDEX slabs_id_next_repair_attempt_idx ON slabs(next_repair_attempt ASC);

CREATE TABLE objects (
    id BIGSERIAL PRIMARY KEY,
    object_key BYTEA NOT NULL CHECK(LENGTH(object_key) = 32),
    encrypted_data_key BYTEA UNIQUE NOT NULL CHECK(LENGTH(encrypted_data_key) = 72), -- user provided, data encryption key (xchacha20 nonce + key + tag)
    encrypted_meta_key BYTEA UNIQUE CHECK(LENGTH(encrypted_meta_key) = 72), -- user provided, metadata encryption key (xchacha20 nonce + key + tag)
    account_id INTEGER REFERENCES accounts(id) NOT NULL, -- account that owns object
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- allow sorting by update time
    encrypted_metadata BYTEA, -- user provided, encrypted metadata
    data_signature BYTEA UNIQUE NOT NULL CHECK(LENGTH(data_signature) = 64), -- signature of blake2b(object_key || encrypted_data_key)
    meta_signature BYTEA UNIQUE NOT NULL CHECK(LENGTH(meta_signature) = 64) -- signature of blake2b(object ID || metadata key || encrypted_metadata)
);

-- object_key is unique per account
CREATE UNIQUE INDEX objects_account_id_object_key_idx ON objects(account_id, object_key);

CREATE TABLE object_slabs (
    object_id BIGINT REFERENCES objects(id) ON DELETE CASCADE,
    slab_digest BYTEA REFERENCES slabs(digest) ON DELETE CASCADE,
    slab_index INTEGER NOT NULL, -- index within corresponding object to retrieve slabs in right order
    slab_offset INTEGER NOT NULL, -- offset within slab
    slab_length INTEGER NOT NULL, -- length of object data within slab
    PRIMARY KEY (object_id, slab_digest, slab_index)
);

-- foreign key constraint indices
-- CREATE INDEX object_slabs_object_id_idx ON object_slabs(object_id); -- covered by object_slabs_object_id_slab_index_idx
CREATE INDEX object_slabs_slab_digest_idx ON object_slabs(slab_digest);

-- speed up sorting by slab_index
CREATE INDEX object_slabs_object_id_slab_index_idx ON object_slabs(object_id, slab_index ASC);

CREATE TABLE object_events (
    object_key BYTEA NOT NULL CHECK(LENGTH(object_key) = 32), -- not a FK since deletions need to hang around
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    was_deleted BOOLEAN NOT NULL, -- true if deleted, false otherwise
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- last time the object was created/updated/deleted
    PRIMARY KEY (account_id, object_key)
);

-- fast sorting by update time and key
CREATE INDEX object_events_updated_at_object_key_idx ON object_events(updated_at ASC, object_key ASC);

CREATE TABLE account_slabs (
    account_id INTEGER REFERENCES accounts(id) NOT NULL, -- account that owns slab
    slab_id BIGSERIAL REFERENCES slabs(id) NOT NULL,
    PRIMARY KEY (account_id, slab_id)
);

-- speed up query used when unpinning slabs
CREATE INDEX account_slabs_slab_id_idx ON account_slabs(slab_id);

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
    consecutive_failed_checks SMALLINT NOT NULL DEFAULT 0,

    -- statistics
    num_migrated INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE stats (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
    -- sector stats
    num_slabs BIGINT NOT NULL DEFAULT 0 CHECK (num_slabs >= 0), -- total number of slabs
    num_migrated_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_migrated_sectors >= 0), -- total number of migrated sectors
    num_pinned_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_pinned_sectors >= 0), -- total number of pinned sectors
    num_unpinnable_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_unpinnable_sectors >= 0), -- total number of unpinnable sectors
    num_unpinned_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_unpinned_sectors >= 0), -- total number of unpinned sectors

    num_sectors_checked BIGINT NOT NULL DEFAULT 0 CHECK (num_sectors_checked >= 0),
    num_sectors_lost BIGINT NOT NULL DEFAULT 0 CHECK (num_sectors_lost >= 0),
    num_sectors_check_failed BIGINT NOT NULL DEFAULT 0 CHECK (num_sectors_check_failed >= 0),
    -- account stats
    num_accounts_registered BIGINT NOT NULL DEFAULT 0 CHECK (num_accounts_registered >= 0), -- number of accounts currently registered
    -- host scan stats
    num_scans BIGINT NOT NULL DEFAULT 0 CHECK (num_scans >= 0), -- total number of performed host scans
    num_scans_failed BIGINT NOT NULL DEFAULT 0 CHECK (num_scans_failed >= 0) -- total number of failed host scans
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

-- for pruning unpinned sectors
CREATE INDEX sectors_uploaded_at_unpinned_idx ON sectors(uploaded_at) WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL;

CREATE TABLE slab_sectors (
    slab_id BIGINT REFERENCES slabs(id) ON DELETE CASCADE,
    sector_id BIGINT REFERENCES sectors(id) ON DELETE CASCADE,
    slab_index SMALLINT NOT NULL, -- index within corresponding slab to retrieve sectors in right order
    PRIMARY KEY (slab_id, sector_id)
);

-- speed up fetching sectors for slab ordered by their position within the slab
CREATE UNIQUE INDEX slab_sectors_slab_id_slab_index_idx ON slab_sectors(slab_id, slab_index ASC);

-- speeds up finding sectors for deletion when unpinning slabs
CREATE UNIQUE INDEX slab_sectors_sector_id_slab_id_idx ON slab_sectors(sector_id, slab_id);
