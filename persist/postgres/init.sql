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

CREATE TABLE wallet_events (
    id BYTEA PRIMARY KEY,
    chain_index BYTEA NOT NULL,
    maturity_height INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_data BYTEA NOT NULL
);
CREATE INDEX wallet_events_chain_index ON wallet_events(chain_index);
CREATE INDEX wallet_events_maturity_height ON wallet_events(maturity_height DESC);

CREATE TABLE wallet_siacoin_elements (
    id BYTEA PRIMARY KEY,
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
