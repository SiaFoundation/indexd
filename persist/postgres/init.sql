CREATE TABLE hello_world ( -- TODO: remove
    sc_value NUMERIC(50, 0) NOT NULL
);

CREATE TABLE syncer_peers (
    peer_address INET PRIMARY KEY,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE syncer_bans (
    net_cidr CIDR PRIMARY KEY,
    expiration TIMESTAMP WITH TIME ZONE NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX syncer_bans_expiration_index_idx ON syncer_bans (expiration);
CREATE INDEX syncer_bans_net_cidr_gist_idx ON syncer_bans USING gist (net_cidr inet_ops); -- fast subnet matches

CREATE TABLE global_settings (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
    db_version INTEGER NOT NULL, -- used for migrations
    last_scanned_index BYTEA -- chain index of the last scanned block
);
