package postgres

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

var migrations = []func(context.Context, *txn, *zap.Logger) error{
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `
			CREATE TABLE stats_deltas (
				id BIGSERIAL PRIMARY KEY,
				stat_name TEXT NOT NULL REFERENCES stats(stat_name),
				stat_delta BIGINT NOT NULL
			);
		`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX stats_deltas_stat_name_idx ON stats_deltas(stat_name);`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		if _, err := tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN has_bad_quic_port BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
			return err
		}
		const query = `
UPDATE hosts SET has_bad_quic_port = TRUE
WHERE EXISTS (
	SELECT 1 FROM host_addresses
	WHERE host_id = hosts.id
	  AND protocol = $1
	  AND (substring(net_address from ':(\d+)$'))::integer IN (
		1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23,
		25, 37, 42, 43, 53, 69, 77, 79, 87, 95, 101,
		102, 103, 104, 109, 110, 111, 113, 115, 117, 119,
		123, 135, 137, 139, 143, 161, 179, 389, 427, 465,
		512, 513, 514, 515, 526, 530, 531, 532, 540, 548,
		554, 556, 563, 587, 601, 636, 989, 990, 993, 995,
		1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061,
		6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679, 6697,
		10080
	)
)`
		_, err := tx.Exec(ctx, query, networkProtocolQUIC)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		// AccountFundInterval dropped from 1h to 15m, so accounts refill 4x as
		// often. Divide existing quota targets by 4 to preserve the bytes/hour
		// rate they were sized for.
		_, err := tx.Exec(ctx, `UPDATE quotas SET fund_target_bytes = fund_target_bytes / 4;`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		if _, err := tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN has_quic BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN has_siamux BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
			return err
		}
		const setQUIC = `
UPDATE hosts SET has_quic = EXISTS (
	SELECT 1 FROM host_addresses
	WHERE host_id = hosts.id
	  AND protocol = $1
	  AND (substring(net_address from ':(\d+)$'))::integer NOT IN (
		1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23,
		25, 37, 42, 43, 53, 69, 77, 79, 87, 95, 101,
		102, 103, 104, 109, 110, 111, 113, 115, 117, 119,
		123, 135, 137, 139, 143, 161, 179, 389, 427, 465,
		512, 513, 514, 515, 526, 530, 531, 532, 540, 548,
		554, 556, 563, 587, 601, 636, 989, 990, 993, 995,
		1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061,
		6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679, 6697,
		10080
	  )
)`
		if _, err := tx.Exec(ctx, setQUIC, networkProtocolQUIC); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE hosts SET has_siamux = EXISTS (SELECT 1 FROM host_addresses WHERE host_id = hosts.id AND protocol = $1)`, networkProtocolTCPSiaMux); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `ALTER TABLE hosts DROP COLUMN has_bad_quic_port`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `DROP INDEX IF EXISTS sectors_next_integrity_check_idx`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `DROP INDEX IF EXISTS slabs_digest_idx`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		if _, err := tx.Exec(ctx, `
CREATE TABLE pools (
    id SERIAL PRIMARY KEY,
    connect_key_id INTEGER UNIQUE NOT NULL REFERENCES app_connect_keys(id) ON DELETE CASCADE,
    pool_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(pool_key) = 64)
);

CREATE TABLE pool_hosts (
    pool_id INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    host_id INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    next_fund TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    consecutive_failed_funds INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT pool_hosts_pk PRIMARY KEY (pool_id, host_id)
);
CREATE INDEX pool_hosts_host_id_next_fund_idx ON pool_hosts (host_id, next_fund);

CREATE TABLE pool_attachments (
    pool_id INTEGER NOT NULL,
    host_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    CONSTRAINT pool_attachments_pk PRIMARY KEY (pool_id, host_id, account_id),
    CONSTRAINT pool_attachments_pool_host_fk FOREIGN KEY (pool_id, host_id) REFERENCES pool_hosts(pool_id, host_id) ON DELETE CASCADE
);
CREATE INDEX pool_attachments_account_id_host_id_idx ON pool_attachments (account_id, host_id);
`); err != nil {
			return err
		}

		// backfill a pool for every existing connect key
		rows, err := tx.Query(ctx, `SELECT id FROM app_connect_keys`)
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, id := range ids {
			poolKey := types.GeneratePrivateKey()
			if _, err := tx.Exec(ctx, `INSERT INTO pools (connect_key_id, pool_key) VALUES ($1, $2)`, id, []byte(poolKey)); err != nil {
				return fmt.Errorf("failed to create pool for connect key %d: %w", id, err)
			}
		}
		return nil
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX object_events_object_key_idx ON object_events(object_key);`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		if _, err := tx.Exec(ctx, `ALTER TABLE pool_hosts ADD COLUMN sharing_attached BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `CREATE INDEX pool_hosts_sharing_unattached_idx ON pool_hosts (host_id) WHERE sharing_attached = FALSE`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS sectors_lost_idx ON sectors(id) WHERE host_id IS NULL`); err != nil {
			return err
		} else if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS sectors_contract_sectors_map_id_id_idx ON sectors(contract_sectors_map_id, id)`); err != nil {
			return err
		} else if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS slabs_id_next_repair_attempt_idx`); err != nil {
			return err
		}
		return nil
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE slab_deletion_queue (
    id BIGSERIAL PRIMARY KEY,
    slab_id BIGINT NOT NULL
);`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE slabs ADD COLUMN version SMALLINT NOT NULL DEFAULT 0 CHECK (version >= 0)`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `
DROP INDEX IF EXISTS contracts_active_host_size_idx;
CREATE INDEX contracts_active_host_size_idx ON contracts(proof_height, host_id) INCLUDE (good, capacity, size, initial_allowance, remaining_allowance) WHERE state IN (0,1) AND renewed_to IS NULL;
`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE preauthorized_keys (
    public_key BYTEA PRIMARY KEY CHECK (LENGTH(public_key) = 32),
    connect_key_id INTEGER NOT NULL REFERENCES app_connect_keys(id) ON DELETE CASCADE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    total_uses INTEGER NOT NULL CHECK (total_uses > 0),
    remaining_uses INTEGER NOT NULL CHECK (remaining_uses >= 0 AND remaining_uses <= total_uses),
    allowed_app_id BYTEA CHECK (allowed_app_id IS NULL OR LENGTH(allowed_app_id) = 32),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE
);
CREATE INDEX preauthorized_keys_connect_key_id_idx ON preauthorized_keys(connect_key_id);
CREATE INDEX preauthorized_keys_expires_at_idx ON preauthorized_keys(expires_at);
`)
		return err
	},
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE sharing_keys (
    id BIGSERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    public_key BYTEA UNIQUE NOT NULL CHECK(LENGTH(public_key) = 32),
    nonce BYTEA UNIQUE NOT NULL CHECK(LENGTH(nonce) = 32), -- share_key = HKDF(app_key, nonce, "share key")
    use_description TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE, -- optional automatic expiration
    object_count BIGINT NOT NULL CHECK(object_count >= 0), -- number of attached objects, maintained by trigger
    size BIGINT NOT NULL CHECK(size >= 0), -- total logical size of attached objects (sum of object.Size())
    pinned_data BIGINT NOT NULL CHECK(pinned_data >= 0), -- total data size of attached objects before redundancy
    pinned_size BIGINT NOT NULL CHECK(pinned_size >= 0), -- total size of attached objects including redundancy
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW() -- allow sorting by update time
);
CREATE INDEX sharing_keys_account_id_idx ON sharing_keys(account_id);
CREATE INDEX sharing_keys_expires_at_idx ON sharing_keys(expires_at);

CREATE TABLE shared_objects (
    object_id BIGINT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    sharing_key_id BIGINT NOT NULL REFERENCES sharing_keys(id) ON DELETE CASCADE,
    encrypted_data_key BYTEA UNIQUE NOT NULL CHECK(LENGTH(encrypted_data_key) = 72), -- user provided, data encryption key (xchacha20 nonce + key + tag)
    encrypted_meta_key BYTEA UNIQUE CHECK(LENGTH(encrypted_meta_key) = 72), -- user provided, metadata encryption key (xchacha20 nonce + key + tag)
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- allow sorting by update time
    encrypted_metadata BYTEA, -- user provided, encrypted metadata
    data_signature BYTEA UNIQUE NOT NULL CHECK(LENGTH(data_signature) = 64), -- signature of blake2b(object_key || encrypted_data_key)
    meta_signature BYTEA UNIQUE NOT NULL CHECK(LENGTH(meta_signature) = 64), -- signature of blake2b(object ID || metadata key || encrypted_metadata)
    size BIGINT NOT NULL, -- logical size of the object (object.Size()), captured at attach time
    pinned_data BIGINT NOT NULL, -- data size of the object before redundancy, captured at attach time
    pinned_size BIGINT NOT NULL, -- size of the object including redundancy, captured at attach time
    PRIMARY KEY (object_id, sharing_key_id)
);
CREATE INDEX shared_objects_sharing_key_id_idx ON shared_objects(sharing_key_id);

CREATE FUNCTION shared_objects_maintain_totals() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        UPDATE sharing_keys SET
            object_count = sharing_keys.object_count + agg.object_count,
            size = sharing_keys.size + agg.size,
            pinned_data = sharing_keys.pinned_data + agg.pinned_data,
            pinned_size = sharing_keys.pinned_size + agg.pinned_size,
            updated_at = NOW()
        FROM (
            SELECT sharing_key_id,
                COUNT(*) AS object_count,
                SUM(size) AS size,
                SUM(pinned_data) AS pinned_data,
                SUM(pinned_size) AS pinned_size
            FROM new_rows
            GROUP BY sharing_key_id
        ) agg
        WHERE sharing_keys.id = agg.sharing_key_id;
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE sharing_keys SET
            size = sharing_keys.size + agg.size,
            pinned_data = sharing_keys.pinned_data + agg.pinned_data,
            pinned_size = sharing_keys.pinned_size + agg.pinned_size,
            updated_at = NOW()
        FROM (
            SELECT sharing_key_id,
                SUM(size) AS size,
                SUM(pinned_data) AS pinned_data,
                SUM(pinned_size) AS pinned_size
            FROM (
                SELECT sharing_key_id, size, pinned_data, pinned_size FROM new_rows
                UNION ALL
                SELECT sharing_key_id, -size, -pinned_data, -pinned_size FROM old_rows
            ) deltas
            GROUP BY sharing_key_id
        ) agg
        WHERE sharing_keys.id = agg.sharing_key_id;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE sharing_keys SET
            object_count = sharing_keys.object_count - agg.object_count,
            size = sharing_keys.size - agg.size,
            pinned_data = sharing_keys.pinned_data - agg.pinned_data,
            pinned_size = sharing_keys.pinned_size - agg.pinned_size,
            updated_at = NOW()
        FROM (
            SELECT sharing_key_id,
                COUNT(*) AS object_count,
                SUM(size) AS size,
                SUM(pinned_data) AS pinned_data,
                SUM(pinned_size) AS pinned_size
            FROM old_rows
            GROUP BY sharing_key_id
        ) agg
        WHERE sharing_keys.id = agg.sharing_key_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER shared_objects_maintain_totals_insert
AFTER INSERT ON shared_objects
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION shared_objects_maintain_totals();

CREATE TRIGGER shared_objects_maintain_totals_update
AFTER UPDATE ON shared_objects
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION shared_objects_maintain_totals();

CREATE TRIGGER shared_objects_maintain_totals_delete
AFTER DELETE ON shared_objects
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION shared_objects_maintain_totals();
`)
		return err
	},
}
