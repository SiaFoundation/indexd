package postgres

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

var migrations = []func(context.Context, *txn, *zap.Logger) error{
	// adds the app_connect_keys table
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE TABLE app_connect_keys (
    app_key TEXT PRIMARY KEY,
    use_description TEXT NOT NULL,
    remaining_uses INTEGER NOT NULL,
    total_uses INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE
);`)
		return err
	},
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX sectors_uploaded_at_unpinned_idx ON sectors(uploaded_at) WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL;`)
		return err
	},
	// adds the service_account column to accounts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE accounts ADD COLUMN service_account BOOLEAN NOT NULL DEFAULT FALSE;`)
		if err != nil {
			return err
		}
		// NOTE: the following is not perfect since a service account might not
		// yet have any rows in the service_accounts table, but it's the best we
		// can do
		_, err = tx.Exec(ctx, `UPDATE accounts SET service_account = TRUE WHERE EXISTS (SELECT 1 FROM service_accounts sa WHERE sa.account_id = accounts.id)`)
		return err
	},
	// add the sectors_stats table
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE TABLE sectors_stats (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
    num_slabs BIGINT NOT NULL DEFAULT 0 CHECK (num_slabs >= 0) -- total number of slabs
);`)
		return err
	},
	// adds the 'max_pinned_data' and 'pinned_data' columns
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE accounts ADD COLUMN pinned_data BIGINT NOT NULL DEFAULT 0 CHECK (pinned_data >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add pinned_data column: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE accounts ADD COLUMN max_pinned_data BIGINT NOT NULL CHECK (max_pinned_data >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add max_pinned_data column: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE app_connect_keys ADD COLUMN max_pinned_data BIGINT NOT NULL CHECK (max_pinned_data >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add max_pinned_data column: %w", err)
		}
		return nil
	},
	// adds the "country_code" and "location" columns
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN country_code TEXT NOT NULL DEFAULT '';`)
		if err != nil {
			return fmt.Errorf("failed to add country_code column: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN location POINT NOT NULL DEFAULT POINT(0.0, 0.0);`)
		if err != nil {
			return fmt.Errorf("failed to add location column: %w", err)
		}
		return nil
	},
	// adds the 'description', 'logo_url' and 'service_url' columns
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		for _, c := range []string{"description", "logo_url", "service_url"} {
			_, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE accounts ADD COLUMN %s TEXT NOT NULL DEFAULT '';`, c))
			if err != nil {
				return fmt.Errorf("failed to add %q column: %w", c, err)
			}
		}
		return nil
	},
	// adds the index on the "country_code" column in hosts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX hosts_country_code_idx ON hosts(country_code);`)
		return err
	},
	// add objects persistence
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE objects (
    id BIGSERIAL PRIMARY KEY,
    object_key BYTEA NOT NULL CHECK(LENGTH(object_key) = 32), -- user provided, object identifier
    account_id INTEGER REFERENCES accounts(id) NOT NULL, -- account that owns object
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- allow sorting by update time
    meta BYTEA -- user provided, encrypted metadata
);

-- object_key is unique per account
CREATE UNIQUE INDEX objects_account_id_object_key_idx ON objects(account_id, object_key);

-- fast sorting by update time and key
CREATE INDEX objects_updated_at_object_key_idx ON objects(updated_at ASC, object_key ASC);

CREATE TABLE object_slabs (
    object_id BIGINT REFERENCES objects(id) ON DELETE CASCADE,
    slab_digest BYTEA REFERENCES slabs(digest) ON DELETE CASCADE,
    slab_index INTEGER NOT NULL, -- index within corresponding slab to retrieve slabs in right order
    slab_offset INTEGER NOT NULL, -- offset within slab
    slab_length INTEGER NOT NULL, -- length of object data within slab
    PRIMARY KEY (object_id, slab_digest, slab_index)
);

-- foreign key constraint indices
-- CREATE INDEX object_slabs_object_id_idx ON object_slabs(object_id); -- covered by object_slabs_object_id_slab_index_idx
CREATE INDEX object_slabs_slab_digest_idx ON object_slabs(slab_digest);

-- speed up sorting by slab_index
CREATE INDEX object_slabs_object_id_slab_index_idx ON object_slabs(object_id, slab_index ASC);
		`)
		return err
	},
	// adds the "num_pinned_sectors" column to sectors_stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE sectors_stats ADD COLUMN num_pinned_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_pinned_sectors >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add num_pinned_sectors column: %w", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE sectors_stats
			SET num_pinned_sectors = (
				SELECT COUNT(id)
				FROM sectors
				WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NOT NULL
			)`)
		if err != nil {
			return fmt.Errorf("failed to initialize num_pinned_sectors: %w", err)
		}
		return nil
	},
	// add num_migrated sector stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE sectors ADD COLUMN num_migrated INTEGER NOT NULL DEFAULT 0;`)
		if err != nil {
			return fmt.Errorf("failed to add num_migrated column: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE sectors_stats ADD COLUMN num_migrated_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_migrated_sectors >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add num_migrated_sectors column: %w", err)
		}
		return nil
	},
	// adds the "num_unpinned_sectors" column to sectors_stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE sectors_stats ADD COLUMN num_unpinned_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_unpinned_sectors >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add num_unpinned_sectors column: %w", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE sectors_stats
			SET num_unpinned_sectors = (
				SELECT COUNT(id)
				FROM sectors
				WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL
			)`)
		if err != nil {
			return fmt.Errorf("failed to initialize num_unpinned_sectors: %w", err)
		}
		return nil
	},
	// adds the "num_unpinnable_sectors" column to sectors_stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE sectors_stats ADD COLUMN num_unpinnable_sectors BIGINT NOT NULL DEFAULT 0 CHECK (num_unpinnable_sectors >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add num_unpinnable_sectors column: %w", err)
		}
		// no need to initialize it as it's a number-go-up statistic
		return nil
	},
	// add host usage stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN usage_account_funding NUMERIC(50,0) NOT NULL DEFAULT 0;`)
		if err != nil {
			return fmt.Errorf("failed to add usage_account_funding column: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE hosts ADD COLUMN usage_total_spent NUMERIC(50,0) NOT NULL DEFAULT 0;`)
		if err != nil {
			return fmt.Errorf("failed to add usage_total_spent column: %w", err)
		}
		return nil
	},
	// adds the index on the "location" column in hosts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `
			CREATE INDEX hosts_location_gist_idx ON hosts USING GIST (location);
			CREATE INDEX contracts_host_active_idx ON contracts (host_id) WHERE state <= 1;`)
		return err
	},
	// add the account_stats table
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE sectors_stats RENAME TO stats;`)
		if err != nil {
			return fmt.Errorf("failed to rename sector stats table: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER INDEX sectors_stats_pkey RENAME TO stats_pkey;`)
		if err != nil {
			return fmt.Errorf("failed to rename sector stats index: %w", err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE stats ADD COLUMN num_accounts_registered BIGINT NOT NULL DEFAULT 0 CHECK (num_accounts_registered >= 0);`)
		if err != nil {
			return fmt.Errorf("failed to add num_accounts_registered column: %w", err)
		}
		return nil
	},
	// add expiration_height index
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `
				CREATE INDEX contracts_proof_height_idx ON contracts (proof_height);
				CREATE INDEX contracts_state_active_idx ON contracts(state) WHERE state = 0 OR state = 1;
			`)
		if err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
		return nil
	},
	// adds the "last_used" column to the accounts table and relevant index
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `ALTER TABLE accounts ADD COLUMN last_used TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();`); err != nil {
			return fmt.Errorf("failed to add last_used column: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX accounts_last_used_idx ON accounts(last_used);`); err != nil {
			return fmt.Errorf("failed to add last_used index: %w", err)
		}
		return nil
	},
	// reset registered accounts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `UPDATE stats SET num_accounts_registered = (SELECT COUNT(*) FROM accounts WHERE service_account != TRUE);`); err != nil {
			return fmt.Errorf("failed to reset num_accounts_registered: %w", err)
		}
		return nil
	},
	// drop host_resolved_cidrs table
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `DROP TABLE IF EXISTS host_resolved_cidrs;`)
		return err
	},
	// add indexes to speed up unpinning slabs
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `CREATE INDEX account_slabs_slab_id_idx ON account_slabs(slab_id);`); err != nil {
			return fmt.Errorf("failed to add account_slabs_slab_id_idx: %w", err)
		} else if _, err := tx.Exec(ctx, `CREATE UNIQUE INDEX slab_sectors_sector_id_slab_id_idx ON slab_sectors(sector_id, slab_id);`); err != nil {
			return fmt.Errorf("failed to add slab_sectors_sector_id_slab_id_idx: %w", err)
		}
		return nil
	},
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		// note: it is not practical to migrate the existing object data since
		// the master key column does not exist.
		if _, err := tx.Exec(ctx, `TRUNCATE objects CASCADE;`); err != nil {
			return fmt.Errorf("failed to drop objects table: %w", err)
		}
		const query = `
ALTER TABLE objects DROP COLUMN meta;
ALTER TABLE objects ADD COLUMN encrypted_master_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(encrypted_master_key) = 72); -- user provided, master encryption key (xchacha20 nonce + key + tag)
ALTER TABLE objects ADD COLUMN encrypted_metadata BYTEA; -- user provided, encrypted metadata
ALTER TABLE objects ADD COLUMN signature BYTEA UNIQUE NOT NULL CHECK (LENGTH(signature) = 64); -- signature of blake2b(object_key || encrypted_master_key || encrypted_metadata)`
		_, err := tx.Exec(ctx, query)
		return err
	},
	// add indices to support host stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX hosts_usage_total_spent_idx ON hosts(usage_total_spent DESC);`)
		if err != nil {
			return fmt.Errorf("failed to create hosts_usage_total_spent_idx index: %w", err)
		}
		_, err = tx.Exec(ctx, `CREATE INDEX contracts_active_host_size_idx ON contracts(proof_height, host_id) INCLUDE (size) WHERE (state = 0 OR state = 1) AND renewed_to IS NULL;`)
		if err != nil {
			return fmt.Errorf("failed to create contracts_active_host_size_idx index: %w", err)
		}
		return nil
	},
	// drop index 'contracts_state_active_idx' and recreate it
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `DROP INDEX contracts_state_active_idx;`); err != nil {
			return fmt.Errorf("failed to drop index: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX contracts_state_active_idx ON contracts(state) WHERE (state = 0 OR state = 1) AND renewed_to IS NULL;`); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
		return nil
	},
	// reset registered accounts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `UPDATE stats SET num_accounts_registered = (SELECT COUNT(*) FROM accounts);`); err != nil {
			return fmt.Errorf("failed to reset num_accounts_registered: %w", err)
		}
		return nil
	},
	// changes the pinning ordering to prefer contracts with available capacity
	func(ctx context.Context, t *txn, _ *zap.Logger) error {
		const query = `
DROP INDEX IF EXISTS contracts_capacity_size_contract_id_idx;
CREATE INDEX contracts_capacity_size_contract_id_idx ON contracts (host_id, (capacity - size) DESC, size) WHERE good = true AND state <= 1 AND remaining_allowance > 0;`

		_, err := t.Exec(ctx, query)
		return err
	},
	// changes the pinning ordering to prefer contracts with available capacity
	func(ctx context.Context, t *txn, _ *zap.Logger) error {
		const query = `
DROP INDEX IF EXISTS contracts_capacity_size_contract_id_idx;
CREATE INDEX contracts_capacity_size_contract_id_idx ON contracts (host_id, (capacity - size) DESC, size) WHERE good = true AND state <= 1 AND remaining_allowance > 0;`

		_, err := t.Exec(ctx, query)
		return err
	},
	// create object events table and inserting existing objects
	func(ctx context.Context, t *txn, _ *zap.Logger) error {
		_, err := t.Exec(ctx, `CREATE TABLE object_events (
    object_key BYTEA NOT NULL CHECK(LENGTH(object_key) = 32), -- not a FK since deletions need to hang around
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    was_deleted BOOLEAN NOT NULL, -- true if deleted, false otherwise
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- last time the object was created/updated/deleted
    PRIMARY KEY (account_id, object_key)
);`)
		if err != nil {
			return fmt.Errorf("failed to create object events table: %w", err)
		}

		_, err = t.Exec(ctx, `
INSERT INTO object_events (object_key, account_id, was_deleted, updated_at)
SELECT o.object_key,
       o.account_id,
       FALSE,
       o.updated_at
FROM objects o;
`)
		if err != nil {
			return fmt.Errorf("failed to insert object events: %w", err)
		}

		_, err = t.Exec(ctx, `CREATE INDEX object_events_updated_at_object_key_idx ON object_events(updated_at ASC, object_key ASC);`)
		if err != nil {
			return fmt.Errorf("failed to create object event index: %w", err)
		}

		_, err = t.Exec(ctx, `DROP INDEX objects_updated_at_object_key_idx;`)
		if err != nil {
			return fmt.Errorf("failed to drop old index: %w", err)
		}

		return nil
	},
	// recreate all contracts indices
	func(ctx context.Context, tx *txn, l *zap.Logger) error {
		if _, err := tx.Exec(ctx, `
			DROP INDEX contracts_state_formation_idx;
			DROP INDEX contracts_state_good_idx;
			DROP INDEX contracts_last_broadcast_attempt_contract_id_idx;
			DROP INDEX contracts_host_id_remaining_allowance_contract_id_idx;
			DROP INDEX contracts_capacity_size_contract_id_idx;
			DROP INDEX contracts_proof_height_idx;
			DROP INDEX contracts_state_active_idx;
			DROP INDEX contracts_active_host_size_idx;
			DROP INDEX contracts_host_active_idx;
		`); err != nil {
			return fmt.Errorf("failed to drop contracts indices: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			CREATE INDEX contracts_active_host_size_idx ON contracts(proof_height, host_id) INCLUDE (good, capacity, size) WHERE state IN (0,1) AND renewed_to IS NULL;
			CREATE INDEX contracts_host_id_active_good_idx ON contracts(host_id) WHERE state IN (0,1) AND renewed_to IS NULL AND good;
			CREATE INDEX contracts_host_id_active_bad_idx ON contracts(host_id) WHERE state IN (0,1) AND renewed_to IS NULL AND NOT good;
			CREATE INDEX contracts_host_id_inactive_good_idx ON contracts (host_id) WHERE state IN (2,3,4) AND good;
			CREATE INDEX contracts_host_id_inactive_bad_idx ON contracts (host_id) WHERE state IN (2,3,4) AND NOT good;
			CREATE INDEX contracts_last_broadcast_attempt_active_idx ON contracts (last_broadcast_attempt ASC, contract_id) WHERE state IN (0,1) AND renewed_to IS NULL;
			CREATE INDEX contracts_host_id_remaining_allowance_active_idx ON contracts (host_id, remaining_allowance DESC, contract_id) WHERE state IN (0,1) AND renewed_to IS NULL AND good AND remaining_allowance > 0;
			CREATE INDEX contracts_size_contract_id_idx ON contracts (host_id, size DESC, contract_id) INCLUDE(next_prune) WHERE state IN (0,1) AND renewed_to IS NULL AND good AND remaining_allowance > 0;
			CREATE INDEX contracts_formation_pending_idx ON contracts(formation) WHERE state = 0;
		`); err != nil {
			return fmt.Errorf("failed to create contracts indices: %w", err)
		}
		return nil
	},
	// reset stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `
		WITH counts AS (
			SELECT
				COUNT(*) FILTER (WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NOT NULL)::bigint AS pinned,
				COUNT(*) FILTER (WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL)::bigint     AS unpinned,
				COUNT(*) FILTER (WHERE host_id IS NULL     AND contract_sectors_map_id IS NULL)::bigint     AS unpinnable
			FROM sectors
		)
		UPDATE stats s
		SET
			num_pinned_sectors     = counts.pinned,
			num_unpinned_sectors   = counts.unpinned,
			num_unpinnable_sectors = counts.unpinnable
		FROM counts`); err != nil {
			return fmt.Errorf("failed to reset sector stats: %w", err)
		}
		return nil
	},
	// reset stats
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		if _, err := tx.Exec(ctx, `
		WITH counts AS (
			SELECT
				COUNT(*) FILTER (WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NOT NULL)::bigint AS pinned,
				COUNT(*) FILTER (WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL)::bigint     AS unpinned,
				COUNT(*) FILTER (WHERE host_id IS NULL     AND contract_sectors_map_id IS NULL)::bigint     AS unpinnable
			FROM sectors
		)
		UPDATE stats s
		SET
			num_pinned_sectors     = counts.pinned,
			num_unpinned_sectors   = counts.unpinned,
			num_unpinnable_sectors = counts.unpinnable
		FROM counts`); err != nil {
			return fmt.Errorf("failed to reset sector stats: %w", err)
		}
		return nil
	},
	// migrate hosts_blocklist reason column to TEXT[] array and add GIN index
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `
			-- add reasons column and migrate data
			ALTER TABLE hosts_blocklist ADD COLUMN reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];
			UPDATE hosts_blocklist SET reasons = ARRAY[reason];
			
			-- drop old column and create index
			DROP INDEX hosts_blocklist_reason_idx;
			ALTER TABLE hosts_blocklist DROP COLUMN reason;
			CREATE INDEX hosts_blocklist_reasons_gin_idx ON hosts_blocklist USING GIN(reasons);
		`)
		return err
	},
	// add index to speed up contract elements for broadcasting
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX contracts_expiration_height_contract_id_idx ON contracts (expiration_height, contract_id) WHERE state = 1 AND renewed_to IS NULL;`)
		return err
	},
	// add wallet_hash to global_settings to detect seed changes
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE global_settings ADD COLUMN wallet_hash BYTEA CHECK(wallet_hash IS NULL OR LENGTH(wallet_hash) = 32);`)
		return err
	},
	// add consecutive_failed_repairs and next_repair_attempt to slabs
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `
			ALTER TABLE slabs DROP COLUMN last_repair_attempt;
			ALTER TABLE slabs ADD COLUMN consecutive_failed_repairs SMALLINT NOT NULL DEFAULT 0 CHECK (consecutive_failed_repairs >= 0);
			ALTER TABLE slabs ADD COLUMN next_repair_attempt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
			CREATE INDEX slabs_id_next_repair_attempt_idx ON slabs(next_repair_attempt ASC);
			DROP INDEX slab_sectors_sector_id_idx;
		`)
		return err
	},
	// add connect_key to accounts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `
ALTER TABLE app_connect_keys RENAME TO app_connect_keys_tmp;
ALTER TABLE app_connect_keys_tmp DROP CONSTRAINT app_connect_keys_pkey;

CREATE TABLE app_connect_keys (
    id SERIAL PRIMARY KEY,
    app_key TEXT UNIQUE NOT NULL,
    use_description TEXT NOT NULL,
    remaining_uses INTEGER NOT NULL,
    total_uses INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE,
    max_pinned_data BIGINT NOT NULL CHECK (max_pinned_data >= 0)
);
INSERT INTO app_connect_keys (app_key, use_description, remaining_uses, total_uses, created_at, updated_at, last_used, max_pinned_data) SELECT app_key, use_description, remaining_uses, total_uses, created_at, updated_at, last_used, max_pinned_data FROM app_connect_keys_tmp;
DROP TABLE app_connect_keys_tmp;

-- add connect key column and create index
ALTER TABLE accounts ADD COLUMN connect_key_id INTEGER REFERENCES app_connect_keys(id);
CREATE INDEX accounts_connect_key_id_idx ON accounts(connect_key_id);
`)
		return err
	},
}
