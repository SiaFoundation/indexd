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
}
