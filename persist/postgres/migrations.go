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
}
