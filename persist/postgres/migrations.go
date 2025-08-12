package postgres

import (
	"context"

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
	// add the sectors_stats table
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE TABLE sectors_stats (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
    num_slabs BIGINT NOT NULL DEFAULT 0 CHECK (num_slabs >= 0) -- total number of slabs
);`)
		return err
	},
}
