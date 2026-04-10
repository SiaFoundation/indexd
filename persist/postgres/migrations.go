package postgres

import (
	"context"

	"go.uber.org/zap"
)

var migrations = []func(context.Context, *txn, *zap.Logger) error{
	func(ctx context.Context, tx *txn, log *zap.Logger) error {
		_, err := tx.Exec(ctx, `
			CREATE TABLE stats_delta (
				id BIGSERIAL PRIMARY KEY,
				stat_name TEXT NOT NULL,
				stat_delta BIGINT NOT NULL
			);
		`)
		return err
	},
}
