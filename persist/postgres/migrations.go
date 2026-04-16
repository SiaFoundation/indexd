package postgres

import (
	"context"

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
}
