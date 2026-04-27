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
}
