package postgres

import (
	"context"
	"fmt"

	"go.sia.tech/indexd/subscriber"
)

type updateTx struct {
	ctx context.Context
	tx  *txn
}

// ResetChainState resets the chain state in the store, clearing all
// wallet-related data, zeroing the stats derived from it, and resetting the
// scanned height and block ID in the global settings. This is typically used to
// force a resync of consensus.
func (s *Store) ResetChainState() error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(ctx, `TRUNCATE wallet_siacoin_elements`); err != nil {
			return fmt.Errorf("failed to clear wallet_siacoin_elements: %w", err)
		} else if _, err := tx.Exec(ctx, `TRUNCATE wallet_broadcasted_sets`); err != nil {
			return fmt.Errorf("failed to clear wallet_broadcasted_sets: %w", err)
		} else if _, err := tx.Exec(ctx, `TRUNCATE wallet_events`); err != nil {
			return fmt.Errorf("failed to clear wallet_events: %w", err)
		} else if _, err := tx.Exec(ctx, `DELETE FROM stats_deltas WHERE stat_name = $1`, statContractTax); err != nil {
			return fmt.Errorf("failed to clear contract tax deltas: %w", err)
		} else if _, err := tx.Exec(ctx, `UPDATE stats SET stat_value = 0 WHERE stat_name = $1`, statContractTax); err != nil {
			return fmt.Errorf("failed to reset contract tax: %w", err)
		} else if res, err := tx.Exec(ctx, `UPDATE global_settings SET scanned_height = 0, scanned_block_id = '\x0000000000000000000000000000000000000000000000000000000000000000'`); err != nil {
			return fmt.Errorf("failed to reset global_settings: %w", err)
		} else if rowsAffected := res.RowsAffected(); rowsAffected != 1 {
			return fmt.Errorf("expected to update 1 row in global_settings, got %d", rowsAffected)
		} else {
			return nil
		}
	})
}

// UpdateChainState applies a chain update to the store.
func (s *Store) UpdateChainState(fn func(tx subscriber.UpdateTx) error) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		return fn(&updateTx{ctx: ctx, tx: tx})
	})
}
