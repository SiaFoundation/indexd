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
// wallet-related data and resetting the scanned height and block ID in the
// global settings. This is typically used to force a resync of consensus.
func (s *Store) ResetChainState(ctx context.Context) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(ctx, `TRUNCATE wallet_siacoin_elements`); err != nil {
			return fmt.Errorf("failed to clear wallet_siacoin_elements: %w", err)
		} else if _, err := tx.Exec(ctx, `TRUNCATE wallet_broadcasted_sets`); err != nil {
			return fmt.Errorf("failed to clear wallet_broadcasted_sets: %w", err)
		} else if _, err := tx.Exec(ctx, `TRUNCATE wallet_events`); err != nil {
			return fmt.Errorf("failed to clear wallet_events: %w", err)
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
func (s *Store) UpdateChainState(ctx context.Context, fn func(tx subscriber.UpdateTx) error) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return fn(&updateTx{ctx: ctx, tx: tx})
	})
}
