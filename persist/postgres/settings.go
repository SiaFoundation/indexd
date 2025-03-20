package postgres

import (
	"context"

	"go.sia.tech/core/types"
)

// LastScannedIndex returns the last scanned index.
func (s *Store) LastScannedIndex(ctx context.Context) (ci types.ChainIndex, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT scanned_height, scanned_block_id FROM global_settings`).Scan(&ci.Height, (*sqlHash256)(&ci.ID))
	})
	return
}

// UpdateLastScannedIndex updates the last scanned index.
func (u *updateTx) UpdateLastScannedIndex(ctx context.Context, ci types.ChainIndex) error {
	_, err := u.tx.Exec(ctx, `UPDATE global_settings SET scanned_height = $1, scanned_block_id = $2`, ci.Height, sqlHash256(ci.ID))
	return err
}
