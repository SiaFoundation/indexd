package postgres

import (
	"context"

	"go.sia.tech/core/types"
)

// LastScannedIndex returns the last scanned index.
func (s *Store) LastScannedIndex(ctx context.Context) (ci types.ChainIndex, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT last_scanned_index FROM global_settings`).Scan(asSiaEncoded(&ci))
	})
	return
}

// UpdateLastScannedIndex updates the last scanned index.
func (u *updateTx) UpdateLastScannedIndex(ctx context.Context, ci types.ChainIndex) error {
	_, err := u.tx.Exec(ctx, `UPDATE global_settings SET last_scanned_index = $1`, asSiaEncoded(&ci))
	return err
}
