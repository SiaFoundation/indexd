package postgres

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/settings"
)

// PinnedSettings returns the pinned settings.
func (s *Store) PinnedSettings(ctx context.Context) (ps settings.PinnedSettings, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `SELECT TRIM(pinned_currency), pinned_min_collateral, pinned_max_storage_price, pinned_max_ingress_price, pinned_max_egress_price FROM global_settings`
		return tx.QueryRow(ctx, query).Scan(
			&ps.Currency,
			&ps.MinCollateral,
			&ps.MaxStoragePrice,
			&ps.MaxIngressPrice,
			&ps.MaxEgressPrice,
		)
	})
	return
}

// UpdatePinnedSettings updates the pinned settings.
func (s *Store) UpdatePinnedSettings(ctx context.Context, ps settings.PinnedSettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `UPDATE global_settings SET pinned_currency = $1, pinned_min_collateral = $2, pinned_max_storage_price = $3, pinned_max_ingress_price = $4, pinned_max_egress_price = $5`
		_, err := tx.Exec(ctx, query, ps.Currency, ps.MinCollateral, ps.MaxStoragePrice, ps.MaxIngressPrice, ps.MaxEgressPrice)
		return err
	})
}

// PriceSettings returns the price settings.
func (s *Store) PriceSettings(ctx context.Context) (ps settings.PriceSettings, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `SELECT max_egress_price, max_ingress_price, max_storage_price, min_collateral FROM global_settings`
		return tx.QueryRow(ctx, query).Scan(
			(*sqlCurrency)(&ps.MaxEgressPrice),
			(*sqlCurrency)(&ps.MaxIngressPrice),
			(*sqlCurrency)(&ps.MaxStoragePrice),
			(*sqlCurrency)(&ps.MinCollateral),
		)
	})
	return
}

// UpdatePriceSettings updates the price settings that can be pinned.
func (s *Store) UpdatePriceSettings(ctx context.Context, ps settings.PriceSettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `UPDATE global_settings SET max_egress_price = $1, max_ingress_price = $2, max_storage_price = $3, min_collateral = $4`
		_, err := tx.Exec(ctx, query, sqlCurrency(ps.MaxEgressPrice), sqlCurrency(ps.MaxIngressPrice), sqlCurrency(ps.MaxStoragePrice), sqlCurrency(ps.MinCollateral))
		return err
	})
}

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
