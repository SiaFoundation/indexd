package postgres

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/pins"
)

// PinnedSettings returns the pinned settings.
func (s *Store) PinnedSettings(ctx context.Context) (ps pins.PinnedSettings, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `SELECT pins_currency, pins_min_collateral, pins_max_storage_price, pins_max_ingress_price, pins_max_egress_price FROM global_settings`
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
func (s *Store) UpdatePinnedSettings(ctx context.Context, ps pins.PinnedSettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `UPDATE global_settings SET pins_currency = $1, pins_min_collateral = $2, pins_max_storage_price = $3, pins_max_ingress_price = $4, pins_max_egress_price = $5`
		_, err := tx.Exec(ctx, query, ps.Currency, ps.MinCollateral, ps.MaxStoragePrice, ps.MaxIngressPrice, ps.MaxEgressPrice)
		return err
	})
}

// UpdateMaintenanceSettings updates the maintenance settings.
func (s *Store) UpdateMaintenanceSettings(ctx context.Context, settings contracts.MaintenanceSettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE global_settings SET contracts_maintenance_enabled = $1, contracts_wanted = $2, contracts_renew_window = $3, contracts_period = $4`,
			settings.Enabled, settings.WantedContracts, settings.RenewWindow, settings.Period)
		return err
	})
}

// UsabilitySettings returns the usability settings used in the host's usability checks.
func (s *Store) UsabilitySettings(ctx context.Context) (us hosts.UsabilitySettings, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `SELECT hosts_max_egress_price, hosts_max_ingress_price, hosts_max_storage_price, hosts_min_collateral, hosts_min_protocol_version FROM global_settings`
		return tx.QueryRow(ctx, query).Scan(
			(*sqlCurrency)(&us.MaxEgressPrice),
			(*sqlCurrency)(&us.MaxIngressPrice),
			(*sqlCurrency)(&us.MaxStoragePrice),
			(*sqlCurrency)(&us.MinCollateral),
			(*sqlProtocolVersion)(&us.MinProtocolVersion),
		)
	})
	return
}

// UpdateUsabilitySettings updates the usability settings.
func (s *Store) UpdateUsabilitySettings(ctx context.Context, us hosts.UsabilitySettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `UPDATE global_settings SET hosts_max_egress_price = $1, hosts_max_ingress_price = $2, hosts_max_storage_price = $3, hosts_min_collateral = $4, hosts_min_protocol_version = $5`
		_, err := tx.Exec(ctx, query, sqlCurrency(us.MaxEgressPrice), sqlCurrency(us.MaxIngressPrice), sqlCurrency(us.MaxStoragePrice), sqlCurrency(us.MinCollateral), sqlProtocolVersion(us.MinProtocolVersion))
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
