package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
)

type (
	contractQueryOpts struct {
		active *bool
		usable *bool
	}

	ContractQueryOpt func(*contractQueryOpts)
)

func WithActive(active bool) ContractQueryOpt {
	return func(opts *contractQueryOpts) {
		opts.active = &active
	}
}

func WithUsable(usable bool) ContractQueryOpt {
	return func(opts *contractQueryOpts) {
		opts.usable = &usable
	}
}

var (
	optTrue = true

	defaultContractQueryOpts = contractQueryOpts{
		active: &optTrue, // return active contracts
		usable: nil,      // return both usable and unusable contracts
	}
)

// AddFormedContract adds a freshly formed contract to the database.
func (s *Store) AddFormedContract(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, proofHeight, expirationHeight uint64, contractPrice, allowance, minerFee types.Currency) error {
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hostKey)).Scan(&hostID); errors.Is(err, pgx.ErrNoRows) {
			return ErrHostNotFound
		} else if err != nil {
			return fmt.Errorf("failed to fetch host: %w", err)
		}
		resp, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, proof_height, expiration_height, contract_price, initial_allowance, miner_fee) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			hostID, sqlHash256(contractID), proofHeight, expirationHeight, sqlCurrency(contractPrice), sqlCurrency(allowance), sqlCurrency(minerFee))
		if err != nil {
			return fmt.Errorf("failed to add formed contract to database: %w", err)
		} else if resp.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %d", resp.RowsAffected())
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// AddRenewedContract adds a renewed contract to the database using the
// following steps:
// - Duplicate the existing contract/row and point the copy to the original
// - Update a potential row that referenced the existing contract in renewed_to to point to the new row
// - Overwrite the existing contract to match the renewed contract
func (s *Store) AddRenewedContract(ctx context.Context, renewedFrom, renewedTo types.FileContractID, proofHeight, expirationHeight uint64, contractPrice, allowance, minerFee types.Currency) error {
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		// defer the evaluation of the UNIQUE constraints while swapping contracts
		if _, err := tx.Exec(ctx, "SET CONSTRAINTS contracts_contract_id_key, contracts_renewed_from_key, contracts_renewed_to_key DEFERRED"); err != nil {
			return fmt.Errorf("failed to defer contract_id key constraint: %w", err)
		}

		// fetch the existing row of the contract
		var existingID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM contracts WHERE contract_id = $1`, sqlHash256(renewedFrom)).Scan(&existingID); err != nil {
			return fmt.Errorf("failed to fetch existing contract: %w", err)
		}

		// duplicate it and make sure the new row renews to the existing row
		var newID int64
		if err := tx.QueryRow(ctx, `
INSERT INTO contracts (host_id, contract_id, proof_height, expiration_height, renewed_from, renewed_to, state, capacity, size, contract_price, initial_allowance, miner_fee, usable, append_sector_spending, free_sector_spending, fund_account_spending, sector_roots_spending) (
	SELECT host_id, contract_id, proof_height, expiration_height, renewed_from, $1, state, capacity, size, contract_price, initial_allowance, miner_fee, usable, append_sector_spending, free_sector_spending, fund_account_spending, sector_roots_spending
	FROM contracts
	WHERE contracts.id = $1
) RETURNING id
`, existingID).Scan(&newID); err != nil {
			return fmt.Errorf("failed to copy renewed contract: %w", err)
		}

		// update a potential row that renewed to the existing contract
		_, err := tx.Exec(context.Background(), `UPDATE contracts SET renewed_to = $1 WHERE renewed_to = $2 AND id != $1`, newID, existingID)
		if err != nil {
			return fmt.Errorf("failed to update renewed_to: %w", err)
		}

		// update the existing row to match the new contract
		resp, err := tx.Exec(ctx, `
UPDATE contracts SET contract_id = $1, proof_height = $2, expiration_height = $3, renewed_from = $4, renewed_to = NULL, state = 0, capacity = CASE WHEN $2 = contracts.proof_height THEN contracts.capacity ELSE contracts.size END, contract_price = $5, initial_allowance = $6, miner_fee = $7, usable = TRUE, append_sector_spending = 0, free_sector_spending = 0, fund_account_spending = 0, sector_roots_spending = 0
WHERE id = $8`, sqlHash256(renewedTo), proofHeight, expirationHeight, newID, sqlCurrency(contractPrice), sqlCurrency(allowance), sqlCurrency(minerFee), existingID)
		if err != nil {
			return fmt.Errorf("failed to init renewed contract: %w", err)
		} else if resp.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %d", resp.RowsAffected())
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) Contract(ctx context.Context, contractID types.FileContractID) (api.Contract, error) {
	var contract api.Contract
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		contract, err = scanContract(tx.QueryRow(ctx, `
SELECT c.contract_id, h.public_key, c.proof_height, c.expiration_height, c_from.contract_id, c_to.contract_id, c.state, c.capacity, c.size, c.contract_price, c.initial_allowance, c.miner_fee, c.usable, c.append_sector_spending, c.free_sector_spending, c.fund_account_spending, c.sector_roots_spending
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id
LEFT JOIN contracts c_from ON c.renewed_from = c_from.id
LEFT JOIN contracts c_to ON c.renewed_to = c_to.id
WHERE c.contract_id = $1`, sqlHash256(contractID)))
		return err
	})
	return contract, err
}

// Contracts queries the contracts in the database. By default, only active
// contracts are returned.
func (s *Store) Contracts(queryOpts ...ContractQueryOpt) ([]api.Contract, error) {
	opts := defaultContractQueryOpts
	for _, opt := range queryOpts {
		opt(&opts)
	}
	panic("not implemented")
}

// SetContractUsable updates the "usable" column of a contract.
func (s *Store) SetContractUsable(usable bool) error {
	panic("not implemented")
}

func scanContract(row scanner) (api.Contract, error) {
	var c api.Contract
	err := row.Scan((*sqlHash256)(&c.ID),
		(*sqlPublicKey)(&c.HostKey),
		&c.ProofHeight, &c.ExpirationHeight,
		(*sqlHash256)(&c.RenewedFrom),
		(*sqlHash256)(&c.RenewedTo),
		(*sqlContractState)(&c.State),
		&c.Capacity,
		&c.Size,
		(*sqlCurrency)(&c.ContractPrice),
		(*sqlCurrency)(&c.InitialAllowance),
		(*sqlCurrency)(&c.MinerFee),
		&c.Usable,
		(*sqlCurrency)(&c.Spending.AppendSector),
		(*sqlCurrency)(&c.Spending.FreeSector),
		(*sqlCurrency)(&c.Spending.FundAccount),
		(*sqlCurrency)(&c.Spending.SectorRoots))
	return c, err
}
