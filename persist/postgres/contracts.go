package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
)

// AddFormedContract adds a freshly formed contract to the database.
func (s *Store) AddFormedContract(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, proofHeight, expirationHeight uint64, contractPrice, allowance, minerFee, totalCollateral types.Currency) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hostKey)).Scan(&hostID); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hostKey, hosts.ErrNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to fetch host: %w", err)
		}
		resp, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, proof_height, expiration_height, contract_price, initial_allowance, remaining_allowance, miner_fee, total_collateral) VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8)`,
			hostID, sqlHash256(contractID), proofHeight, expirationHeight, sqlCurrency(contractPrice), sqlCurrency(allowance), sqlCurrency(minerFee), sqlCurrency(totalCollateral))
		if err != nil {
			return fmt.Errorf("failed to add formed contract to database: %w", err)
		} else if resp.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %d", resp.RowsAffected())
		}
		resp, err = tx.Exec(ctx, `INSERT INTO contract_sectors_map (contract_id) VALUES ($1)`, sqlHash256(contractID))
		if err != nil {
			return fmt.Errorf("failed to add entry to contract sectors map: %w", err)
		} else if resp.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %d", resp.RowsAffected())
		}
		return nil
	})
}

// AddRenewedContract adds a renewed contract to the database. It will update
// the renewed contract and point it to the renewal, as well as update the
// contract id in the contract sectors map.
func (s *Store) AddRenewedContract(ctx context.Context, params contracts.AddRenewedContractParams) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
INSERT INTO contracts(host_id, contract_id, proof_height, expiration_height, renewed_from, capacity, size, contract_price, initial_allowance, remaining_allowance, miner_fee, used_collateral, total_collateral)
(SELECT host_id, $1, $2, $3, contract_id, CASE WHEN $2 = proof_height THEN capacity ELSE size END, size, $4, $5, $5, $6, $7, $8 FROM contracts WHERE contract_id = $9)`,
			sqlHash256(params.RenewedTo),
			params.ProofHeight,
			params.ExpirationHeight,
			sqlCurrency(params.ContractPrice),
			sqlCurrency(params.Allowance),
			sqlCurrency(params.MinerFee),
			sqlCurrency(params.UsedCollateral),
			sqlCurrency(params.TotalCollateral),
			sqlHash256(params.RenewedFrom),
		)
		if err != nil {
			return fmt.Errorf("failed to add renewal to database: %w", err)
		}

		res, err := tx.Exec(ctx, `UPDATE contracts SET renewed_to = $1 WHERE contract_id = $2`, sqlHash256(params.RenewedTo), sqlHash256(params.RenewedFrom))
		if err != nil {
			return fmt.Errorf("failed to update renewed contract: %w", err)
		} else if res.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %d", res.RowsAffected())
		}

		res, err = tx.Exec(ctx, `UPDATE contract_sectors_map SET contract_id = $1 WHERE contract_id = $2`, sqlHash256(params.RenewedTo), sqlHash256(params.RenewedFrom))
		if err != nil {
			return fmt.Errorf("failed to update contract sectors map: %w", err)
		} else if res.RowsAffected() != 1 {
			return fmt.Errorf("failed to update contract sectors map, no entry found for contract %v", sqlHash256(params.RenewedFrom))
		}
		return nil
	})
}

// Contract returns a single contract
func (s *Store) Contract(ctx context.Context, contractID types.FileContractID) (contracts.Contract, error) {
	var contract contracts.Contract
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		contract, err = scanContract(tx.QueryRow(ctx, `
SELECT c.contract_id, c.formation, h.public_key, c.proof_height, c.expiration_height, c.renewed_from, c.renewed_to, c.revision_number, c.state, c.capacity, c.size, c.contract_price, c.initial_allowance, c.remaining_allowance, c.miner_fee, c.used_collateral, c.total_collateral, c.good, c.append_sector_spending, c.free_sector_spending, c.fund_account_spending, c.sector_roots_spending
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id
WHERE c.contract_id = $1`, sqlHash256(contractID)))
		return err
	}); errors.Is(err, sql.ErrNoRows) {
		return contracts.Contract{}, fmt.Errorf("contract %q: %w", contractID, contracts.ErrNotFound)
	} else if err != nil {
		return contracts.Contract{}, fmt.Errorf("failed to fetch contract: %w", err)
	}
	return contract, nil
}

// Contracts queries the contracts in the database.
func (s *Store) Contracts(ctx context.Context, offset, limit int, queryOpts ...contracts.ContractQueryOpt) ([]contracts.Contract, error) {
	var opts contracts.ContractQueryOpts
	for _, opt := range queryOpts {
		opt(&opts)
	}

	var contracts []contracts.Contract
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, `
SELECT c.contract_id, c.formation, h.public_key, c.proof_height, c.expiration_height, c.renewed_from, c.renewed_to, c.revision_number, c.state, c.capacity, c.size, c.contract_price, c.initial_allowance, c.remaining_allowance, c.miner_fee, c.used_collateral, c.total_collateral, c.good, c.append_sector_spending, c.free_sector_spending, c.fund_account_spending, c.sector_roots_spending
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id
WHERE 
	-- good filter
	(($1::boolean IS NULL) OR ($1::boolean = c.good)) AND
	-- active filter
	(
		$2::boolean IS NULL OR 
		($2::boolean = TRUE AND c.state <= 1) OR 
		($2::boolean = FALSE AND c.state > 1)
	)
LIMIT $3 OFFSET $4`, opts.Good, opts.Revisable, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query contracts: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			contract, err := scanContract(rows)
			if err != nil {
				return fmt.Errorf("failed to scan contract: %w", err)
			}
			contracts = append(contracts, contract)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return contracts, nil
}

// ContractsForFunding returns up to 'limit' contracts for the given host key
// that are good for funding ephemeral accounts with. The contracts are sorted
// by the remaining allowance in descending fashion.
func (s *Store) ContractsForFunding(ctx context.Context, hk types.PublicKey, limit int) ([]types.FileContractID, error) {
	var fcids []types.FileContractID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
SELECT c.contract_id
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id
WHERE h.public_key = $1 AND c.good = TRUE AND c.state <= $2  AND c.remaining_allowance > 0
ORDER BY c.remaining_allowance DESC
LIMIT $3
`, sqlPublicKey(hk), sqlContractState(contracts.ContractStateActive), limit)
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for funding: %w", err)
		}
		for rows.Next() {
			var fcid types.FileContractID
			if err := rows.Scan((*sqlHash256)(&fcid)); err != nil {
				return fmt.Errorf("failed to scan contract ID: %w", err)
			}
			fcids = append(fcids, fcid)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return fcids, nil
}

// ContractElementsForBroadcast returns the contract elements of contracts that
// have been expired for at least 'maxBlocksSinceExpiry' blocks.
func (s *Store) ContractElementsForBroadcast(ctx context.Context, maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error) {
	var fces []types.V2FileContractElement
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
WITH current_height AS (
    SELECT scanned_height FROM global_settings
)
SELECT
    fces.contract_id,
    fces.contract,
    fces.leaf_index,
    fces.merkle_proof
FROM contract_elements fces
INNER JOIN contracts c ON fces.contract_id = c.contract_id
CROSS JOIN current_height
WHERE current_height.scanned_height >= c.expiration_height + $1;
`, maxBlocksSinceExpiry)
		if err != nil {
			return err
		}
		for rows.Next() {
			fce, err := scanContractElement(rows)
			if err != nil {
				return err
			}
			fces = append(fces, fce)
		}
		return rows.Err()
	})
	return fces, err
}

// MaintenanceSettings returns the current maintenance settings.
func (s *Store) MaintenanceSettings(ctx context.Context) (contracts.MaintenanceSettings, error) {
	var settings contracts.MaintenanceSettings
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT contracts_maintenance_enabled, contracts_wanted, contracts_renew_window, contracts_period FROM global_settings`).
			Scan(&settings.Enabled, &settings.WantedContracts, &settings.RenewWindow, &settings.Period)
	})
	return settings, err
}

// PruneExpiredContractElements prunes contract elements for contracts that have
// been expired for at least 'maxBlocksSinceExpiry' blocks.
func (s *Store) PruneExpiredContractElements(ctx context.Context, maxBlocksSinceExpiry uint64) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
WITH current_height AS (
    SELECT scanned_height FROM global_settings
)
DELETE FROM contract_elements fces
USING contracts, current_height
WHERE fces.contract_id = contracts.contract_id AND current_height.scanned_height >= contracts.expiration_height + $1;
`, maxBlocksSinceExpiry)
		return err
	})
}

func (tx *updateTx) ContractElements() ([]types.V2FileContractElement, error) {
	rows, err := tx.tx.Query(tx.ctx, `SELECT contract_id, contract, leaf_index, merkle_proof FROM contract_elements fces`)
	if err != nil {
		return nil, err
	}
	var fces []types.V2FileContractElement
	for rows.Next() {
		fce, err := scanContractElement(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract element: %w", err)
		}
		fces = append(fces, fce)
	}
	return fces, rows.Err()
}

func (tx *updateTx) IsKnownContract(contractID types.FileContractID) (bool, error) {
	var exists bool
	err := tx.tx.QueryRow(tx.ctx, `SELECT EXISTS (SELECT 1 FROM contracts WHERE contract_id = $1)`, sqlHash256(contractID)).
		Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if contract is known: %w", err)
	}
	return exists, nil
}

// MarkUnrenewableContractsBad marks all contracts as bad that have a proof
// height <= minProofHeight bad.
func (s *Store) MarkUnrenewableContractsBad(ctx context.Context, minProofHeight uint64) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET good = FALSE WHERE proof_height <= $1`, minProofHeight)
		return err
	})
}

// RejectPendingContracts marks all contracts as rejected that are currently
// pending and have a formation height older than 'maxFormation'.
func (s *Store) RejectPendingContracts(ctx context.Context, maxFormation time.Time) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET state = $1 WHERE state = $2 AND formation < $3`,
			sqlContractState(contracts.ContractStateRejected), sqlContractState(contracts.ContractStatePending), maxFormation)
		return err
	})
}

// SyncContract updates the contract with the given ID to the provided
// parameters which are expected to contain information about the latest
// revision of a contract.
func (s *Store) SyncContract(ctx context.Context, contractID types.FileContractID, params contracts.ContractSyncParams) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
UPDATE contracts
SET capacity = $1, remaining_allowance = $2, revision_number = $3, size = $4, used_collateral = $5
WHERE contract_id = $6
`, params.Capacity, sqlCurrency(params.RemainingAllowance), params.RevisionNumber, params.Size, sqlCurrency(params.UsedCollateral), sqlHash256(contractID))
		return err
	})
}

func (tx *updateTx) UpdateContractElements(fces ...types.V2FileContractElement) error {
	for _, fce := range fces {
		_, err := tx.tx.Exec(tx.ctx, `
INSERT INTO contract_elements (contract_id, contract, leaf_index, merkle_proof) VALUES ($1, $2, $3, $4) 
ON CONFLICT (contract_id) DO UPDATE SET contract = EXCLUDED.contract, leaf_index = EXCLUDED.leaf_index, merkle_proof = EXCLUDED.merkle_proof
`, sqlHash256(fce.ID), (*sqlFileContract)(&fce.V2FileContract), fce.StateElement.LeafIndex, sqlMerkleProof(fce.StateElement.MerkleProof))
		if err != nil {
			return fmt.Errorf("failed to update contract element for contract %v: %w", fce.ID, err)
		}
	}
	return nil
}

// UpdateContractState updates the state of a contract to the provided one.
func (tx *updateTx) UpdateContractState(contractID types.FileContractID, state contracts.ContractState) error {
	_, err := tx.tx.Exec(tx.ctx, `UPDATE contracts SET state = $1 WHERE contract_id = $2`, sqlContractState(state), sqlHash256(contractID))
	if err != nil {
		return fmt.Errorf("failed to update contract state: %w", err)
	}
	return nil
}

func scanContract(row scanner) (contracts.Contract, error) {
	var c contracts.Contract
	err := row.Scan((*sqlHash256)(&c.ID),
		&c.Formation,
		(*sqlPublicKey)(&c.HostKey),
		&c.ProofHeight, &c.ExpirationHeight,
		asNullable((*sqlHash256)(&c.RenewedFrom)),
		asNullable((*sqlHash256)(&c.RenewedTo)),
		&c.RevisionNumber,
		(*sqlContractState)(&c.State),
		&c.Capacity,
		&c.Size,
		(*sqlCurrency)(&c.ContractPrice),
		(*sqlCurrency)(&c.InitialAllowance),
		(*sqlCurrency)(&c.RemainingAllowance),
		(*sqlCurrency)(&c.MinerFee),
		(*sqlCurrency)(&c.UsedCollateral),
		(*sqlCurrency)(&c.TotalCollateral),
		&c.Good,
		(*sqlCurrency)(&c.Spending.AppendSector),
		(*sqlCurrency)(&c.Spending.FreeSector),
		(*sqlCurrency)(&c.Spending.FundAccount),
		(*sqlCurrency)(&c.Spending.SectorRoots))
	return c, err
}

func scanContractElement(row scanner) (types.V2FileContractElement, error) {
	var fce types.V2FileContractElement
	err := row.Scan((*sqlHash256)(&fce.ID), (*sqlFileContract)(&fce.V2FileContract), &fce.StateElement.LeafIndex, (*sqlMerkleProof)(&fce.StateElement.MerkleProof))
	return fce, err
}
