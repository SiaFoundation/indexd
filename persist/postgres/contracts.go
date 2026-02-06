package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

// ContractRevision returns the revision for the contract with given ID as well
// as a boolean that indicates whether the contract was renewed.
func (s *Store) ContractRevision(contractID types.FileContractID) (rhp.ContractRevision, bool, error) {
	var renewed bool
	var revision sqlFileContract
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT raw_revision, renewed_to IS NOT NULL FROM contracts WHERE contract_id = $1`, sqlHash256(contractID)).Scan(&revision, &renewed)
	}); errors.Is(err, sql.ErrNoRows) {
		return rhp.ContractRevision{}, false, fmt.Errorf("contract %q: %w", contractID, contracts.ErrNotFound)
	} else if err != nil {
		return rhp.ContractRevision{}, false, fmt.Errorf("failed to fetch contract revision: %w", err)
	}
	return rhp.ContractRevision{ID: contractID, Revision: types.V2FileContract(revision)}, renewed, nil
}

// ContractsStats returns statistics about the contracts in the database.
func (s *Store) ContractsStats() (resp admin.ContractsStatsResponse, _ error) {
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		var numContracts, numGood, totalCapacity, totalSize uint64
		err := tx.QueryRow(ctx, `
			WITH globals AS (
				SELECT scanned_height FROM global_settings
			)
			SELECT
				COUNT(*),       				-- non-expired contracts
				COALESCE(SUM(good::int), 0), 	-- good contracts
				COALESCE(SUM(capacity), 0),  	-- total capacity
				COALESCE(SUM(size), 0)      	-- total size
			FROM contracts
			CROSS JOIN globals
			WHERE
				state IN (0,1) AND
				renewed_to IS NULL AND
				proof_height > globals.scanned_height
		`).Scan(&numContracts, &numGood, &totalCapacity, &totalSize)
		if err != nil {
			return err
		}

		var numRenewing uint64
		err = tx.QueryRow(ctx, `
			WITH globals AS (
				SELECT contracts_renew_window, scanned_height FROM global_settings
			)
			SELECT COUNT(*)
			FROM contracts
			CROSS JOIN globals
			WHERE
				good = TRUE AND
				state IN (0,1) AND
				renewed_to IS NULL AND
				proof_height > globals.scanned_height AND
				globals.scanned_height + globals.contracts_renew_window >= proof_height
		`).Scan(&numRenewing)
		if err != nil {
			return err
		}

		resp = admin.ContractsStatsResponse{
			Contracts:    numContracts,
			BadContracts: numContracts - numGood,
			Renewing:     numRenewing,

			TotalCapacity: totalCapacity,
			TotalSize:     totalSize,
		}
		return nil
	})
	return resp, err
}

// UpdateContractRevision updates the contract revision in the database.
func (s *Store) UpdateContractRevision(contract rhp.ContractRevision, usage proto.Usage) error {
	update := contract.Revision
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var existingRevisionNumber uint64
		err := tx.QueryRow(ctx, `SELECT revision_number FROM contracts WHERE contract_id = $1 FOR UPDATE`, sqlHash256(contract.ID)).Scan(&existingRevisionNumber)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("contract %q: %w", contract.ID, contracts.ErrNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to fetch contract revision: %w", err)
		} else if existingRevisionNumber >= update.RevisionNumber {
			return fmt.Errorf("failed to update contract revision, the updated revision number %d is not strictly higher than the existing revision number %d", update.RevisionNumber, existingRevisionNumber)
		}

		query := `UPDATE contracts SET raw_revision = $1, revision_number = $2, capacity = $3, size = $4, remaining_allowance = $5, used_collateral = $6 WHERE contract_id = $7`
		res, err := tx.Exec(ctx, query, sqlFileContract(update), update.RevisionNumber, update.Capacity, update.Filesize, sqlCurrency(update.RenterOutput.Value), sqlCurrency(contract.Revision.RiskedCollateral()), sqlHash256(contract.ID))
		if err != nil {
			return fmt.Errorf("failed to update contract revision: %w", err)
		} else if res.RowsAffected() != 1 {
			return fmt.Errorf("contract %q: %w", contract.ID, contracts.ErrNotFound)
		}

		return updateHostUsage(ctx, tx, contract.Revision.HostPublicKey, usage)
	})
}

// AddFormedContract adds a freshly formed contract to the database.
func (s *Store) AddFormedContract(hostKey types.PublicKey, contractID types.FileContractID, revision types.V2FileContract, contractPrice, allowance, minerFee types.Currency, usage proto.Usage) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var hostID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = $1`, sqlPublicKey(hostKey)).Scan(&hostID); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("host %q: %w", hostKey, hosts.ErrNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to fetch host: %w", err)
		}
		resp, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, proof_height, expiration_height, capacity, size, revision_number, contract_price, initial_allowance, remaining_allowance, miner_fee, total_collateral, raw_revision) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $11, $12)`,
			hostID, sqlHash256(contractID), revision.ProofHeight, revision.ExpirationHeight, revision.Capacity, revision.Filesize, revision.RevisionNumber, sqlCurrency(contractPrice), sqlCurrency(allowance), sqlCurrency(minerFee), sqlCurrency(revision.TotalCollateral), sqlFileContract(revision))
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

		return updateHostUsage(ctx, tx, hostKey, usage)
	})
}

// AddRenewedContract adds a renewed contract to the database. It will update
// the renewed contract and point it to the renewal, as well as update the
// contract id in the contract sectors map.
func (s *Store) AddRenewedContract(renewedFrom, renewedTo types.FileContractID, revision types.V2FileContract, contractPrice, minerFee types.Currency, usage proto.Usage) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `
INSERT INTO contracts(host_id, contract_id, renewed_from, raw_revision, revision_number, proof_height, expiration_height, capacity, size, initial_allowance, remaining_allowance, total_collateral, used_collateral, contract_price, miner_fee)
(SELECT host_id, $1, contract_id, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10, $11, $12 FROM contracts WHERE contract_id = $13)`,
			sqlHash256(renewedTo),
			sqlFileContract(revision),
			revision.RevisionNumber,
			revision.ProofHeight,
			revision.ExpirationHeight,
			revision.Capacity,
			revision.Filesize,
			sqlCurrency(revision.RenterOutput.Value), // initial & remaining allowance
			sqlCurrency(revision.TotalCollateral),
			sqlCurrency(revision.RiskedCollateral()),
			sqlCurrency(contractPrice),
			sqlCurrency(minerFee),
			sqlHash256(renewedFrom),
		)
		if err != nil {
			return fmt.Errorf("failed to add renewal to database: %w", err)
		}

		res, err := tx.Exec(ctx, `UPDATE contracts SET renewed_to = $1 WHERE contract_id = $2`, sqlHash256(renewedTo), sqlHash256(renewedFrom))
		if err != nil {
			return fmt.Errorf("failed to update renewed contract: %w", err)
		} else if res.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %d", res.RowsAffected())
		}

		res, err = tx.Exec(ctx, `UPDATE contract_sectors_map SET contract_id = $1 WHERE contract_id = $2`, sqlHash256(renewedTo), sqlHash256(renewedFrom))
		if err != nil {
			return fmt.Errorf("failed to update contract sectors map: %w", err)
		} else if res.RowsAffected() != 1 {
			return fmt.Errorf("failed to update contract sectors map, no entry found for contract %v", sqlHash256(renewedFrom))
		}

		return updateHostUsage(ctx, tx, revision.HostPublicKey, usage)
	})
}

// Contract returns a single contract
func (s *Store) Contract(contractID types.FileContractID) (contracts.Contract, error) {
	var contract contracts.Contract
	if err := s.transaction(func(ctx context.Context, tx *txn) (err error) {
		contract, err = scanContract(tx.QueryRow(ctx, `
SELECT c.contract_id, c.formation, h.public_key, c.proof_height, c.expiration_height, c.renewed_from, c.renewed_to, c.revision_number, c.state, c.capacity, c.size, c.contract_price, c.initial_allowance, c.remaining_allowance, c.miner_fee, c.used_collateral, c.total_collateral, c.good, c.append_sector_spending, c.free_sector_spending, c.fund_account_spending, c.sector_roots_spending, c.next_prune, c.last_broadcast_attempt
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
func (s *Store) Contracts(offset, limit int, queryOpts ...contracts.ContractQueryOpt) ([]contracts.Contract, error) {
	var opts contracts.ContractQueryOpts
	for _, opt := range queryOpts {
		opt(&opts)
	}

	ids := make([]sqlHash256, len(opts.IDs))
	for i := range ids {
		ids[i] = sqlHash256(opts.IDs[i])
	}

	hks := make([]sqlPublicKey, len(opts.HostKeys))
	for i := range hks {
		hks[i] = sqlPublicKey(opts.HostKeys[i])
	}

	orderClause, err := buildContractOrderByClause(opts.Sorting)
	if err != nil {
		return nil, err
	}

	var contracts []contracts.Contract
	if err := s.transaction(func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT c.contract_id, c.formation, h.public_key, c.proof_height, c.expiration_height, c.renewed_from, c.renewed_to, c.revision_number, c.state, c.capacity, c.size, c.contract_price, c.initial_allowance, c.remaining_allowance, c.miner_fee, c.used_collateral, c.total_collateral, c.good, c.append_sector_spending, c.free_sector_spending, c.fund_account_spending, c.sector_roots_spending, c.next_prune, c.last_broadcast_attempt
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id
WHERE
	-- good filter
	(($1::boolean IS NULL) OR ($1::boolean = c.good)) AND
	-- revisable filter
	(
		$2::boolean IS NULL OR
		($2::boolean = TRUE AND c.state IN (0,1) AND c.renewed_to IS NULL) OR
		($2::boolean = FALSE AND c.state IN (2,3,4))
	)
	-- ID filter
	AND ((CARDINALITY($5::bytea[]) = 0) OR (contract_id = ANY($5)))
	-- public key filter
	AND ((CARDINALITY($6::bytea[]) = 0) OR (h.public_key = ANY($6)))
%s -- order by clause
LIMIT $3 OFFSET $4`, orderClause), opts.Good, opts.Revisable, limit, offset, ids, hks)
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

func buildContractOrderByClause(sorts []contracts.ContractSortOpt) (string, error) {
	if len(sorts) == 0 {
		return "", nil
	}

	sortMapping := map[string]string{
		"id":                    "c.contract_id",
		"hostKey":               "h.public_key",
		"formation":             "c.formation",
		"renewedFrom":           "c.renewed_from",
		"nextPrune":             "c.next_prune",
		"lastBroadcastAttempt":  "c.last_broadcast_attempt",
		"revisionNumber":        "c.revision_number",
		"proofHeight":           "c.proof_height",
		"expirationHeight":      "c.expiration_height",
		"capacity":              "c.capacity",
		"size":                  "c.size",
		"initialAllowance":      "c.initial_allowance",
		"remainingAllowance":    "c.remaining_allowance",
		"totalCollateral":       "c.total_collateral",
		"renewedTo":             "c.renewed_to",
		"usedCollateral":        "c.used_collateral",
		"contractPrice":         "c.contract_price",
		"minerFee":              "c.miner_fee",
		"good":                  "c.good",
		"state":                 "c.state",
		"spending.appendSector": "c.append_sector_spending",
		"spending.freeSector":   "c.free_sector_spending",
		"spending.fundAccount":  "c.fund_account_spending",
		"spending.sectorRoots":  "c.sector_roots_spending",
	}

	parts := make([]string, 0, len(sorts))
	for _, sort := range sorts {
		column, ok := sortMapping[sort.Field]
		if !ok {
			return "", fmt.Errorf("%w: invalid sort column: %q, must be one of %v", contracts.ErrInvalidSortField, sort.Field, slices.Collect(maps.Keys(sortMapping)))
		}
		if sort.Descending {
			parts = append(parts, fmt.Sprintf("%s DESC", column))
		} else {
			parts = append(parts, fmt.Sprintf("%s ASC", column))
		}
	}

	return "ORDER BY " + strings.Join(parts, ", "), nil
}

// ContractsForBroadcasting returns up to 'limit' contracts that need their
// revisions to be rebroadcasted because they haven't been broadcasted (or seen
// on chain) since 'minBroadcast'. The contracts are sorted by the last
// broadcast time.
func (s *Store) ContractsForBroadcasting(minBroadcast time.Time, limit int) ([]types.FileContractID, error) {
	var fcids []types.FileContractID
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT c.contract_id
			FROM contracts c
			WHERE c.state IN (0, 1) AND c.renewed_to IS NULL AND c.last_broadcast_attempt < $1
			ORDER BY c.last_broadcast_attempt ASC
			LIMIT $2`, minBroadcast, limit)
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for broadcasting: %w", err)
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

// ContractsForFunding returns up to 'limit' contracts for the given host key
// that are good for funding ephemeral accounts with. The contracts are sorted
// by the remaining allowance in descending fashion.
func (s *Store) ContractsForFunding(hk types.PublicKey, limit int) ([]types.FileContractID, error) {
	var fcids []types.FileContractID
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
SELECT c.contract_id
FROM contracts c
WHERE
	c.host_id = (SELECT id FROM hosts WHERE public_key = $1) AND
	c.state IN (0, 1) AND
	c.renewed_to IS NULL AND
	c.good AND
	c.remaining_allowance > 0
ORDER BY c.remaining_allowance DESC
LIMIT $2
`, sqlPublicKey(hk), limit)
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

// ContractsForPinning returns usable contracts for the given host key that have
// a size less than the given max contract size. The contracts are sorted by
// size, capacity in descending fashion.
func (s *Store) ContractsForPinning(hk types.PublicKey, maxContractSize uint64) ([]types.FileContractID, error) {
	var fcids []types.FileContractID
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		// covered by index contracts_host_id_active_good_idx
		rows, err := tx.Query(ctx, `
SELECT c.contract_id
FROM contracts c
WHERE
	c.host_id = (SELECT id FROM hosts WHERE public_key = $1) AND
	c.state IN (0, 1) AND
	c.renewed_to IS NULL AND
	c.good AND
	c.remaining_allowance > 0 AND
	c.size < $2
ORDER BY (c.capacity - c.size) DESC`, sqlPublicKey(hk), maxContractSize)
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for pinning: %w", err)
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

// ContractsForPruning returns usable contracts for the given host key that are
// up for pruning. The contracts are sorted by size in descending fashion.
func (s *Store) ContractsForPruning(hk types.PublicKey) ([]types.FileContractID, error) {
	var fcids []types.FileContractID
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
SELECT c.contract_id
FROM contracts c
WHERE
	c.host_id = (SELECT id FROM hosts WHERE public_key = $1) AND
	c.state IN (0, 1) AND
	c.renewed_to IS NULL AND
	c.good AND
	c.remaining_allowance > 0 AND
	c.next_prune < NOW()
ORDER BY c.size DESC`, sqlPublicKey(hk))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for pruning: %w", err)
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

// ContractElement returns the contract element for the given contract ID.
func (s *Store) ContractElement(contractID types.FileContractID) (types.V2FileContractElement, error) {
	var fce types.V2FileContractElement
	if err := s.transaction(func(ctx context.Context, tx *txn) (err error) {
		fce, err = scanContractElement(tx.QueryRow(ctx, `SELECT contract_id, contract, leaf_index, merkle_proof FROM contract_elements fces WHERE contract_id = $1`, sqlHash256(contractID)))
		return
	}); errors.Is(err, sql.ErrNoRows) {
		return types.V2FileContractElement{}, fmt.Errorf("contract %q: %w", contractID, contracts.ErrNotFound)
	} else if err != nil {
		return types.V2FileContractElement{}, err
	}
	return fce, nil
}

// ContractElementsForBroadcast returns the contract elements of contracts that
// have been expired for at least 'maxBlocksSinceExpiry' blocks.
func (s *Store) ContractElementsForBroadcast(maxBlocksSinceExpiry uint64) ([]types.V2FileContractElement, error) {
	var fces []types.V2FileContractElement
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
WITH current_height AS (
	SELECT scanned_height FROM global_settings
)
SELECT
	fces.contract_id,
	fces.contract,
	fces.leaf_index,
	fces.merkle_proof
FROM contracts c
INNER JOIN contract_elements fces ON fces.contract_id = c.contract_id
CROSS JOIN current_height
WHERE current_height.scanned_height >= c.expiration_height + $1
	AND c.state = 1
	AND c.renewed_to IS NULL;`, maxBlocksSinceExpiry)
		if err != nil {
			return err
		}
		defer rows.Close()

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
func (s *Store) MaintenanceSettings() (contracts.MaintenanceSettings, error) {
	var settings contracts.MaintenanceSettings
	err := s.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT contracts_maintenance_enabled, contracts_wanted, contracts_renew_window, contracts_period FROM global_settings`).
			Scan(&settings.Enabled, &settings.WantedContracts, &settings.RenewWindow, &settings.Period)
	})
	return settings, err
}

// PruneExpiredContractElements prunes contract elements for contracts that have
// been expired for at least 'maxBlocksSinceExpiry' blocks.
func (s *Store) PruneExpiredContractElements(maxBlocksSinceExpiry uint64) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
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

// PruneContractSectorsMap prunes the contract_sectors_map table of contracts
// that have been expired for at least 'maxBlocksSinceExpiry' blocks.
func (s *Store) PruneContractSectorsMap(maxBlocksSinceExpiry uint64) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		// fetch rows to prune
		var toPrune []int64
		csmToHostID := make(map[int64]int64)
		rows, err := tx.Query(ctx, `
			WITH current_height AS (
				SELECT scanned_height FROM global_settings
			)
			SELECT csm.id, c.host_id
			FROM contract_sectors_map csm
			CROSS JOIN current_height
			INNER JOIN contracts c ON csm.contract_id = c.contract_id
			WHERE current_height.scanned_height >= c.expiration_height + $1
		`, maxBlocksSinceExpiry)
		if err != nil {
			return fmt.Errorf("failed to query contract_sectors_map for pruning: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id, hostID int64
			if err := rows.Scan(&id, &hostID); err != nil {
				return fmt.Errorf("failed to scan contract_sectors_map id: %w", err)
			}
			toPrune = append(toPrune, id)
			csmToHostID[id] = hostID
		}
		if rows.Err() != nil {
			return fmt.Errorf("failed to iterate over contract_sectors_map rows: %w", rows.Err())
		} else if len(toPrune) == 0 {
			return nil
		}

		s.log.Debug("pruning contract sectors map", zap.Uint64("maxBlocksSinceExpiry", maxBlocksSinceExpiry), zap.Int("toPrune", len(toPrune)))

		updateBatch := &pgx.Batch{}
		pruneBatch := &pgx.Batch{}
		for _, id := range toPrune {
			// update sectors table
			updateBatch.Queue(`
				UPDATE sectors s
				SET contract_sectors_map_id = NULL
				WHERE s.contract_sectors_map_id = $1
			`, id)

			// prune the rows
			pruneBatch.Queue(`
				DELETE FROM contract_sectors_map csm
				WHERE csm.id = $1
			`, id)
		}

		var totalUnpinned int64
		var unpinnedDeltas []unpinnedDelta
		res := tx.SendBatch(ctx, updateBatch)
		for _, id := range toPrune {
			ct, err := res.Exec()
			if err != nil {
				res.Close()
				return fmt.Errorf("failed to update sectors table: %w", err)
			}
			unpinned := ct.RowsAffected()
			unpinnedDeltas = append(unpinnedDeltas, unpinnedDelta{hostID: csmToHostID[id], delta: int64(unpinned)})
			totalUnpinned += unpinned
		}
		if err := res.Close(); err != nil {
			return err
		} else if err := incrementNumPinnedSectors(ctx, tx, -totalUnpinned); err != nil {
			return fmt.Errorf("failed to update number of pinned sectors: %w", err)
		} else if err := incrementNumUnpinnedSectors(ctx, tx, totalUnpinned); err != nil {
			return fmt.Errorf("failed to update number of unpinned sectors: %w", err)
		} else if err := incrementHostsUnpinnedSectors(ctx, tx, unpinnedDeltas); err != nil {
			return fmt.Errorf("failed to update hosts unpinned sectors: %w", err)
		}

		if err := tx.SendBatch(ctx, pruneBatch).Close(); err != nil {
			return fmt.Errorf("failed to prune contract_sectors_map: %w", err)
		}
		return nil
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

func (tx *updateTx) DeleteContractElements(contractIDs ...types.FileContractID) error {
	batch := &pgx.Batch{}
	for _, contractID := range contractIDs {
		batch.Queue(`DELETE FROM contract_elements WHERE contract_id = $1`, sqlHash256(contractID))
	}
	return tx.tx.SendBatch(tx.ctx, batch).Close()
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

// MarkBroadcastAttempt marks a broadcast attempt for the given contract.
func (s *Store) MarkBroadcastAttempt(contractID types.FileContractID) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET last_broadcast_attempt = NOW() WHERE contract_id = $1`, sqlHash256(contractID))
		return err
	})
}

// UpdateNextPrune updates the contract's next prune time to the given time
func (s *Store) UpdateNextPrune(contractID types.FileContractID, nextPrune time.Time) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET next_prune = $1 WHERE contract_id = $2`, nextPrune, sqlHash256(contractID))
		return err
	})
}

// MarkUnrenewableContractsBad marks all contracts as bad that have a proof
// height <= minProofHeight bad.
func (s *Store) MarkUnrenewableContractsBad(minProofHeight uint64) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET good = FALSE WHERE proof_height <= $1`, minProofHeight)
		return err
	})
}

func (s *Store) markContractBad(ctx context.Context, tx *txn, contractID types.FileContractID) error {
	res, err := tx.Exec(ctx, `UPDATE contracts SET good = FALSE WHERE contract_id = $1`, sqlHash256(contractID))
	if err != nil {
		return fmt.Errorf("failed to mark contract bad: %w", err)
	} else if res.RowsAffected() != 1 {
		return fmt.Errorf("contract %q: %w", contractID, contracts.ErrNotFound)
	}
	return nil
}

// MarkContractBad marks a specific contract as bad.
func (s *Store) MarkContractBad(contractID types.FileContractID) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		return s.markContractBad(ctx, tx, contractID)
	})
}

// DeleteContract unpins all sectors from a contract, updates stats, and marks
// the contract as bad.
func (s *Store) DeleteContract(contractID types.FileContractID) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		var contractMapID int64
		err := tx.QueryRow(ctx, `SELECT id FROM contract_sectors_map WHERE contract_id = $1`, sqlHash256(contractID)).Scan(&contractMapID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("contract %q: %w", contractID, contracts.ErrNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to get contract sectors map ID: %w", err)
		}

		// count the number of sectors pinned to this contract
		var pinned int64
		err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM sectors WHERE contract_sectors_map_id = $1`, contractMapID).Scan(&pinned)
		if err != nil {
			return fmt.Errorf("failed to count pinned sectors: %w", err)
		}

		// unpin all sectors by setting contract_sectors_map_id to NULL and
		// updating uploaded_at to prevent MarkSectorsUnpinnable from immediately
		// marking them as unpinnable
		_, err = tx.Exec(ctx, `UPDATE sectors SET contract_sectors_map_id = NULL, uploaded_at = NOW() WHERE contract_sectors_map_id = $1`, contractMapID)
		if err != nil {
			return fmt.Errorf("failed to unpin sectors: %w", err)
		}

		// delete the entry in contract_sectors_map_id
		_, err = tx.Exec(ctx, `DELETE FROM contract_sectors_map WHERE id = $1`, contractMapID)
		if err != nil {
			return fmt.Errorf("failed to delete from contract sectors map: %w", err)
		}

		// update stats
		if pinned > 0 {
			if err := incrementNumPinnedSectors(ctx, tx, -pinned); err != nil {
				return fmt.Errorf("failed to update pinned sectors stat: %w", err)
			}
			if err := incrementNumUnpinnedSectors(ctx, tx, pinned); err != nil {
				return fmt.Errorf("failed to update unpinned sectors stat: %w", err)
			}
		}

		// mark the contract as bad
		return s.markContractBad(ctx, tx, contractID)
	})
}

// RejectPendingContracts marks all contracts as rejected that are currently
// pending and have a formation height older than 'maxFormation'.
func (s *Store) RejectPendingContracts(maxFormation time.Time) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET state = 4 WHERE state = 0 AND formation < $1`, maxFormation)
		return err
	})
}

// UpdateContractRenewedTo updates the renewed to ID of a contract to the
// provided one. We will not renew to a contract that has been deleted to avoid
// violating foreign key constraints.
func (tx *updateTx) UpdateContractRenewedTo(contractID types.FileContractID, renewedTo *types.FileContractID) error {
	if renewedTo != nil {
		var exists bool
		if err := tx.tx.QueryRow(tx.ctx, `SELECT EXISTS(SELECT 1 FROM contracts WHERE contract_id = $1)`, sqlHash256(*renewedTo)).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check if renewal exists: %w", err)
		}
		if !exists {
			return nil
		}
	}

	res, err := tx.tx.Exec(tx.ctx, `UPDATE contracts SET renewed_to = $1 WHERE contract_id = $2`, (*sqlHash256)(renewedTo), sqlHash256(contractID))
	if err != nil {
		return fmt.Errorf("failed to update contract renewed to: %w", err)
	} else if res.RowsAffected() != 1 {
		return fmt.Errorf("expected 1 row to be affected, got %d", res.RowsAffected())
	}
	return nil
}

// PrunableContractRoots diffs the given roots with the roots in the database
// and returns the roots that can be pruned.
func (s *Store) PrunableContractRoots(contractID types.FileContractID, roots []types.Hash256) ([]types.Hash256, error) {
	var sqlRoots []sqlHash256
	for _, root := range roots {
		sqlRoots = append(sqlRoots, sqlHash256(root))
	}

	prunable := make([]types.Hash256, 0, len(roots))
	if err := s.transaction(func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			SELECT s.sector_root
			FROM sectors s
			INNER JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
			WHERE csm.contract_id = $1 AND s.sector_root = ANY($2)`, sqlHash256(contractID), sqlRoots)
		if err != nil {
			return fmt.Errorf("failed to fetch prunable contract roots: %w", err)
		}
		defer rows.Close()

		lookup := make(map[sqlHash256]struct{})
		for rows.Next() {
			var root sqlHash256
			if err := rows.Scan(&root); err != nil {
				return fmt.Errorf("failed to scan root: %w", err)
			}
			lookup[root] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate over rows: %w", err)
		}

		for _, root := range roots {
			if _, ok := lookup[sqlHash256(root)]; !ok {
				prunable = append(prunable, root)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return prunable, nil
}

// ScheduleContractsForPruning schedules contracts for pruning by updating their
// next prune time to the current time.
func (s *Store) ScheduleContractsForPruning() error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET next_prune = NOW() WHERE good = TRUE AND state <= $1`, sqlContractState(contracts.ContractStateActive))
		if err != nil {
			return fmt.Errorf("failed to schedule contracts for pruning: %w", err)
		}
		return nil
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
	var lastPrune sql.NullTime
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
		(*sqlCurrency)(&c.Spending.SectorRoots),
		&lastPrune,
		&c.LastBroadcastAttempt,
	)
	if lastPrune.Valid {
		c.NextPrune = lastPrune.Time
	}
	return c, err
}

func scanContractElement(row scanner) (types.V2FileContractElement, error) {
	var fce types.V2FileContractElement
	err := row.Scan((*sqlHash256)(&fce.ID), (*sqlFileContract)(&fce.V2FileContract), &fce.StateElement.LeafIndex, (*sqlMerkleProof)(&fce.StateElement.MerkleProof))
	return fce, err
}

func updateHostUsage(ctx context.Context, tx *txn, hostKey types.PublicKey, usage proto.Usage) error {
	query := `UPDATE hosts SET usage_account_funding = usage_account_funding + $1, usage_total_spent = usage_total_spent + $2 WHERE public_key = $3`
	resp, err := tx.Exec(ctx, query, sqlCurrency(usage.AccountFunding), sqlCurrency(usage.RenterCost()), sqlPublicKey(hostKey))
	if err != nil {
		return fmt.Errorf("failed to update host usage: %w", err)
	} else if resp.RowsAffected() != 1 {
		return fmt.Errorf("host %q: %w", hostKey, hosts.ErrNotFound)
	}
	return nil
}
