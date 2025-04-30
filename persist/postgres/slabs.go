package postgres

import (
	"context"
	"errors"
	"fmt"
)

const slabPruneBatchSize = 35

// PruneSlabs will delete slabs (and its sectors) that are not referenced by any
// account. It will do so in batches of 35 (roughly 1k sectors at default
// redundancy) and will continue until either the slabs to prune are exhausted
// or the context is cancelled. It will return the total number of slabs pruned,
// or an error if one occurred.
func (s *Store) PruneSlabs(ctx context.Context) (int64, error) {
	var pruned int64
	for {
		n, err := s.pruneSlabsLimit(ctx, slabPruneBatchSize)
		if err != nil {
			if pruned > 0 && errors.Is(err, context.Canceled) {
				break // ignore context cancellation if slabs were pruned
			}
			return pruned, err
		}

		pruned += n
		if n < slabPruneBatchSize {
			break
		}
	}
	return pruned, nil
}

func (s *Store) pruneSlabsLimit(ctx context.Context, limit int) (int64, error) {
	var pruned int64
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		slabIDs, err := prunableSlabs(ctx, tx, limit)
		if err != nil {
			return fmt.Errorf("failed to fetch slabs to prune: %w", err)
		} else if len(slabIDs) == 0 {
			return nil
		}

		counts, err := prunableSectorCounts(ctx, tx, slabIDs)
		if err != nil {
			return fmt.Errorf("failed to fetch prunable sector counts: %w", err)
		}
		_ = counts // TODO: increment prunable data on the contract

		_, err = tx.Exec(ctx, `
		DELETE FROM sectors s
		WHERE EXISTS (
			SELECT 1
			FROM slab_sectors ss_in
			WHERE ss_in.sector_id = s.id
			AND ss_in.slab_id = ANY ($1)
		)
		AND NOT EXISTS (
			SELECT 1
			FROM slab_sectors ss_other
			WHERE ss_other.sector_id = s.id
			AND ss_other.slab_id <> ALL ($1)
		);`, slabIDs)
		if err != nil {
			return fmt.Errorf("failed to delete sectors: %w", err)
		}

		res, err := tx.Exec(ctx, `DELETE FROM slabs WHERE id = ANY($1);`, slabIDs)
		if err != nil {
			return fmt.Errorf("failed to delete slabs: %w", err)
		} else if res.RowsAffected() != int64(len(slabIDs)) {
			return fmt.Errorf("deleted %d slabs, expected %d", res.RowsAffected(), len(slabIDs))
		}
		pruned = res.RowsAffected()

		return nil
	}); err != nil {
		return 0, err
	}
	return pruned, nil
}

func prunableSectorCounts(ctx context.Context, tx *txn, slabIDs []int64) (map[sqlHash256]int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT csm.contract_id, COUNT(*)
		FROM slab_sectors ss
		INNER JOIN sectors s ON ss.sector_id = s.id
		INNER JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
		WHERE ss.slab_id = ANY($1)
		GROUP BY csm.contract_id;
	`, slabIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to query prunable sector counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[sqlHash256]int64)
	for rows.Next() {
		var contractID sqlHash256
		var count int64
		if err := rows.Scan(&contractID, &count); err != nil {
			return nil, fmt.Errorf("failed to scan contract ID and count: %w", err)
		}
		counts[contractID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return counts, nil
}

func prunableSlabs(ctx context.Context, tx *txn, limit int) ([]int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT slabs.id
		FROM slabs
		LEFT JOIN account_slabs ON slabs.id = account_slabs.slab_id
		WHERE account_slabs.slab_id IS NULL
		LIMIT $1;`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query slabs for pruning: %w", err)
	}
	defer rows.Close()

	var slabIDs []int64
	for rows.Next() {
		var slabID int64
		if err := rows.Scan(&slabID); err != nil {
			return nil, fmt.Errorf("failed to scan slab ID: %w", err)
		}
		slabIDs = append(slabIDs, slabID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return slabIDs, nil
}
