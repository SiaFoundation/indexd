package postgres

import "context"

// unhealthySlabExists is a correlated SQL boolean expression (referencing the
// slabs row through the alias "s") that is TRUE when the slab has a sector that
// needs migration: one not stored on a host, or pinned to a contract that is no
// longer good. It is the single source of truth for the slab health definition
// used by both UnhealthySlabs and the needs_repair flag maintained below.
const unhealthySlabExists = `EXISTS (
	SELECT 1
	FROM slab_sectors ss
	JOIN sectors sec ON sec.id = ss.sector_id
	LEFT JOIN contract_sectors_map csm ON csm.id = sec.contract_sectors_map_id
	LEFT JOIN contracts c ON c.contract_id = csm.contract_id
	WHERE ss.slab_id = s.id
		AND (
			sec.host_id IS NULL
			OR (sec.contract_sectors_map_id IS NOT NULL
				AND (c.good = FALSE OR c.state NOT IN (0, 1)))
		)
)`

// recomputeSlabHealth recalculates the needs_repair flag for every slab
// returned by slabIDSubquery (which may reference args as $1..$N). It must be
// called in the same transaction as, and after, any mutation that can change
// whether a slab's sectors need migration.
func recomputeSlabHealth(ctx context.Context, tx *txn, slabIDSubquery string, args ...any) error {
	// IS DISTINCT FROM skips rewriting rows whose health is unchanged
	_, err := tx.Exec(ctx, `UPDATE slabs s SET needs_repair = `+unhealthySlabExists+`
		WHERE s.id IN (`+slabIDSubquery+`)
			AND s.needs_repair IS DISTINCT FROM (`+unhealthySlabExists+`)`, args...)
	return err
}

// recomputeSlabHealthBySectors recomputes the needs_repair flag for every slab
// that contains any of the given sectors.
func recomputeSlabHealthBySectors(ctx context.Context, tx *txn, sectorIDs []int64) error {
	if len(sectorIDs) == 0 {
		return nil
	}
	return recomputeSlabHealth(ctx, tx, `SELECT slab_id FROM slab_sectors WHERE sector_id = ANY($1)`, sectorIDs)
}

// recomputeSlabHealthBySlabs recomputes the needs_repair flag for the given
// slabs.
func recomputeSlabHealthBySlabs(ctx context.Context, tx *txn, slabIDs []int64) error {
	if len(slabIDs) == 0 {
		return nil
	}
	return recomputeSlabHealth(ctx, tx, `SELECT id FROM slabs WHERE id = ANY($1)`, slabIDs)
}

// recomputeSlabHealthByContractHost recomputes the needs_repair flag for every
// slab that has a sector pinned to one of the given host's contracts.
func recomputeSlabHealthByContractHost(ctx context.Context, tx *txn, hostID int64) error {
	return recomputeSlabHealth(ctx, tx, `
		SELECT ss.slab_id
		FROM contracts c
		JOIN contract_sectors_map csm ON csm.contract_id = c.contract_id
		JOIN sectors sec ON sec.contract_sectors_map_id = csm.id
		JOIN slab_sectors ss ON ss.sector_id = sec.id
		WHERE c.host_id = $1`, hostID)
}

// recomputeSlabHealthByHostKey recomputes the needs_repair flag for every slab
// that has a sector pinned to one of the given host's contracts, identified by
// the host's public key.
func recomputeSlabHealthByHostKey(ctx context.Context, tx *txn, hostKey sqlPublicKey) error {
	return recomputeSlabHealth(ctx, tx, `
		SELECT ss.slab_id
		FROM hosts h
		JOIN contracts c ON c.host_id = h.id
		JOIN contract_sectors_map csm ON csm.contract_id = c.contract_id
		JOIN sectors sec ON sec.contract_sectors_map_id = csm.id
		JOIN slab_sectors ss ON ss.sector_id = sec.id
		WHERE h.public_key = $1`, hostKey)
}

// recomputeSlabHealthByContract recomputes the needs_repair flag for every slab
// that has a sector pinned to the given contract.
func recomputeSlabHealthByContract(ctx context.Context, tx *txn, contractID sqlHash256) error {
	return recomputeSlabHealth(ctx, tx, `
		SELECT ss.slab_id
		FROM contract_sectors_map csm
		JOIN sectors sec ON sec.contract_sectors_map_id = csm.id
		JOIN slab_sectors ss ON ss.sector_id = sec.id
		WHERE csm.contract_id = $1`, contractID)
}
