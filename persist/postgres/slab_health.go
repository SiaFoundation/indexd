package postgres

import "context"

// unhealthySlabExists is an SQL boolean expression that is true when the slab
// has a sector that needs migration: one not stored on a host, or pinned to a
// contract that is no longer good.
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
// returned by slabIDSubquery.  It must be called in the same transaction as,
// and after, any mutation that can change whether a slab's sectors need
// migration.
func recomputeSlabHealth(ctx context.Context, tx *txn, slabIDSubquery string, args ...any) error {
	_, err := tx.Exec(ctx, `UPDATE slabs t SET needs_repair = h.unhealthy
		FROM (
			SELECT s.id, `+unhealthySlabExists+` AS unhealthy
			FROM slabs s
			WHERE s.id IN (`+slabIDSubquery+`)
		) h
		WHERE t.id = h.id AND t.needs_repair IS DISTINCT FROM h.unhealthy`, args...)
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

// recomputeSlabHealthByHostID recomputes the needs_repair flag for every
// slab that has a sector pinned to one of the given host's contracts.
func recomputeSlabHealthByHostID(ctx context.Context, tx *txn, hostID int64) error {
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
