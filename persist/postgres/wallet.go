package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

var (
	// ErrSiacoinElementNotFound is returned when a siacoin element is not
	// found in the database.
	ErrSiacoinElementNotFound = errors.New("not found")
)

var _ wallet.SingleAddressStore = (*Store)(nil)

// Tip returns the last scanned index.
func (s *Store) Tip() (ci types.ChainIndex, err error) {
	return s.LastScannedIndex(context.Background())
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
// including immature outputs.
func (s *Store) UnspentSiacoinElements() (sces []types.SiacoinElement, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT output_id, value, address, merkle_proof, leaf_index, maturity_height FROM wallet_siacoin_elements`)
		if err != nil {
			return fmt.Errorf("failed to query unspent siacoin elements: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var se types.SiacoinElement
			if err := rows.Scan((*sqlHash256)(&se.ID), (*sqlCurrency)(&se.SiacoinOutput.Value), (*sqlHash256)(&se.SiacoinOutput.Address), (*sqlMerkleProof)(&se.StateElement.MerkleProof), &se.StateElement.LeafIndex, &se.MaturityHeight); err != nil {
				return fmt.Errorf("failed to scan unspent siacoin element: %w", err)
			}
			sces = append(sces, se)
		}
		return rows.Err()
	})
	return
}

// WalletEvents returns a paginated list of transactions ordered by maturity
// height, descending. If no more transactions are available, (nil, nil) should
// be returned.
func (s *Store) WalletEvents(offset, limit int) ([]wallet.Event, error) {
	// sanity check input
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var events []wallet.Event
	if err := s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		var tip types.ChainIndex
		err := tx.QueryRow(ctx, `SELECT last_scanned_index FROM global_settings`).Scan(asSiaEncoded(&tip))
		if err != nil {
			return fmt.Errorf("failed to query last scanned index: %w", err)
		}

		rows, err := tx.Query(ctx, `SELECT chain_index, maturity_height, event_id, event_type, event_data FROM wallet_events ORDER BY maturity_height DESC, id DESC LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query wallet events: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var event wallet.Event
			err := rows.Scan(asSiaEncoded(&event.Index), &event.MaturityHeight, (*sqlHash256)(&event.ID), &event.Type, sqlDecodeEvent(&event.Data))
			if err != nil {
				return fmt.Errorf("failed to scan wallet event: %w", err)
			}
			if tip.Height >= event.Index.Height {
				event.Confirmations = 1 + tip.Height - event.Index.Height
			}
			events = append(events, event)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return events, nil
}

// WalletEventCount returns the total number of events relevant to the wallet.
func (s *Store) WalletEventCount() (count uint64, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM wallet_events`).Scan(&count)
		return err
	})
	return
}

func (u *updateTx) UpdateWalletSiacoinElementProofs(updater wallet.ProofUpdater) error {
	rows, err := u.tx.Query(u.ctx, `SELECT output_id, leaf_index, merkle_proof FROM wallet_siacoin_elements`)
	if err != nil {
		return fmt.Errorf("failed to query siacoin elements: %w", err)
	}
	defer rows.Close()

	type sce struct {
		id          sqlHash256
		leafIndex   uint64
		merkleProof sqlMerkleProof
	}

	sces := make(map[sqlHash256]*types.StateElement)
	for rows.Next() {
		var sce sce
		if err := rows.Scan(&sce.id, &sce.leafIndex, &sce.merkleProof); err != nil {
			return fmt.Errorf("failed to scan siacoin element: %w", err)
		}
		sces[sce.id] = &types.StateElement{
			LeafIndex:   sce.leafIndex,
			MerkleProof: sce.merkleProof,
		}
		updater.UpdateElementProof(sces[sce.id])
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for id, se := range sces {
		const query = `UPDATE wallet_siacoin_elements SET leaf_index = $1, merkle_proof = $2 WHERE output_id = $3`
		if _, err := u.tx.Exec(u.ctx, query, se.LeafIndex, sqlMerkleProof(se.MerkleProof), id); err != nil {
			return fmt.Errorf("failed to update siacoin element: %w", err)
		}
	}
	return nil
}

func (u *updateTx) WalletApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event, timestamp time.Time) error {
	if len(spent) > 0 {
		for _, se := range spent {
			if res, err := u.tx.Exec(u.ctx, `DELETE FROM wallet_siacoin_elements WHERE output_id = $1`, sqlHash256(se.ID)); err != nil {
				return fmt.Errorf("failed to delete siacoin element: %w", err)
			} else if res.RowsAffected() != 1 {
				return fmt.Errorf("failed to delete siacoin element %v: %w", se.ID, ErrSiacoinElementNotFound)
			}
		}
	}

	if len(created) > 0 {
		for _, se := range created {
			if res, err := u.tx.Exec(u.ctx, `INSERT INTO wallet_siacoin_elements (output_id, value, address, merkle_proof, leaf_index, maturity_height) VALUES ($1, $2, $3, $4, $5, $6)`,
				sqlHash256(se.ID),
				sqlCurrency(se.SiacoinOutput.Value),
				sqlHash256(se.SiacoinOutput.Address),
				sqlMerkleProof(se.StateElement.MerkleProof),
				se.StateElement.LeafIndex,
				se.MaturityHeight,
			); err != nil {
				return fmt.Errorf("failed to insert siacoin element: %w", err)
			} else if res.RowsAffected() != 1 {
				return errors.New("failed to insert siacoin element")
			}
		}
	}

	if len(events) > 0 {
		for _, e := range events {
			if res, err := u.tx.Exec(u.ctx, `INSERT INTO wallet_events (chain_index, maturity_height, event_id, event_type, event_data) VALUES ($1, $2, $3, $4, $5)`,
				asSiaEncoded(&e.Index),
				e.MaturityHeight,
				sqlHash256(e.ID),
				e.Type,
				sqlEncodeEvent(e.Type, e.Data),
			); err != nil {
				return fmt.Errorf("failed to insert event: %w", err)
			} else if res.RowsAffected() != 1 {
				return errors.New("failed to insert event")
			}
		}
	}
	return nil
}

func (u *updateTx) WalletRevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement, timestamp time.Time) error {
	if len(removed) > 0 {
		for _, se := range removed {
			if res, err := u.tx.Exec(u.ctx, `DELETE FROM wallet_siacoin_elements WHERE output_id = $1`, sqlHash256(se.ID)); err != nil {
				return fmt.Errorf("failed to delete siacoin element: %w", err)
			} else if res.RowsAffected() != 1 {
				return fmt.Errorf("failed to delete siacoin element %v: %w", se.ID, ErrSiacoinElementNotFound)
			}
		}
	}

	if len(unspent) > 0 {
		for _, se := range unspent {
			if res, err := u.tx.Exec(u.ctx, `INSERT INTO wallet_siacoin_elements (output_id, value, address, merkle_proof, leaf_index, maturity_height) VALUES ($1, $2, $3, $4, $5, $6)`,
				sqlHash256(se.ID),
				sqlCurrency(se.SiacoinOutput.Value),
				sqlHash256(se.SiacoinOutput.Address),
				sqlMerkleProof(se.StateElement.MerkleProof),
				se.StateElement.LeafIndex,
				se.MaturityHeight,
			); err != nil {
				return fmt.Errorf("failed to insert siacoin element: %w", err)
			} else if res.RowsAffected() != 1 {
				return errors.New("failed to insert siacoin element")
			}
		}
	}

	_, err := u.tx.Exec(u.ctx, `DELETE FROM wallet_events WHERE chain_index = $1`, asSiaEncoded(&index))
	if err != nil {
		return fmt.Errorf("failed to delete events: %w", err)
	}
	return nil
}

func validateOffsetLimit(offset, limit int) error {
	if offset < 0 {
		return errors.New("offset can not be negative")
	} else if limit < 0 {
		return errors.New("limit can not be negative")
	}
	return nil
}
