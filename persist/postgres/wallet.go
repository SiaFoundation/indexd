package postgres

import (
	"context"
	"fmt"
	"math"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

var _ wallet.SingleAddressStore = (*Store)(nil)

// Tip returns the last scanned index.
func (s *Store) Tip() (ci types.ChainIndex, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT last_scanned_index FROM global_settings`).Scan(decode(&ci))
	})
	return
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
// including immature outputs.
func (s *Store) UnspentSiacoinElements() (sces []types.SiacoinElement, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT id, value, address, merkle_proof, leaf_index, maturity_height FROM wallet_siacoin_elements`)
		if err != nil {
			return fmt.Errorf("failed to query unspent siacoin elements: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var se types.SiacoinElement
			if err := rows.Scan(decode(&se.ID), decode(&se.SiacoinOutput.Value), decode(&se.SiacoinOutput.Address), decode(&se.StateElement.MerkleProof), &se.StateElement.LeafIndex, &se.MaturityHeight); err != nil {
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
func (s *Store) WalletEvents(offset, limit int) (events []wallet.Event, err error) {
	if limit == 0 || limit == -1 {
		limit = math.MaxInt64
	}
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT id, chain_index, maturity_height, event_type, event_data FROM wallet_events ORDER BY maturity_height DESC LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query wallet events: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			if event, err := scanEvent(rows); err != nil {
				return fmt.Errorf("failed to scan wallet event: %w", err)
			} else {
				events = append(events, event)
			}
		}
		return rows.Err()
	})
	return
}

// WalletEventCount returns the total number of events relevant to the wallet.
func (s *Store) WalletEventCount() (count uint64, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM wallet_events`).Scan(&count)
		return err
	})
	return
}

func scanEvent(rows scanner) (event wallet.Event, _ error) {
	var buf []byte
	err := rows.Scan(decode(&event.ID), decode(&event.Index), &event.MaturityHeight, &event.Type, &buf)
	if err != nil {
		return
	}

	dec := types.NewBufDecoder(buf)
	switch event.Type {
	case wallet.EventTypeMinerPayout,
		wallet.EventTypeSiafundClaim,
		wallet.EventTypeFoundationSubsidy:
		var e wallet.EventPayout
		e.DecodeFrom(dec)
		event.Data = e
	case wallet.EventTypeV1ContractResolution:
		var e wallet.EventV1ContractResolution
		e.DecodeFrom(dec)
		event.Data = e
	case wallet.EventTypeV2ContractResolution:
		var e wallet.EventV2ContractResolution
		e.DecodeFrom(dec)
		event.Data = e
	case wallet.EventTypeV1Transaction:
		var e wallet.EventV1Transaction
		e.DecodeFrom(dec)
		event.Data = e
	case wallet.EventTypeV2Transaction:
		var e wallet.EventV2Transaction
		e.DecodeFrom(dec)
		event.Data = e
	default:
		return wallet.Event{}, fmt.Errorf("unknown event type %v", event.Type)
	}
	return
}
