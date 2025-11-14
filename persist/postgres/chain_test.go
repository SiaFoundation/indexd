package postgres

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type testProofUpdater struct{ fn func(*types.StateElement) }

func (u testProofUpdater) UpdateElementProof(se *types.StateElement) {
	u.fn(se)
}

func TestResetChainState(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// define helper to assert number of rows in a table
	assertTableCount := func(table string, want int) {
		t.Helper()
		var got int
		if err := store.pool.QueryRow(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&got); err != nil {
			t.Fatal(err)
		} else if got != want {
			t.Fatalf("expected %d rows in %s, got %d", want, table, got)
		}
	}

	// prepare test elements and events
	index := newTestChainIndex()
	created := []types.SiacoinElement{newTestSiacoinElement()}
	events := []wallet.Event{newTestEvent()}
	events[0].Index = index
	set := wallet.BroadcastedSet{
		Basis:         index,
		Transactions:  []types.V2Transaction{{MinerFee: types.Siacoins(1)}},
		BroadcastedAt: time.Now().Round(time.Second),
	}

	// prepare store with random chain state
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return errors.Join(
			tx.WalletApplyIndex(index, created, nil, events, time.Now()),
			tx.UpdateLastScannedIndex(index),
		)
	}); err != nil {
		t.Fatal(err)
	} else if err := store.AddBroadcastedSet(set); err != nil {
		t.Fatal(err)
	}

	// assert chain state before reset
	if ci, err := store.LastScannedIndex(); err != nil {
		t.Fatal(err)
	} else if ci != index {
		t.Fatal("unexpected last scanned index", ci, index)
	}

	assertTableCount("wallet_siacoin_elements", 1)
	assertTableCount("wallet_broadcasted_sets", 1)
	assertTableCount("wallet_events", 1)

	if err := store.ResetChainState(); err != nil {
		t.Fatal(err)
	}

	// assert chain state after reset
	if ci, err := store.LastScannedIndex(); err != nil {
		t.Fatal(err)
	} else if ci != (types.ChainIndex{}) {
		t.Fatal("unexpected last scanned index", ci, index)
	}

	assertTableCount("wallet_siacoin_elements", 0)
	assertTableCount("wallet_broadcasted_sets", 0)
	assertTableCount("wallet_events", 0)
}

func TestUpdateChainState(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	sces := []types.SiacoinElement{newTestSiacoinElement()}
	events := []wallet.Event{newTestEvent()}
	events[0].Index = types.ChainIndex{Height: 1}

	// assert err when spending non-existing output
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 1}, nil, sces, events, time.Now())
	}); !errors.Is(err, ErrSiacoinElementNotFound) {
		t.Fatal("unexpected error", err)
	}

	// create elements
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 1}, sces, nil, events, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("unexpected number of events", len(events))
	} else if expectedTip, err := store.Tip(); err != nil {
		t.Fatal(err)
	} else if tip != expectedTip {
		t.Fatal("unexpected tip", tip, expectedTip)
	}

	// spend it
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 2}, nil, sces, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if expectedTip, err := store.Tip(); err != nil {
		t.Fatal(err)
	} else if tip != expectedTip {
		t.Fatal("unexpected tip", tip, expectedTip)
	}

	// revert spend
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.WalletRevertIndex(types.ChainIndex{Height: 2}, nil, sces, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("unexpected number of events", len(events))
	} else if expectedTip, err := store.Tip(); err != nil {
		t.Fatal(err)
	} else if tip != expectedTip {
		t.Fatal("unexpected tip", tip, expectedTip)
	}

	// update state elements
	update := types.StateElement{LeafIndex: 2, MerkleProof: append(sces[0].StateElement.MerkleProof, types.Hash256{1})}
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.UpdateWalletSiacoinElementProofs(testProofUpdater{
			fn: func(se *types.StateElement) {
				se.LeafIndex = update.LeafIndex
				se.MerkleProof = update.MerkleProof
			},
		})
	}); err != nil {
		t.Fatal("unexpected error", err)
	} else if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if !reflect.DeepEqual(utxos[0].StateElement, update) {
		t.Fatal("unexpected state element", utxos[0].StateElement)
	} else if expectedTip, err := store.Tip(); err != nil {
		t.Fatal(err)
	} else if tip != expectedTip {
		t.Fatal("unexpected tip", tip, expectedTip)
	}

	// revert create
	if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
		return tx.WalletRevertIndex(types.ChainIndex{Height: 1}, sces, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 0 {
		t.Fatal("unexpected number of events", len(events))
	} else if expectedTip, err := store.Tip(); err != nil {
		t.Fatal(err)
	} else if tip != expectedTip {
		t.Fatal("unexpected tip", tip, expectedTip)
	}
}

func BenchmarkUpdateWalletSiacoinElementProofs(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	for range 1000 {
		se := newTestSiacoinElement()
		frand.Read(se.ID[:])
		if _, err := store.pool.Exec(b.Context(), `INSERT INTO wallet_siacoin_elements (output_id, value, address, merkle_proof, leaf_index, maturity_height) VALUES ($1, $2, $3, $4, $5, $6)`,
			sqlHash256(se.ID),
			sqlCurrency(se.SiacoinOutput.Value),
			sqlHash256(se.SiacoinOutput.Address),
			sqlMerkleProof(se.StateElement.MerkleProof),
			se.StateElement.LeafIndex,
			se.MaturityHeight,
		); err != nil {
			b.Fatal(err)
		}
	}

	for b.Loop() {
		if err := store.UpdateChainState(func(tx subscriber.UpdateTx) error {
			return tx.UpdateWalletSiacoinElementProofs(testProofUpdater{
				fn: func(se *types.StateElement) {
					se.LeafIndex++
					se.MerkleProof = append(se.MerkleProof, frand.Entropy256())
					if len(se.MerkleProof) > 16 {
						se.MerkleProof = se.MerkleProof[len(se.MerkleProof)-16:]
					}
				},
			})
		}); err != nil {
			b.Fatal(err)
		}
	}
}
