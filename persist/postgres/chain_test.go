package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap/zaptest"
)

type testProofUpdater struct{ fn func(*types.StateElement) }

func (u testProofUpdater) UpdateElementProof(se *types.StateElement) {
	u.fn(se)
}

func proofUpdater(fn func(*types.StateElement)) testProofUpdater {
	return testProofUpdater{fn: fn}
}

func TestProcessChainUpdate(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	sces := []types.SiacoinElement{newTestSiacoinElement()}
	events := []wallet.Event{newTestEvent()}
	events[0].Index = types.ChainIndex{Height: 1}

	// assert err when spending non-existing output
	if err := store.ApplyChainUpdate(context.Background(), func(tx wallet.UpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 1}, nil, sces, events, time.Now())
	}); !errors.Is(err, ErrSiacoinElementNotFound) {
		t.Fatal("unexpected error", err)
	}

	// create elements
	if err := store.ApplyChainUpdate(context.Background(), func(tx wallet.UpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 1}, sces, nil, events, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("unexpected number of events", len(events))
	}

	// spend it
	if err := store.ApplyChainUpdate(context.Background(), func(tx wallet.UpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 2}, nil, sces, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatal("unexpected number of utxos", len(utxos))
	}

	// revert spend
	if err := store.ApplyChainUpdate(context.Background(), func(tx wallet.UpdateTx) error {
		return tx.WalletRevertIndex(types.ChainIndex{Height: 2}, nil, sces, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("unexpected number of events", len(events))
	}

	// update state elements
	update := types.StateElement{LeafIndex: 2, MerkleProof: append(sces[0].StateElement.MerkleProof, types.Hash256{1})}
	if err := store.ApplyChainUpdate(context.Background(), func(tx wallet.UpdateTx) error {
		return tx.UpdateWalletSiacoinElementProofs(testProofUpdater{
			fn: func(se *types.StateElement) {
				se.LeafIndex = update.LeafIndex
				se.MerkleProof = update.MerkleProof
			},
		})
	}); err != nil {
		t.Fatal("unexpected error", err)
	} else if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if !reflect.DeepEqual(utxos[0].StateElement, update) {
		t.Fatal("unexpected state element", utxos[0].StateElement)
	}

	// revert create
	if err := store.ApplyChainUpdate(context.Background(), func(tx wallet.UpdateTx) error {
		return tx.WalletRevertIndex(types.ChainIndex{Height: 1}, sces, nil, time.Now())
	}); err != nil {
		t.Fatal(err)
	} else if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 0 {
		t.Fatal("unexpected number of events", len(events))
	}
}
