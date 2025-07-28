package postgres

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSingleAddressWalletStoreTip(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if _, err := store.pool.Exec(context.Background(), `UPDATE global_settings SET scanned_height = 0, scanned_block_id = $1`, sqlHash256{}); err != nil {
		t.Fatal(err)
	}

	ci, err := store.Tip()
	if err != nil {
		t.Fatal(err)
	} else if ci != (types.ChainIndex{}) {
		t.Fatal("unexpected tip", ci)
	}

	update := newTestChainIndex()
	if _, err := store.pool.Exec(context.Background(), `UPDATE global_settings SET scanned_height = $1, scanned_block_id = $2`, update.Height, sqlHash256(update.ID)); err != nil {
		t.Fatal(err)
	} else if update == (types.ChainIndex{}) {
		t.Fatal("unexpected tip", ci)
	}

	ci, err = store.Tip()
	if err != nil {
		t.Fatal(err)
	} else if ci != update {
		t.Fatal("unexpected tip", ci)
	}
}

func TestSingleAddressWalletStoreUnspentSiacoinElements(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	ci := newTestChainIndex()
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error { return tx.UpdateLastScannedIndex(context.Background(), ci) })
	if err != nil {
		t.Fatal(err)
	}

	if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if tip != ci {
		t.Fatal("unexpected tip", tip, ci)
	}

	update := newTestSiacoinElement()
	if _, err := store.pool.Exec(context.Background(), `INSERT INTO wallet_siacoin_elements (output_id, value, address, merkle_proof, leaf_index, maturity_height) VALUES ($1, $2, $3, $4, $5, $6)`,
		sqlHash256(update.ID),
		sqlCurrency(update.SiacoinOutput.Value),
		sqlHash256(update.SiacoinOutput.Address),
		sqlMerkleProof(update.StateElement.MerkleProof),
		update.StateElement.LeafIndex,
		update.MaturityHeight,
	); err != nil {
		t.Fatal(err)
	}

	if tip, utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if !reflect.DeepEqual(utxos[0], update) {
		t.Fatal("unexpected utxo", utxos[0])
	} else if expectedTip, err := store.Tip(); err != nil {
		t.Fatal(err)
	} else if tip != expectedTip {
		t.Fatal("unexpected tip", tip, expectedTip)
	}
}

func TestSingleAddressWalletStoreWalletEventsAndWalletEventCount(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 0 {
		t.Fatal("unexpected number of events", len(events))
	}

	if count, err := store.WalletEventCount(); err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatal("unexpected number of events", count)
	}

	update := newTestEvent()
	err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		return insertWalletEvent(ctx, tx, update)
	})
	if err != nil {
		t.Fatal(err)
	}

	if count, err := store.WalletEventCount(); err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatal("unexpected number of events", count)
	}

	if events, err := store.WalletEvents(0, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("unexpected number of events", len(events))
	} else if !reflect.DeepEqual(events[0], update) {
		t.Fatal("unexpected event", update, events[0])
	}

	if events, err := store.WalletEvents(1, 10); err != nil {
		t.Fatal(err)
	} else if len(events) != 0 {
		t.Fatal("unexpected number of events", len(events))
	}
}

func TestSingleAddressWalletStoreWalletEvent(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	update := newTestEvent()
	err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		return insertWalletEvent(ctx, tx, update)
	})
	if err != nil {
		t.Fatal(err)
	}

	if event, err := store.WalletEvent(update.ID); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(event, update) {
		t.Fatal("unexpected event", update, event)
	}
}

func TestSingleAddressWalletStoreBroadcastedSets(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// assert there are no sets
	if sets, err := store.BroadcastedSets(); err != nil {
		t.Fatal(err)
	} else if len(sets) != 0 {
		t.Fatal("unexpected number of broadcasted sets", len(sets))
	}

	// add a set
	set := wallet.BroadcastedSet{
		Basis:         types.ChainIndex{Height: 1, ID: types.BlockID{1}},
		Transactions:  []types.V2Transaction{{MinerFee: types.Siacoins(1)}},
		BroadcastedAt: time.Now().Round(time.Second),
	}
	if err := store.AddBroadcastedSet(set); err != nil {
		t.Fatal(err)
	}

	// assert adding the same set is a no-op
	if err := store.AddBroadcastedSet(set); err != nil {
		t.Fatal(err)
	}

	// assert the set was added
	if sets, err := store.BroadcastedSets(); err != nil {
		t.Fatal(err)
	} else if len(sets) != 1 {
		t.Fatal("unexpected number of broadcasted sets", len(sets))
	} else if !reflect.DeepEqual(sets[0], set) {
		t.Fatal("unexpected broadcasted set", sets[0], set)
	}

	// assert the set can be removed
	if err := store.RemoveBroadcastedSet(set); err != nil {
		t.Fatal(err)
	} else if sets, err := store.BroadcastedSets(); err != nil {
		t.Fatal(err)
	} else if len(sets) != 0 {
		t.Fatal("unexpected number of broadcasted sets", len(sets))
	}

	// assert ErrBroadcastedSetNotFound is returned when trying to remove a
	// non-existing set
	err := store.RemoveBroadcastedSet(set)
	if !errors.Is(err, ErrBroadcastedSetNotFound) {
		t.Fatal("unexpected", err)
	}
}

func newTestEvent() wallet.Event {
	return wallet.Event{
		ID:             types.Hash256{1},
		Index:          newTestChainIndex(),
		Type:           wallet.EventTypeSiafundClaim,
		Data:           wallet.EventPayout{SiacoinElement: newTestSiacoinElement()},
		Timestamp:      time.Unix(int64(frand.Intn(int(time.Now().Unix()))), 0),
		MaturityHeight: 4,
		Relevant:       []types.Address{frand.Entropy256()},
	}
}

func newTestChainIndex() (ci types.ChainIndex) {
	ci.Height = frand.Uint64n(math.MaxInt64)
	frand.Read(ci.ID[:])
	return ci
}

func newTestSiacoinElement() types.SiacoinElement {
	return types.SiacoinElement{
		ID: types.SiacoinOutputID{1},
		SiacoinOutput: types.SiacoinOutput{
			Value:   types.Siacoins(2),
			Address: types.StandardAddress(types.GeneratePrivateKey().PublicKey()),
		},
		StateElement: types.StateElement{
			MerkleProof: []types.Hash256{{3}, {4}, {5}},
			LeafIndex:   6,
		},
	}
}
