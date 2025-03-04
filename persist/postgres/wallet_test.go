package postgres

import (
	"context"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap/zaptest"
)

func TestSingleAddressWalletStoreTip(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	if _, err := store.pool.Exec(context.Background(), `UPDATE global_settings SET last_scanned_index = NULL`); err != nil {
		t.Fatal(err)
	}

	ci, err := store.Tip()
	if err != nil {
		t.Fatal(err)
	} else if ci != (types.ChainIndex{}) {
		t.Fatal("unexpected tip", ci)
	}

	update := types.ChainIndex{Height: 1, ID: types.BlockID{1}}
	if _, err := store.pool.Exec(context.Background(), `UPDATE global_settings SET last_scanned_index = $1`, asSiaEncoded(&update)); err != nil {
		t.Fatal(err)
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

	if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatal("unexpected number of utxos", len(utxos))
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

	if utxos, err := store.UnspentSiacoinElements(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatal("unexpected number of utxos", len(utxos))
	} else if !reflect.DeepEqual(utxos[0], update) {
		t.Fatal("unexpected utxo", utxos[0])
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
	if _, err := store.pool.Exec(context.Background(), `INSERT INTO wallet_events (chain_index, maturity_height, event_id, event_type, event_data) VALUES ($1, $2, $3, $4, $5)`,
		asSiaEncoded(&update.Index),
		update.MaturityHeight,
		sqlHash256(update.ID),
		update.Type,
		sqlEncodeEvent(update.Type, update.Data),
	); err != nil {
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

func newTestEvent() wallet.Event {
	return wallet.Event{
		ID: types.Hash256{1},
		Index: types.ChainIndex{
			Height: 2,
			ID:     types.BlockID{3},
		},
		Type:           wallet.EventTypeSiafundClaim,
		Data:           wallet.EventPayout{SiacoinElement: newTestSiacoinElement()},
		MaturityHeight: 4,
	}
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
