package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestWalletLockUnlock(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	expectedLocked := make(map[types.SiacoinOutputID]bool)
	lockedIDs := make([]types.SiacoinOutputID, 10)
	for i := range lockedIDs {
		lockedIDs[i] = frand.Entropy256()
		expectedLocked[lockedIDs[i]] = true
	}
	if err := store.LockUTXOs(lockedIDs, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ids, err := store.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != len(lockedIDs) {
		t.Fatalf("expected %d locked outputs, got %d", len(lockedIDs), len(ids))
	}
	for _, id := range ids {
		if _, ok := expectedLocked[id]; !ok {
			t.Fatalf("unexpected locked output %s", id)
		}
	}

	if err := store.ReleaseUTXOs(lockedIDs); err != nil {
		t.Fatal(err)
	}

	ids, err = store.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("expected 0 locked outputs, got %d", len(ids))
	}

	// lock the ids, but set the unlock time to the past
	if err := store.LockUTXOs(lockedIDs, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	ids, err = store.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("expected 0 locked outputs, got %d", len(ids))
	}

	// assert the outputs were cleaned up
	var count int
	err = store.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_locked_utxos`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected 0 locked outputs, got %d", count)
	}
}

func BenchmarkWalletLockUnlock(b *testing.B) {
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))

	randomTime := func() time.Time {
		return time.Now().Add(-time.Duration(frand.Uint64n(60*60)) * time.Second)
	}

	// prepare database with 1M locked UTXOs
	outputIDs := make([]types.SiacoinOutputID, 1e6)
	for range 1000 {
		utxos := make([]types.SiacoinOutputID, 1000)
		for i := range utxos {
			utxos[i] = frand.Entropy256()
		}
		err := store.LockUTXOs(utxos, randomTime())
		if err != nil {
			b.Fatal(err)
		}
		outputIDs = append(outputIDs, utxos...)
	}

	b.Run("LockUTXOs", func(b *testing.B) {
		for b.Loop() {
			utxos := make([]types.SiacoinOutputID, 1000)
			for i := range utxos {
				utxos[i] = frand.Entropy256()
			}
			if err := store.LockUTXOs(utxos, randomTime()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LockedUTXOs", func(b *testing.B) {
		for b.Loop() {
			_, err := store.LockedUTXOs(randomTime())
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ReleaseUTXOs", func(b *testing.B) {
		for b.Loop() {
			frand.Shuffle(len(outputIDs), func(i, j int) { outputIDs[i], outputIDs[j] = outputIDs[j], outputIDs[i] })
			if err := store.ReleaseUTXOs(outputIDs[:1000]); err != nil {
				b.Fatal(err)
			}
			outputIDs = outputIDs[1000:]
		}
	})
}

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

	update := types.ChainIndex{Height: 1, ID: types.BlockID{1}}
	if _, err := store.pool.Exec(context.Background(), `UPDATE global_settings SET scanned_height = $1, scanned_block_id = $2`, update.Height, sqlHash256(update.ID)); err != nil {
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
		sqlChainIndex(update.Index),
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
