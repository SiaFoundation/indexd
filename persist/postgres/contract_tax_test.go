package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

func taxTestEvent() wallet.Event {
	return wallet.Event{
		ID:        types.Hash256{1},
		Index:     types.ChainIndex{Height: 1, ID: types.BlockID{1}},
		Type:      wallet.EventTypeV2Transaction,
		Timestamp: time.Now(),
		Relevant:  []types.Address{{1}},
		Data: wallet.EventV2Transaction{
			SiacoinInputs: []types.V2SiacoinInput{{SatisfiedPolicy: types.SatisfiedPolicy{Policy: types.PolicyAbove(0)}, Parent: types.SiacoinElement{
				SiacoinOutput: types.SiacoinOutput{Address: types.Address{1}, Value: types.Siacoins(1000)},
			}}},
			FileContracts: []types.V2FileContract{{
				RenterOutput: types.SiacoinOutput{Value: types.Siacoins(100)},
				HostOutput:   types.SiacoinOutput{Value: types.Siacoins(50)},
			}},
		},
	}
}

func TestWalletEventContractTax(t *testing.T) {
	for _, name := range []string{"formation", "renewal", "expiration", "incoming", "v1"} {
		t.Run(name, func(t *testing.T) {
			event := taxTestEvent()
			data := event.Data.(wallet.EventV2Transaction)
			want := types.Siacoins(6)
			switch name {
			case "renewal":
				data.FileContractResolutions = []types.V2FileContractResolution{{Resolution: &types.V2FileContractRenewal{NewContract: data.FileContracts[0]}}}
				data.FileContracts = nil
			case "expiration":
				data.FileContractResolutions = []types.V2FileContractResolution{{Resolution: &types.V2FileContractExpiration{}}}
				data.FileContracts = nil
				want = types.ZeroCurrency
			case "incoming":
				data.SiacoinInputs[0].Parent.SiacoinOutput.Address = types.Address{2}
				data.SiacoinOutputs = []types.SiacoinOutput{{Address: types.Address{1}, Value: types.Siacoins(1)}}
				want = types.ZeroCurrency
			}
			event.Data = data
			if name == "v1" {
				event.Type = wallet.EventTypeV1Transaction
				event.Data = wallet.EventV1Transaction{
					Transaction: types.Transaction{FileContracts: []types.FileContract{{
						Payout:            types.Siacoins(156),
						ValidProofOutputs: []types.SiacoinOutput{{Value: types.Siacoins(100)}, {Value: types.Siacoins(50)}},
					}}},
					SpentSiacoinElements: []types.SiacoinElement{data.SiacoinInputs[0].Parent},
				}
			}
			got, err := walletEventContractTax(event)
			if err != nil {
				t.Fatal(err)
			} else if got != want {
				t.Fatalf("expected tax %v, got %v", want, got)
			}
		})
	}
}

func TestMigrationContractTax(t *testing.T) {
	event := taxTestEvent()
	encoded, err := (*sqlWalletEvent)(&event).Value()
	if err != nil {
		t.Fatal(err)
	}
	index, err := sqlChainIndex(event.Index).Value()
	if err != nil {
		t.Fatal(err)
	}
	seed := fmt.Sprintf(`INSERT INTO wallet_events (chain_index, maturity_height, event_id, event_type, event_data)
VALUES ('\x%x', 1, '\x%x', '%s', '\x%x')`, index, event.ID[:], event.Type, encoded)
	store := initV1Database(t, connectionInfoFromEnv(), seed)
	defer store.Close()

	var tax types.Currency
	if err := store.transaction(func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, sqlStatSelect(statContractTax)).Scan((*sqlCurrency)(&tax))
	}); err != nil {
		t.Fatal(err)
	} else if tax != types.Siacoins(6) {
		t.Fatalf("expected tax %v, got %v", types.Siacoins(6), tax)
	}
}
