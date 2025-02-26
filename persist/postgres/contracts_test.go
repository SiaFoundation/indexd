package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
)

func TestFormRenewContract(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// helper to assert contract in db
	assertContract := func(id types.FileContractID, expected api.Contract) {
		t.Helper()
		contract, err := store.Contract(context.Background(), id)
		if err != nil {
			t.Fatal("failed to fetch contract", err)
		} else if !reflect.DeepEqual(contract, expected) {
			t.Fatalf("mismatch: \n%+v\n%+v", contract, expected)
		}
	}

	// form contract
	expected_formed := api.Contract{
		ID:               types.FileContractID{1, 2, 3},
		HostKey:          hk,
		ProofHeight:      100,
		ExpirationHeight: 200,
		State:            api.ContractStatePending,

		ContractPrice:    types.Siacoins(1),
		InitialAllowance: types.Siacoins(2),
		MinerFee:         types.Siacoins(3),

		Usable: true,
	}
	err = store.AddFormedContract(context.Background(), expected_formed.ID, expected_formed.HostKey, expected_formed.ProofHeight, expected_formed.ExpirationHeight, expected_formed.ContractPrice, expected_formed.InitialAllowance, expected_formed.MinerFee)
	if err != nil {
		t.Fatal("failed to add formed contract", err)
	}
	assertContract(expected_formed.ID, expected_formed)

	// simulate using the contract and marking it unusable
	modifyContract := func(contractID types.FileContractID) {
		err = store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			resp, err := tx.Exec(context.Background(), `
					UPDATE contracts
					SET state = 1, capacity = 2000, size = 1000, usable = FALSE, append_sector_spending = 1, free_sector_spending = 2, fund_account_spending = 3, sector_roots_spending = 4
					WHERE contract_id = $1
					`, sqlHash256(contractID))
			if err != nil {
				return err
			} else if resp.RowsAffected() != 1 {
				t.Fatalf("expected 1 row to be affected, got %d", resp.RowsAffected())
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	modifyContract(expected_formed.ID)

	expected_formed.State = api.ContractStateActive
	expected_formed.Capacity = 2000
	expected_formed.Size = 1000
	expected_formed.Usable = false
	expected_formed.Spending = api.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expected_formed.ID, expected_formed)

	// refresh the contract
	expectedRefreshed := expected_formed
	expectedRefreshed.ID = types.FileContractID{4, 5, 6}
	expectedRefreshed.State = api.ContractStatePending
	expectedRefreshed.ContractPrice = types.Siacoins(2)
	expectedRefreshed.InitialAllowance = types.Siacoins(3)
	expectedRefreshed.MinerFee = types.Siacoins(4)
	expectedRefreshed.Usable = true
	expectedRefreshed.RenewedFrom = expected_formed.ID
	expectedRefreshed.Spending = api.ContractSpending{}
	err = store.AddRenewedContract(context.Background(), expectedRefreshed.RenewedFrom, expectedRefreshed.ID, expectedRefreshed.ProofHeight, expectedRefreshed.ExpirationHeight, expectedRefreshed.ContractPrice, expectedRefreshed.InitialAllowance, expectedRefreshed.MinerFee)
	if err != nil {
		t.Fatal("failed to add refreshed contract", err)
	}
	expected_formed.RenewedTo = expectedRefreshed.ID
	assertContract(expected_formed.ID, expected_formed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)
}
