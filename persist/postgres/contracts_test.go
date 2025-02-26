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
	expectedFormed := api.Contract{
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
	err = store.AddFormedContract(context.Background(), expectedFormed.ID, expectedFormed.HostKey, expectedFormed.ProofHeight, expectedFormed.ExpirationHeight, expectedFormed.ContractPrice, expectedFormed.InitialAllowance, expectedFormed.MinerFee)
	if err != nil {
		t.Fatal("failed to add formed contract", err)
	}
	assertContract(expectedFormed.ID, expectedFormed)

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
	modifyContract(expectedFormed.ID)

	expectedFormed.State = api.ContractStateActive
	expectedFormed.Capacity = 2000
	expectedFormed.Size = 1000
	expectedFormed.Usable = false
	expectedFormed.Spending = api.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expectedFormed.ID, expectedFormed)

	// refresh the contract
	expectedRefreshed := api.Contract{
		ID:               types.FileContractID{4, 5, 6},
		Capacity:         expectedFormed.Capacity,         // same capacity after refresh
		Size:             expectedFormed.Size,             // same size after refresh
		HostKey:          expectedFormed.HostKey,          // same host
		ProofHeight:      expectedFormed.ProofHeight,      // same proof height for refresh
		ExpirationHeight: expectedFormed.ExpirationHeight, // same expiration height for refresh
		State:            api.ContractStatePending,        // refresh resets state
		ContractPrice:    types.Siacoins(2),               // new contract price
		InitialAllowance: types.Siacoins(3),               // new initial allowance
		MinerFee:         types.Siacoins(4),               // new miner fee
		Usable:           true,                            // refreshed contract is usable
		RenewedFrom:      expectedFormed.ID,               // refreshed from formed contract
		Spending:         api.ContractSpending{},          // spending is reset
	}
	err = store.AddRenewedContract(context.Background(), expectedRefreshed.RenewedFrom, expectedRefreshed.ID, expectedRefreshed.ProofHeight, expectedRefreshed.ExpirationHeight, expectedRefreshed.ContractPrice, expectedRefreshed.InitialAllowance, expectedRefreshed.MinerFee)
	if err != nil {
		t.Fatal("failed to add refreshed contract", err)
	}
	expectedFormed.RenewedTo = expectedRefreshed.ID
	assertContract(expectedFormed.ID, expectedFormed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)

	// modify the refreshed contract
	modifyContract(expectedRefreshed.ID)
	expectedRefreshed.State = api.ContractStateActive
	expectedRefreshed.Capacity = 2000
	expectedRefreshed.Size = 1000
	expectedRefreshed.Usable = false
	expectedRefreshed.Spending = api.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expectedRefreshed.ID, expectedRefreshed)

	// renew the refreshed contract
	expectedRenewed := api.Contract{
		ID:               types.FileContractID{7, 8, 9},
		Capacity:         expectedRefreshed.Size,                 // capacity shrinks to size upon renewal
		Size:             expectedRefreshed.Size,                 // same size after renewal
		HostKey:          expectedRefreshed.HostKey,              // same host
		ProofHeight:      expectedRefreshed.ProofHeight * 2,      // higher proof height for renew
		ExpirationHeight: expectedRefreshed.ExpirationHeight * 2, // higher expiration height for renew
		State:            api.ContractStatePending,               // renewal resets state
		ContractPrice:    types.Siacoins(5),                      // new contract price
		InitialAllowance: types.Siacoins(6),                      // new initial allowance
		MinerFee:         types.Siacoins(7),                      // new miner fee
		Usable:           true,                                   // renewed contract is usable
		RenewedFrom:      expectedRefreshed.ID,                   // renewed from refreshed contract
		Spending:         api.ContractSpending{},                 // spending is reset
	}
	err = store.AddRenewedContract(context.Background(), expectedRenewed.RenewedFrom, expectedRenewed.ID, expectedRenewed.ProofHeight, expectedRenewed.ExpirationHeight, expectedRenewed.ContractPrice, expectedRenewed.InitialAllowance, expectedRenewed.MinerFee)
	if err != nil {
		t.Fatal("failed to add refreshed contract", err)
	}
	expectedRefreshed.RenewedTo = expectedRenewed.ID
	assertContract(expectedFormed.ID, expectedFormed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)
	assertContract(expectedRenewed.ID, expectedRenewed)
}
