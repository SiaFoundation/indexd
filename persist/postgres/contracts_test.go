package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestContracts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add three contracts
	fcid1 := types.FileContractID{1}
	fcid2 := types.FileContractID{2}
	fcid3 := types.FileContractID{3}
	if err := errors.Join(
		store.AddFormedContract(context.Background(), fcid1, hk, 0, 0, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency),
		store.AddFormedContract(context.Background(), fcid2, hk, 0, 0, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency),
		store.AddFormedContract(context.Background(), fcid3, hk, 0, 0, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency),
	); err != nil {
		t.Fatal(err)
	}

	// mark the second one resolved so it's considered inactive
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractState(fcid2, contracts.ContractStateResolved)
	}); err != nil {
		t.Fatal(err)
	}

	// mark the third as bad so it's considered not good
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET good = FALSE WHERE contract_id = $1`, sqlHash256(fcid3))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// assert store returns all contracts without filters
	if css, err := store.Contracts(context.Background(), 0, 10); err != nil {
		t.Fatal(err)
	} else if len(css) != 3 {
		t.Fatal("expected 3 contracts, got", len(css))
	}

	// assert limit and offset
	if css, err := store.Contracts(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	} else if len(css) != 1 {
		t.Fatal("expected 1 contract, got", len(css))
	} else if css, err := store.Contracts(context.Background(), 3, 1); err != nil {
		t.Fatal(err)
	} else if len(css) != 0 {
		t.Fatal("expected no contracts, got", len(css))
	}

	// assert WithRevisable(true)
	if css, err := store.Contracts(context.Background(), 0, 10, contracts.WithRevisable(true)); err != nil {
		t.Fatal(err)
	} else if len(css) != 2 {
		t.Fatal("expected 2 contracts, got", len(css))
	}

	// assert WithRevisable(false)
	if css, err := store.Contracts(context.Background(), 0, 10, contracts.WithRevisable(false)); err != nil {
		t.Fatal(err)
	} else if len(css) != 1 {
		t.Fatal("expected 1 contract, got", len(css))
	} else if css[0].ID != fcid2 {
		t.Fatalf("expected contract %v, got %v", fcid2, css[0].ID)
	}

	// assert WithRevisable(true) + WithGood(true)
	if css, err := store.Contracts(context.Background(), 0, 10, contracts.WithRevisable(true), contracts.WithGood(true)); err != nil {
		t.Fatal(err)
	} else if len(css) != 1 {
		t.Fatal("expected 1 contract, got", len(css))
	} else if css[0].ID != fcid1 {
		t.Fatalf("expected contract %v, got %v", fcid1, css[0].ID)
	}
}

func TestContractElementsForBroadcast(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// add a contract
	fce := types.V2FileContractElement{
		ID: types.FileContractID{1, 2, 3},
		StateElement: types.StateElement{
			LeafIndex:   1,
			MerkleProof: []types.Hash256{{1}},
		},
		V2FileContract: types.V2FileContract{
			ExpirationHeight: 100,
			HostPublicKey:    hk,
		},
	}
	if err := store.AddFormedContract(context.Background(), fce.ID, hk, 50, fce.V2FileContract.ExpirationHeight, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4)); err != nil {
		t.Fatal(err)
	}

	// helper to assert contracts to broadcast
	assertContractsToBroadcast := func(maxBlocksSinceExpiry uint64, n int) {
		t.Helper()
		fces, err := store.ContractElementsForBroadcast(context.Background(), maxBlocksSinceExpiry)
		if err != nil {
			t.Fatal(err)
		} else if len(fces) != n {
			t.Fatalf("expected %d contracts to broadcast, got %d", n, len(fces))
		}
	}

	// set height to 10 blocks after expiration
	err = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(context.Background(), types.ChainIndex{Height: fce.V2FileContract.ExpirationHeight + 10})
	})
	if err != nil {
		t.Fatal(err)
	}

	// no contracts to broadcast since we haven't added the element yet
	assertContractsToBroadcast(1, 0)

	// add contract element, 1 contract to broadcast
	err = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractElements(fce)
	})
	if err != nil {
		t.Fatal(err)
	}

	assertContractsToBroadcast(11, 0) // still within bounds
	assertContractsToBroadcast(10, 1) // not within bounds

	// assert fce matches expected
	fces, err := store.ContractElementsForBroadcast(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	} else if len(fces) != 1 {
		t.Fatalf("expected 1 contract to broadcast, got %d", len(fces))
	} else if !reflect.DeepEqual(fces[0], fce) {
		t.Fatalf("mismatch: \n%+v\n%+v", fce, fces[0])
	}
}

func TestPruneExpiredContractElements(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	addContract := func(expirationHeight uint64) types.FileContractID {
		t.Helper()
		var contractID types.FileContractID
		frand.Read(contractID[:])
		if err := store.AddFormedContract(context.Background(), contractID, hk, 50, expirationHeight, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4)); err != nil {
			t.Fatal(err)
		}
		err = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.UpdateContractElements(types.V2FileContractElement{
				ID: contractID,
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		return contractID
	}

	assertContracts := func(contractIDs []types.FileContractID) {
		t.Helper()
		_ = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			fces, err := tx.ContractElements()
			if err != nil {
				t.Fatal(err)
			} else if len(fces) != len(contractIDs) {
				t.Fatalf("expected %d contracts, got %d", len(contractIDs), len(fces))
			}
		outer:
			for _, c := range fces {
				for _, id := range contractIDs {
					if c.ID == id {
						continue outer
					}
				}
				t.Fatalf("contract %v is missing", c.ID)
			}
			return nil
		})
	}

	// set height to block 12
	bh := uint64(12)
	err = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(context.Background(), types.ChainIndex{Height: bh})
	})
	if err != nil {
		t.Fatal(err)
	}

	// add 3 contracts at different expiration heights
	c1 := addContract(10)
	c2 := addContract(11)
	c3 := addContract(12)

	// prune with a buffer of 3, should not prune anything
	if err := store.PruneExpiredContractElements(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	assertContracts([]types.FileContractID{c1, c2, c3})

	// prune with a buffer of 1, should prune c1 and c2
	if err := store.PruneExpiredContractElements(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	assertContracts([]types.FileContractID{c3})

	// prune with a buffer of 0 to prune c3 too
	if err := store.PruneExpiredContractElements(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	assertContracts([]types.FileContractID{})

	// assert only the elements got pruned but the contracts remain
	err = store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		var count int
		err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM contracts").Scan(&count)
		if err != nil {
			return err
		} else if count != 3 {
			t.Fatalf("expected 3 contracts, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFormRenewContract(t *testing.T) {
	start := time.Now().Round(time.Microsecond)
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
	assertContract := func(id types.FileContractID, expected contracts.Contract) {
		t.Helper()
		contract, err := store.Contract(context.Background(), id)
		if err != nil {
			t.Fatal("failed to fetch contract", err)
		} else if contract.Formation.Before(start) || contract.Formation.After(time.Now().Round(time.Microsecond)) {
			t.Fatalf("expected formation time to be after start time but not in the future")
		}
		contract.Formation = time.Time{}
		if !reflect.DeepEqual(contract, expected) {
			t.Fatalf("mismatch: \n%+v\n%+v", contract, expected)
		}
	}

	// form contract
	expectedFormed := contracts.Contract{
		ID:               types.FileContractID{1, 2, 3},
		HostKey:          hk,
		ProofHeight:      100,
		ExpirationHeight: 200,
		State:            contracts.ContractStatePending,

		ContractPrice:      types.Siacoins(1),
		InitialAllowance:   types.Siacoins(2),
		RemainingAllowance: types.Siacoins(2),
		MinerFee:           types.Siacoins(3),
		TotalCollateral:    types.Siacoins(4),

		Good: true,
	}
	err = store.AddFormedContract(context.Background(), expectedFormed.ID, expectedFormed.HostKey, expectedFormed.ProofHeight, expectedFormed.ExpirationHeight, expectedFormed.ContractPrice, expectedFormed.InitialAllowance, expectedFormed.MinerFee, expectedFormed.TotalCollateral)
	if err != nil {
		t.Fatal("failed to add formed contract", err)
	}
	assertContract(expectedFormed.ID, expectedFormed)

	// simulate using the contract and marking it not good
	modifyContract := func(contractID types.FileContractID) {
		err = store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			resp, err := tx.Exec(context.Background(), `
					UPDATE contracts
					SET state = 1, capacity = 2000, size = 1000, good = FALSE, append_sector_spending = 1, free_sector_spending = 2, fund_account_spending = 3, sector_roots_spending = 4
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

	expectedFormed.State = contracts.ContractStateActive
	expectedFormed.Capacity = 2000
	expectedFormed.Size = 1000
	expectedFormed.Good = false
	expectedFormed.Spending = contracts.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expectedFormed.ID, expectedFormed)

	// refresh the contract
	expectedRefreshed := contracts.Contract{
		ID:                 types.FileContractID{4, 5, 6},
		Capacity:           expectedFormed.Capacity,         // same capacity after refresh
		Size:               expectedFormed.Size,             // same size after refresh
		HostKey:            expectedFormed.HostKey,          // same host
		ProofHeight:        expectedFormed.ProofHeight,      // same proof height for refresh
		ExpirationHeight:   expectedFormed.ExpirationHeight, // same expiration height for refresh
		State:              contracts.ContractStatePending,  // refresh resets state
		ContractPrice:      types.Siacoins(2),               // new contract price
		InitialAllowance:   types.Siacoins(3),               // new initial allowance
		RemainingAllowance: types.Siacoins(3),               // matches initial allowance
		MinerFee:           types.Siacoins(4),               // new miner fee
		Good:               true,                            // refreshed contract is good
		RenewedFrom:        expectedFormed.ID,               // refreshed from formed contract
		Spending:           contracts.ContractSpending{},    // spending is reset
		UsedCollateral:     types.Siacoins(4),
		TotalCollateral:    types.Siacoins(5),
	}
	err = store.AddRenewedContract(context.Background(), contracts.AddRenewedContractParams{
		RenewedFrom:      expectedRefreshed.RenewedFrom,
		RenewedTo:        expectedRefreshed.ID,
		ProofHeight:      expectedRefreshed.ProofHeight,
		ExpirationHeight: expectedRefreshed.ExpirationHeight,
		ContractPrice:    expectedRefreshed.ContractPrice,
		Allowance:        expectedRefreshed.InitialAllowance,
		MinerFee:         expectedRefreshed.MinerFee,
		UsedCollateral:   expectedRefreshed.UsedCollateral,
		TotalCollateral:  expectedRefreshed.TotalCollateral,
	})
	if err != nil {
		t.Fatal("failed to add refreshed contract", err)
	}
	expectedFormed.RenewedTo = expectedRefreshed.ID
	assertContract(expectedFormed.ID, expectedFormed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)

	// modify the refreshed contract
	modifyContract(expectedRefreshed.ID)
	expectedRefreshed.State = contracts.ContractStateActive
	expectedRefreshed.Capacity = 2000
	expectedRefreshed.Size = 1000
	expectedRefreshed.Good = false
	expectedRefreshed.Spending = contracts.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expectedRefreshed.ID, expectedRefreshed)

	// renew the refreshed contract
	expectedRenewed := contracts.Contract{
		ID:                 types.FileContractID{7, 8, 9},
		Capacity:           expectedRefreshed.Size,                 // capacity shrinks to size upon renewal
		Size:               expectedRefreshed.Size,                 // same size after renewal
		HostKey:            expectedRefreshed.HostKey,              // same host
		ProofHeight:        expectedRefreshed.ProofHeight * 2,      // higher proof height for renew
		ExpirationHeight:   expectedRefreshed.ExpirationHeight * 2, // higher expiration height for renew
		State:              contracts.ContractStatePending,         // renewal resets state
		ContractPrice:      types.Siacoins(5),                      // new contract price
		InitialAllowance:   types.Siacoins(6),                      // new initial allowance
		RemainingAllowance: types.Siacoins(6),                      // matches initial allowance
		MinerFee:           types.Siacoins(7),                      // new miner fee
		Good:               true,                                   // renewed contract is good
		RenewedFrom:        expectedRefreshed.ID,                   // renewed from refreshed contract
		Spending:           contracts.ContractSpending{},           // spending is reset
		UsedCollateral:     types.Siacoins(4),
		TotalCollateral:    types.Siacoins(5),
	}
	err = store.AddRenewedContract(context.Background(), contracts.AddRenewedContractParams{
		RenewedFrom:      expectedRenewed.RenewedFrom,
		RenewedTo:        expectedRenewed.ID,
		ProofHeight:      expectedRenewed.ProofHeight,
		ExpirationHeight: expectedRenewed.ExpirationHeight,
		ContractPrice:    expectedRenewed.ContractPrice,
		Allowance:        expectedRenewed.InitialAllowance,
		MinerFee:         expectedRenewed.MinerFee,
		UsedCollateral:   expectedRenewed.UsedCollateral,
		TotalCollateral:  expectedRenewed.TotalCollateral,
	})
	if err != nil {
		t.Fatal("failed to add refreshed contract", err)
	}
	expectedRefreshed.RenewedTo = expectedRenewed.ID
	assertContract(expectedFormed.ID, expectedFormed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)
	assertContract(expectedRenewed.ID, expectedRenewed)
}

func TestRejectContracts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// helper to assert contract state
	assertContractState := func(id types.FileContractID, state contracts.ContractState) {
		t.Helper()
		contract, err := store.Contract(context.Background(), id)
		if err != nil {
			t.Fatal("failed to fetch contract", err)
		} else if contract.State != state {
			t.Fatalf("expected state %v, got %v", state, contract.State)
		}
	}

	// helper to modify contract
	updateStateAndFormation := func(id types.FileContractID, state contracts.ContractState, formation time.Time) {
		t.Helper()
		err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.UpdateContractState(id, state)
		})
		if err != nil {
			t.Fatal(err)
		}
		err = store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			_, err := tx.Exec(ctx, `UPDATE contracts SET formation = $1 WHERE contract_id = $2`, formation, sqlHash256(id))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// form 3 contracts
	now := time.Now()
	for i := range 3 {
		contractID := types.FileContractID{byte(i + 1)}
		err = store.AddFormedContract(context.Background(), contractID, hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4))
		if err != nil {
			t.Fatal("failed to add formed contract", err)
		}
		switch i {
		case 0:
			updateStateAndFormation(contractID, contracts.ContractStatePending, now) // recently formed
		case 1:
			updateStateAndFormation(contractID, contracts.ContractStatePending, now.Add(-time.Hour)) // formed an hour ago
		case 2:
			updateStateAndFormation(contractID, contracts.ContractStateActive, now.Add(-time.Hour)) // formed an hour ago but active
		}
	}

	// assert initial state of contracts
	recentID := types.FileContractID{1}
	assertContractState(recentID, contracts.ContractStatePending)
	oldID := types.FileContractID{2}
	assertContractState(oldID, contracts.ContractStatePending)
	activeID := types.FileContractID{3}
	assertContractState(activeID, contracts.ContractStateActive)

	// reject pending contracts older than 30 minutes
	err = store.RejectPendingContracts(context.Background(), now.Add(-30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	// assert state of contracts after rejection
	assertContractState(recentID, contracts.ContractStatePending)
	assertContractState(oldID, contracts.ContractStateRejected)
	assertContractState(activeID, contracts.ContractStateActive)
}

func TestUpdateContractElement(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	fce := types.V2FileContractElement{
		ID: types.FileContractID{1, 2, 3},
		StateElement: types.StateElement{
			LeafIndex:   1,
			MerkleProof: []types.Hash256{{3}, {2}, {1}},
		},
		V2FileContract: types.V2FileContract{}, // can be empty
	}

	assertFileContractElement := func() {
		t.Helper()
		ele, err := store.FileContractElement(fce.ID)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(ele, fce) {
			t.Fatalf("mismatch: \n%+v\n%+v", fce, ele)
		}
		var fces []types.V2FileContractElement
		err = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) (err error) {
			fces, err = tx.ContractElements()
			return
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, ele := range fces {
			if reflect.DeepEqual(ele, fce) {
				return // found
			}
		}
		t.Fatal("contract element not found")
	}

	// contract shouldn't exist
	assertKnownContract := func(known bool) {
		t.Helper()
		var storedKnown bool
		err = store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) (err error) {
			storedKnown, err = tx.IsKnownContract(fce.ID)
			return err
		})
		if err != nil {
			t.Fatal(err)
		} else if storedKnown != known {
			t.Fatalf("expected known=%v, got %v", known, storedKnown)
		}
	}
	assertKnownContract(false)

	// add a contract
	if err := store.AddFormedContract(context.Background(), fce.ID, hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4)); err != nil {
		t.Fatal(err)
	}
	assertKnownContract(true)

	updateElement := func() {
		t.Helper()
		err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.UpdateContractElements(fce)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// insert contract and assert state in db
	updateElement()
	assertFileContractElement()

	// update the elements fields
	fce.V2FileContract = types.V2FileContract{RevisionNumber: 12345}
	fce.StateElement.LeafIndex = 99
	fce.StateElement.MerkleProof = []types.Hash256{{9}, {8}, {7}}
	updateElement()
	assertFileContractElement()
}

// FileContractElement is a helper for fetching a types.V2FileContractElement
// from the database. It is within the test package since it's currently not
// used in production where we fetch elements in batches. If needed, this can be
// moved to `contracts.go` after adding a context to its list of args.
func (s *Store) FileContractElement(contractID types.FileContractID) (types.V2FileContractElement, error) {
	var fce types.V2FileContractElement
	err := s.transaction(context.Background(), func(ctx context.Context, tx *txn) (err error) {
		fce, err = scanContractElement(tx.QueryRow(ctx, `SELECT contract_id, contract, leaf_index, merkle_proof FROM contract_elements WHERE contract_id = $1`, sqlHash256(contractID)))
		return err
	})
	return fce, err
}

func TestUpdateContractState(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// create a contract
	contractID := types.FileContractID{1, 2, 3}

	updateState := func(state contracts.ContractState) {
		t.Helper()
		err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.UpdateContractState(contractID, state)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertState := func(state contracts.ContractState) {
		t.Helper()
		c, err := store.Contract(context.Background(), contractID)
		if err != nil {
			t.Fatal(err)
		} else if c.State != state {
			t.Fatalf("expected state %v, got %v", state, c.State)
		}
	}

	// add a contract
	if err := store.AddFormedContract(context.Background(), contractID, hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4)); err != nil {
		t.Fatal(err)
	}

	// run tests
	assertState(contracts.ContractStatePending) // fresh contract state
	updateState(contracts.ContractStateActive)  // set to active
	assertState(contracts.ContractStateActive)  // assert active
}

func TestMarkUnrenewableContractsBad(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	const proofHeight = 100

	// prepare 2 contracts, one for testing and another one that remains good
	// for the duration of the test to serve as a canary
	hk := types.PublicKey{1, 1, 1}
	fcid := types.FileContractID{1}
	goodFCID := types.FileContractID{2}

	// helper to assert state of contracts
	assertContractGood := func(good bool) {
		t.Helper()

		// check test contract
		contract, err := store.Contract(context.Background(), fcid)
		if err != nil {
			t.Fatal(err)
		} else if contract.Good != good {
			t.Fatalf("expected good=%v, got %v", good, contract.Good)
		}

		// check canary
		contract, err = store.Contract(context.Background(), goodFCID)
		if err != nil {
			t.Fatal(err)
		} else if !contract.Good {
			t.Fatal("canary should be good")
		}
	}

	// add a host and the contracts
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddFormedContract(context.Background(), fcid, hk, proofHeight, 9999, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4)); err != nil {
		t.Fatal(err)
	} else if err := store.AddFormedContract(context.Background(), goodFCID, hk, 8888, 9999, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(4)); err != nil {
		t.Fatal(err)
	}

	assertContractGood(true)
	store.MarkUnrenewableContractsBad(context.Background(), proofHeight-1)
	assertContractGood(true)
	store.MarkUnrenewableContractsBad(context.Background(), proofHeight)
	assertContractGood(false)
	store.MarkUnrenewableContractsBad(context.Background(), proofHeight+1)
	assertContractGood(false)
}

func TestSyncContract(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// add a contract
	contractID := types.FileContractID{1}
	if err := store.AddFormedContract(context.Background(), contractID, hk, 50, 100, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3), types.Siacoins(10000)); err != nil {
		t.Fatal(err)
	}

	// helper to sync and assert contract
	assertContract := func(params contracts.ContractSyncParams) {
		t.Helper()
		if err := store.SyncContract(context.Background(), contractID, params); err != nil {
			t.Fatal(err)
		}
		contract, err := store.Contract(context.Background(), contractID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Capacity != params.Capacity {
			t.Fatalf("expected capacity %d, got %d", params.Capacity, contract.Capacity)
		} else if contract.RemainingAllowance != params.RemainingAllowance {
			t.Fatalf("expected remaining allowance %d, got %d", params.RemainingAllowance, contract.RemainingAllowance)
		} else if contract.RevisionNumber != params.RevisionNumber {
			t.Fatalf("expected revision number %d, got %d", params.RevisionNumber, contract.RevisionNumber)
		} else if contract.Size != params.Size {
			t.Fatalf("expected size %d, got %d", params.Size, contract.Size)
		} else if contract.UsedCollateral != params.UsedCollateral {
			t.Fatalf("expected used collateral %d, got %d", params.UsedCollateral, contract.UsedCollateral)
		}
	}

	// assert setting it to some values works
	assertContract(contracts.ContractSyncParams{
		Capacity:           1000,
		RemainingAllowance: types.Siacoins(1),
		RevisionNumber:     100,
		Size:               900,
		UsedCollateral:     types.Siacoins(10),
	})

	// try again with different values
	assertContract(contracts.ContractSyncParams{
		Capacity:           2000,
		RemainingAllowance: types.Siacoins(2),
		RevisionNumber:     200,
		Size:               1900,
		UsedCollateral:     types.Siacoins(20),
	})
}
