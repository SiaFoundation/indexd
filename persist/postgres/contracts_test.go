package postgres

import (
	"context"
	"fmt"
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
	if err := store.AddFormedContract(context.Background(), fce.ID, hk, 50, fce.V2FileContract.ExpirationHeight, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3)); err != nil {
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
		if err := store.AddFormedContract(context.Background(), contractID, hk, 50, expirationHeight, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3)); err != nil {
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

		ContractPrice:    types.Siacoins(1),
		InitialAllowance: types.Siacoins(2),
		MinerFee:         types.Siacoins(3),

		Good: true,
	}
	err = store.AddFormedContract(context.Background(), expectedFormed.ID, expectedFormed.HostKey, expectedFormed.ProofHeight, expectedFormed.ExpirationHeight, expectedFormed.ContractPrice, expectedFormed.InitialAllowance, expectedFormed.MinerFee)
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
		ID:               types.FileContractID{4, 5, 6},
		Capacity:         expectedFormed.Capacity,         // same capacity after refresh
		Size:             expectedFormed.Size,             // same size after refresh
		HostKey:          expectedFormed.HostKey,          // same host
		ProofHeight:      expectedFormed.ProofHeight,      // same proof height for refresh
		ExpirationHeight: expectedFormed.ExpirationHeight, // same expiration height for refresh
		State:            contracts.ContractStatePending,  // refresh resets state
		ContractPrice:    types.Siacoins(2),               // new contract price
		InitialAllowance: types.Siacoins(3),               // new initial allowance
		MinerFee:         types.Siacoins(4),               // new miner fee
		Good:             true,                            // refreshed contract is good
		RenewedFrom:      expectedFormed.ID,               // refreshed from formed contract
		Spending:         contracts.ContractSpending{},    // spending is reset
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
		ID:               types.FileContractID{7, 8, 9},
		Capacity:         expectedRefreshed.Size,                 // capacity shrinks to size upon renewal
		Size:             expectedRefreshed.Size,                 // same size after renewal
		HostKey:          expectedRefreshed.HostKey,              // same host
		ProofHeight:      expectedRefreshed.ProofHeight * 2,      // higher proof height for renew
		ExpirationHeight: expectedRefreshed.ExpirationHeight * 2, // higher expiration height for renew
		State:            contracts.ContractStatePending,         // renewal resets state
		ContractPrice:    types.Siacoins(5),                      // new contract price
		InitialAllowance: types.Siacoins(6),                      // new initial allowance
		MinerFee:         types.Siacoins(7),                      // new miner fee
		Good:             true,                                   // renewed contract is good
		RenewedFrom:      expectedRefreshed.ID,                   // renewed from refreshed contract
		Spending:         contracts.ContractSpending{},           // spending is reset
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
		err = store.AddFormedContract(context.Background(), contractID, hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3))
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

func TestSetContractGood(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// helpers
	assertContractGood := func(id int64, good bool) {
		t.Helper()
		err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			var got bool
			if err := tx.QueryRow(ctx, `SELECT good FROM contracts WHERE id = $1`, id).Scan(&got); err != nil {
				t.Fatal(err)
			} else if got != good {
				t.Fatalf("expected good=%v, got %v", good, got)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	setContractGood := func(id int64, good bool) {
		t.Helper()
		if !good {
			if err := store.SetContractBad(types.FileContractID{byte(id)}); err != nil {
				t.Fatal("failed to set contract.'good'", err)
			}
		} else {
			if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
				_, err := tx.Exec(ctx, `UPDATE contracts SET good = TRUE WHERE contract_id = $1`, sqlHash256{byte(id)})
				if err != nil {
					return fmt.Errorf("failed to update contract.'good': %w", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// form contracts
	for i := range 3 {
		expectedFormed := contracts.Contract{
			ID:      types.FileContractID{byte(i + 1)},
			HostKey: hk,
		}
		err = store.AddFormedContract(context.Background(), expectedFormed.ID, expectedFormed.HostKey, expectedFormed.ProofHeight, expectedFormed.ExpirationHeight, expectedFormed.ContractPrice, expectedFormed.InitialAllowance, expectedFormed.MinerFee)
		if err != nil {
			t.Fatal("failed to add formed contract", err)
		}
		assertContractGood(int64(i+1), true) // good by default
		setContractGood(int64(i+1), false)   // set bad
	}

	// all bad
	assertContractGood(1, false)
	assertContractGood(2, false)
	assertContractGood(3, false)

	// 1 and 3 good
	setContractGood(1, true)
	setContractGood(3, true)
	assertContractGood(1, true)
	assertContractGood(2, false)
	assertContractGood(3, true)

	// 2 good
	setContractGood(1, false)
	setContractGood(2, true)
	setContractGood(3, false)
	assertContractGood(1, false)
	assertContractGood(2, true)
	assertContractGood(3, false)
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
	if err := store.AddFormedContract(context.Background(), fce.ID, hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3)); err != nil {
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
		fce, err = scanContractElement(tx.QueryRow(ctx, `
SELECT c.contract_id, fce.contract, fce.leaf_index, fce.merkle_proof
FROM contract_elements fce
INNER JOIN contracts c ON c.id = fce.contract_id
WHERE c.contract_id = $1`, sqlHash256(contractID)))
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
	if err := store.AddFormedContract(context.Background(), contractID, hk, 100, 200, types.Siacoins(1), types.Siacoins(2), types.Siacoins(3)); err != nil {
		t.Fatal(err)
	}

	// run tests
	assertState(contracts.ContractStatePending) // fresh contract state
	updateState(contracts.ContractStateActive)  // set to active
	assertState(contracts.ContractStateActive)  // assert active
}
