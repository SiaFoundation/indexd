package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestContracts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host with three contracts
	hk := store.addTestHost(t)
	fcid1 := store.addTestContract(t, hk, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk, types.FileContractID{2})
	fcid3 := store.addTestContract(t, hk, types.FileContractID{3})

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

func TestContractElement(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := store.addTestHost(t)

	// assert contract element is not found
	_, err := store.ContractElement(context.Background(), types.FileContractID(hk))
	if !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal(err)
	}

	// add a contract and an element
	fcid := store.addTestContract(t, hk)
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractElements(types.V2FileContractElement{
			ID: fcid,
			StateElement: types.StateElement{
				LeafIndex:   1,
				MerkleProof: []types.Hash256{{1}},
			},
			V2FileContract: types.V2FileContract{
				ExpirationHeight: 100,
				HostPublicKey:    hk,
			},
		})
	}); err != nil {
		t.Fatal(err)
	}

	// assert contract element is found
	_, err = store.ContractElement(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	}
}

func TestContractElementsForBroadcast(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := store.addTestHost(t)

	// add a contract
	fcid := types.FileContractID{1}
	revision := newTestRevision(hk)
	if err := store.AddFormedContract(context.Background(), hk, fcid, revision, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
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
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(context.Background(), types.ChainIndex{Height: revision.ExpirationHeight + 10})
	})
	if err != nil {
		t.Fatal(err)
	}

	// no contracts to broadcast since we haven't added the element yet
	assertContractsToBroadcast(1, 0)

	fce := types.V2FileContractElement{
		ID:             fcid,
		StateElement:   types.StateElement{LeafIndex: 1, MerkleProof: []types.Hash256{{1}}},
		V2FileContract: revision,
	}

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

func TestContractsForBroadcasting(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host with two contracts
	hk := store.addTestHost(t)
	fcid1 := store.addTestContract(t, hk, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk, types.FileContractID{2})

	// tweak timestamp to assert order next
	now := time.Now()
	store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		_, err1 := tx.Exec(ctx, `UPDATE contracts SET last_broadcast_attempt = $1 WHERE contract_id = $2`, now.Add(-1*time.Minute), sqlHash256(fcid1))
		_, err2 := tx.Exec(ctx, `UPDATE contracts SET last_broadcast_attempt = $1 WHERE contract_id = $2`, now.Add(-2*time.Minute), sqlHash256(fcid2))
		return errors.Join(err1, err2)
	})

	// assert both are returned
	res, err := store.ContractsForBroadcasting(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 2 {
		t.Fatalf("expected 2 contracts, got %d", len(res))
	} else if res[0] != fcid2 || res[1] != fcid1 {
		t.Fatalf("expected %v, %v, got %v, %v", fcid2, fcid1, res[0], res[1])
	}

	// assert limit is respected
	res, err = store.ContractsForBroadcasting(context.Background(), now, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(res))
	}

	// mark broadcast attempt
	err = store.MarkBroadcastAttempt(context.Background(), res[0])
	if err != nil {
		t.Fatal(err)
	}

	// assert only one gets returned
	res, err = store.ContractsForBroadcasting(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 1 {
		t.Fatalf("expected 1 contracts, got %d", len(res))
	}

	// renew the contract
	if err := store.AddRenewedContract(context.Background(), res[0], types.FileContractID{9, 9, 9}, types.V2FileContract{}, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}

	// assert none are returned
	res, err = store.ContractsForBroadcasting(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 0 {
		t.Fatalf("expected 0 contracts, got %d", len(res))
	}
}

func TestContractsForFunding(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	updateAllowance := func(contractID types.FileContractID, allowance types.Currency) {
		t.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE contracts SET initial_allowance = $1, remaining_allowance = $1 WHERE contract_id = $2`, sqlCurrency(allowance), sqlHash256(contractID))
		if err != nil {
			t.Fatal(err)
		}
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add four contracts for h1
	fcid1 := store.addTestContract(t, hk1, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk1, types.FileContractID{2})
	fcid3 := store.addTestContract(t, hk1, types.FileContractID{3})
	fcid4 := store.addTestContract(t, hk1, types.FileContractID{4})

	// update their allowance
	updateAllowance(fcid1, types.Siacoins(1))
	updateAllowance(fcid2, types.Siacoins(3))
	updateAllowance(fcid3, types.Siacoins(2))
	updateAllowance(fcid4, types.ZeroCurrency)

	// assert only 3 contracts are returned for h1, in order, the fourth has no
	// remaining allowance and h2 doesn't have contracts yet
	if fcids, err := store.ContractsForFunding(context.Background(), hk1, 10); err != nil {
		t.Fatal("unexpected", err)
	} else if len(fcids) != 3 {
		t.Fatalf("expected 3 contract, got %d", len(fcids))
	} else if fcids[0] != fcid2 {
		t.Fatalf("expected contract %v, got %v", fcid2, fcids[0])
	} else if fcids[1] != fcid3 {
		t.Fatalf("expected contract %v, got %v", fcid3, fcids[1])
	} else if fcids[2] != fcid1 {
		t.Fatalf("expected contract %v, got %v", fcid1, fcids[2])
	} else if fcids, err := store.ContractsForFunding(context.Background(), hk1, 1); err != nil {
		t.Fatal("unexpected", err)
	} else if len(fcids) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(fcids))
	} else if fcids, err := store.ContractsForFunding(context.Background(), hk2, 10); err != nil {
		t.Fatal("unexpected", err)
	} else if len(fcids) != 0 {
		t.Fatalf("expected no contracts, got %d", len(fcids))
	}

	// mark first contract as resolved
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractState(fcid1, contracts.ContractStateResolved)
	}); err != nil {
		t.Fatal(err)
	}

	// mark second one as bad
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, `UPDATE contracts SET good = FALSE WHERE contract_id = $1`, sqlHash256(fcid2))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// add a contract for h2 with allowance
	fcid5 := store.addTestContract(t, hk2, types.FileContractID{5})
	updateAllowance(fcid5, types.Siacoins(1))

	// assert we have only one contract for h1 now, and one on h2
	if fcids, err := store.ContractsForFunding(context.Background(), hk1, 10); err != nil {
		t.Fatal("unexpected", err)
	} else if len(fcids) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(fcids))
	} else if fcids[0] != fcid3 {
		t.Fatalf("expected contract %v, got %v", fcid3, fcids[0])
	} else if fcids, err := store.ContractsForFunding(context.Background(), hk2, 10); err != nil {
		t.Fatal("unexpected", err)
	} else if len(fcids) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(fcids))
	} else if fcids[0] != fcid5 {
		t.Fatalf("expected contract %v, got %v", fcid5, fcids[0])
	}
}

func TestContractsForPinning(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	addContract := func(hk types.PublicKey, fcid types.FileContractID, allowance types.Currency, size, capacity uint64, state contracts.ContractState, good bool) {
		t.Helper()
		store.addTestContract(t, hk, fcid)
		query := `UPDATE contracts SET size = $1, capacity = $2, state = $3, good = $4, initial_allowance = $5, remaining_allowance = $5 WHERE contract_id = $6`
		_, err := store.pool.Exec(context.Background(), query, size, capacity, sqlContractState(state), good, sqlCurrency(allowance), sqlHash256(fcid))
		if err != nil {
			t.Fatal(err)
		}
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add contracts for h1
	const maxContractSize = 499
	addContract(hk1, types.FileContractID{1}, types.ZeroCurrency, 100, 100, contracts.ContractStateActive, true)       // no allowance
	addContract(hk1, types.FileContractID{2}, types.NewCurrency64(1), 100, 100, contracts.ContractStateResolved, true) // resolved
	addContract(hk1, types.FileContractID{3}, types.NewCurrency64(1), 100, 100, contracts.ContractStateActive, false)  // bad
	addContract(hk1, types.FileContractID{4}, types.NewCurrency64(1), 500, 500, contracts.ContractStateActive, true)   // too big
	addContract(hk1, types.FileContractID{5}, types.NewCurrency64(1), 100, 100, contracts.ContractStateActive, true)   // ok - low capacity
	addContract(hk1, types.FileContractID{6}, types.NewCurrency64(1), 300, 400, contracts.ContractStateActive, true)   // ok - matching capacity - large contract
	addContract(hk1, types.FileContractID{7}, types.NewCurrency64(1), 200, 400, contracts.ContractStateActive, true)   // ok - matching capacity - smaller contract

	// add contracts for h2
	addContract(hk2, types.FileContractID{8}, types.NewCurrency64(1), 100, 100, contracts.ContractStateActive, true) // ok

	// assert contracts for pinning for h1
	contractIDs, err := store.ContractsForPinning(context.Background(), hk1, maxContractSize)
	if err != nil {
		t.Fatal(err)
	} else if len(contractIDs) != 3 {
		t.Fatalf("expected 3 contracts, got %d", len(contractIDs))
	} else if contractIDs[0] != (types.FileContractID{6}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{6}, contractIDs[0])
	} else if contractIDs[1] != (types.FileContractID{7}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{7}, contractIDs[1])
	} else if contractIDs[2] != (types.FileContractID{5}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{5}, contractIDs[2])
	}

	// assert contracts for pinning for h2
	contractIDs, err = store.ContractsForPinning(context.Background(), hk2, maxContractSize)
	if err != nil {
		t.Fatal(err)
	} else if len(contractIDs) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contractIDs))
	} else if contractIDs[0] != (types.FileContractID{8}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{8}, contractIDs[0])
	}
}

func TestContractsForPruning(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	addContract := func(hk types.PublicKey, fcid types.FileContractID, allowance types.Currency, size uint64, state contracts.ContractState, good bool, nextPrune time.Time) {
		t.Helper()
		store.addTestContract(t, hk, fcid)
		query := `UPDATE contracts SET state = $1, good = $2, size = $3, capacity = $4, next_prune = $5, initial_allowance = $6, remaining_allowance = $6 WHERE contract_id = $7`
		_, err := store.pool.Exec(context.Background(), query, sqlContractState(state), good, size, size, nextPrune, sqlCurrency(allowance), sqlHash256(fcid))
		if err != nil {
			t.Fatal(err)
		}
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	now := time.Now().Round(time.Microsecond).Add(-time.Microsecond)
	tomorrow := now.Add(24 * time.Hour)

	// add contracts for h1
	addContract(hk1, types.FileContractID{1}, types.ZeroCurrency, 100, contracts.ContractStateActive, true, now)          // no allowance
	addContract(hk1, types.FileContractID{2}, types.NewCurrency64(1), 100, contracts.ContractStateResolved, true, now)    // resolved
	addContract(hk1, types.FileContractID{3}, types.NewCurrency64(1), 100, contracts.ContractStateActive, false, now)     // bad
	addContract(hk1, types.FileContractID{4}, types.NewCurrency64(1), 100, contracts.ContractStateActive, true, tomorrow) // pruned recently
	addContract(hk1, types.FileContractID{5}, types.NewCurrency64(1), 100, contracts.ContractStateActive, true, now)      // ok - small size
	addContract(hk1, types.FileContractID{6}, types.NewCurrency64(1), 200, contracts.ContractStateActive, true, now)      // ok - big size

	// add contracts for h2
	addContract(hk2, types.FileContractID{7}, types.NewCurrency64(1), 100, contracts.ContractStateActive, true, now) // ok

	// assert contracts for pruning for h1
	contractIDs, err := store.ContractsForPruning(context.Background(), hk1)
	if err != nil {
		t.Fatal(err)
	} else if len(contractIDs) != 2 {
		t.Fatalf("expected 2 contracts, got %d", len(contractIDs))
	} else if contractIDs[0] != (types.FileContractID{6}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{6}, contractIDs[0])
	} else if contractIDs[1] != (types.FileContractID{5}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{5}, contractIDs[1])
	}

	// assert contracts for pruning for h2
	contractIDs, err = store.ContractsForPruning(context.Background(), hk2)
	if err != nil {
		t.Fatal(err)
	} else if len(contractIDs) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contractIDs))
	} else if contractIDs[0] != (types.FileContractID{7}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{7}, contractIDs[0])
	}
}

func TestPrunableContractRoots(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		t.Fatal("failed to add account:", err)
	}

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add a contract for each host
	fcid1 := store.addTestContract(t, hk1, types.FileContractID{1})
	fcid2 := store.addTestContract(t, hk2, types.FileContractID{2})

	// prepare roots
	roots := []types.Hash256{
		frand.Entropy256(),
		frand.Entropy256(),
		frand.Entropy256(),
		frand.Entropy256(),
	}

	// pin two slabs to add sectors
	_, err := store.PinSlab(context.Background(), account, time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     11,
		Sectors: []slabs.SectorPinParams{
			{Root: roots[0], HostKey: hk1},
			{Root: roots[2], HostKey: hk2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	slabID, err := store.PinSlab(context.Background(), account, time.Now(), slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     11,
		Sectors: []slabs.SectorPinParams{
			{Root: roots[1], HostKey: hk1},
			{Root: roots[3], HostKey: hk2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// pin sectors for h1
	if sectors, err := store.UnpinnedSectors(context.Background(), hk1, 10); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(context.Background(), fcid1, sectors); err != nil {
		t.Fatal(err)
	}

	// pin sectors for h2
	if sectors, err := store.UnpinnedSectors(context.Background(), hk2, 10); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(context.Background(), fcid2, sectors); err != nil {
		t.Fatal(err)
	}

	// assert no prunable roots for either contract
	if indices, err := store.PrunableContractRoots(context.Background(), fcid1, roots[:2]); err != nil {
		t.Fatal(err)
	} else if len(indices) != 0 {
		t.Fatalf("unexpected prunable indices, %+v", indices)
	} else if indices, err := store.PrunableContractRoots(context.Background(), fcid2, roots[2:]); err != nil {
		t.Fatal(err)
	} else if len(indices) != 0 {
		t.Fatalf("unexpected prunable indices, %+v", indices)
	}

	// unpin the second slab to remove sectors for both hosts
	if err := store.UnpinSlab(context.Background(), account, slabID); err != nil {
		t.Fatal(err)
	}

	// assert prunable roots for both contracts
	if prunable, err := store.PrunableContractRoots(context.Background(), fcid1, roots[:2]); err != nil {
		t.Fatal(err)
	} else if len(prunable) != 1 || prunable[0] != roots[1] {
		t.Fatalf("unexpected prunable roots, %+v", prunable)
	} else if prunable, err := store.PrunableContractRoots(context.Background(), fcid2, roots[2:]); err != nil {
		t.Fatal(err)
	} else if len(prunable) != 1 || prunable[0] != roots[3] {
		t.Fatalf("unexpected prunable roots, %+v", prunable)
	}
}
func TestPruneExpiredContractElements(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := store.addTestHost(t)

	addContract := func(expirationHeight uint64) types.FileContractID {
		t.Helper()
		var contractID types.FileContractID
		frand.Read(contractID[:])

		revision := newTestRevision(hk)
		revision.ExpirationHeight = expirationHeight
		if err := store.AddFormedContract(context.Background(), hk, contractID, revision, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
			return tx.UpdateContractElements(types.V2FileContractElement{
				ID: contractID,
			})
		}); err != nil {
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
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateLastScannedIndex(context.Background(), types.ChainIndex{Height: bh})
	}); err != nil {
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
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		var count int
		err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM contracts").Scan(&count)
		if err != nil {
			return err
		} else if count != 3 {
			t.Fatalf("expected 3 contracts, got %d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFormRenewContract(t *testing.T) {
	start := time.Now().Round(time.Microsecond)
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := store.addTestHost(t)

	// define helper to assert contract in db
	assertContract := func(id types.FileContractID, expected contracts.Contract) {
		t.Helper()

		contract, err := store.Contract(context.Background(), id)
		if err != nil {
			t.Fatal("failed to fetch contract", err)
		} else if contract.Formation.Before(start) || contract.Formation.After(time.Now().Round(time.Microsecond)) {
			t.Fatalf("expected formation time to be after start time but not in the future")
		}

		contract.Formation = time.Time{}
		contract.LastBroadcastAttempt = time.Time{}
		contract.NextPrune = time.Time{}
		if !reflect.DeepEqual(contract, expected) {
			t.Fatalf("mismatch: \n%+v\n%+v", contract, expected)
		}
	}

	// define helper to simulate contract usage
	simulateUsage := func(contractID types.FileContractID) {
		t.Helper()
		if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
			resp, err := tx.Exec(context.Background(), `UPDATE contracts SET state = 1, good = FALSE, append_sector_spending = 1, free_sector_spending = 2, fund_account_spending = 3, sector_roots_spending = 4 WHERE contract_id = $1`, sqlHash256(contractID))
			if err != nil {
				return err
			} else if resp.RowsAffected() != 1 {
				t.Fatalf("expected 1 row to be affected, got %d", resp.RowsAffected())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// form contract
	formation := newTestRevision(hk)
	formation.RenterOutput.Value = types.NewCurrency64(math.MaxUint64) // initial allowance

	expectedFormed := contracts.Contract{
		ID:      types.FileContractID{1},
		HostKey: hk,
		State:   contracts.ContractStatePending,

		// revision fields
		RevisionNumber:     formation.RevisionNumber,
		ProofHeight:        formation.ProofHeight,
		ExpirationHeight:   formation.ExpirationHeight,
		Capacity:           formation.Capacity,
		Size:               formation.Filesize,
		InitialAllowance:   formation.RenterOutput.Value,
		RemainingAllowance: formation.RenterOutput.Value,
		TotalCollateral:    formation.TotalCollateral,

		ContractPrice: types.Siacoins(1),
		MinerFee:      types.Siacoins(2),

		Good: true,
	}
	if err := store.AddFormedContract(context.Background(), hk, expectedFormed.ID, formation, expectedFormed.ContractPrice, expectedFormed.InitialAllowance, expectedFormed.MinerFee); err != nil {
		t.Fatal("failed to add formed contract", err)
	}

	// assert the contract matches the expectations
	assertContract(expectedFormed.ID, expectedFormed)

	// assert the contract sector mapping exists
	var mapID int64
	if err := store.pool.QueryRow(context.Background(), `SELECT id FROM contract_sectors_map WHERE contract_id = $1`, sqlHash256(expectedFormed.ID)).Scan(&mapID); err != nil {
		t.Fatal(err)
	}

	// simulate usage, assert the contract is active, bad and has spending
	simulateUsage(expectedFormed.ID)
	expectedFormed.State = contracts.ContractStateActive
	expectedFormed.Good = false
	expectedFormed.Spending = contracts.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expectedFormed.ID, expectedFormed)

	// prepare a refresh of the contract, we want to assert spending gets reset,
	// refreshed contracts are good and the renewed from is set
	refresh := formation
	refresh.RenterOutput.Value = types.NewCurrency64(math.MaxUint64) // new initial allowance

	expectedRefreshed := contracts.Contract{
		ID:          types.FileContractID{2},
		HostKey:     expectedFormed.HostKey, // same host
		RenewedFrom: expectedFormed.ID,      // refreshed from formed contract

		// revision fields
		RevisionNumber:     refresh.RevisionNumber,
		ProofHeight:        refresh.ProofHeight,
		ExpirationHeight:   refresh.ExpirationHeight,
		Capacity:           refresh.Capacity,
		Size:               refresh.Filesize,
		InitialAllowance:   refresh.RenterOutput.Value,
		RemainingAllowance: refresh.RenterOutput.Value,
		TotalCollateral:    refresh.TotalCollateral,

		UsedCollateral: refresh.TotalCollateral.Div64(2), // updated used collateral
		ContractPrice:  types.Siacoins(2),                // new contract price
		MinerFee:       types.Siacoins(4),                // new miner fee

		State: contracts.ContractStatePending, // refresh resets state
		Good:  true,                           // refreshed contract is good

		Spending: contracts.ContractSpending{}, // spending is reset
	}

	if err := store.AddRenewedContract(context.Background(), expectedRefreshed.RenewedFrom, expectedRefreshed.ID, refresh, expectedRefreshed.ContractPrice, expectedRefreshed.MinerFee, expectedRefreshed.UsedCollateral); err != nil {
		t.Fatal("failed to add refreshed contract", err)
	}
	expectedFormed.RenewedTo = expectedRefreshed.ID
	assertContract(expectedFormed.ID, expectedFormed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)

	// modify the refreshed contract
	simulateUsage(expectedRefreshed.ID)
	expectedRefreshed.State = contracts.ContractStateActive
	expectedRefreshed.Good = false
	expectedRefreshed.Spending = contracts.ContractSpending{
		AppendSector: types.NewCurrency64(1),
		FreeSector:   types.NewCurrency64(2),
		FundAccount:  types.NewCurrency64(3),
		SectorRoots:  types.NewCurrency64(4),
	}
	assertContract(expectedRefreshed.ID, expectedRefreshed)

	// renew the refreshed contract
	renewal := refresh
	renewal.RenterOutput.Value = types.Siacoins(6) // new initial allowance
	renewal.Capacity = renewal.Filesize            // capacity shrinks to size upon renewal
	renewal.ProofHeight *= 2                       // higher proof height for renew
	renewal.ExpirationHeight *= 2                  // higher expiration height for renew

	expectedRenewed := contracts.Contract{
		ID:          types.FileContractID{7, 8, 9},
		HostKey:     expectedRefreshed.HostKey, // same host
		RenewedFrom: expectedRefreshed.ID,      // renewed from refreshed contract

		// revision fields
		RevisionNumber:     renewal.RevisionNumber,
		ProofHeight:        renewal.ProofHeight,
		ExpirationHeight:   renewal.ExpirationHeight,
		Capacity:           renewal.Capacity,
		Size:               renewal.Filesize,
		InitialAllowance:   renewal.RenterOutput.Value,
		RemainingAllowance: renewal.RenterOutput.Value,
		TotalCollateral:    renewal.TotalCollateral,

		UsedCollateral: renewal.TotalCollateral.Div64(4), // updated used collateral
		ContractPrice:  types.Siacoins(5),                // new contract price
		MinerFee:       types.Siacoins(7),                // new miner fee

		State: contracts.ContractStatePending, // renewal resets state
		Good:  true,                           // renewed contract is good

		Spending: contracts.ContractSpending{}, // spending is reset
	}

	if err := store.AddRenewedContract(context.Background(), expectedRenewed.RenewedFrom, expectedRenewed.ID, renewal, expectedRenewed.ContractPrice, expectedRenewed.MinerFee, expectedRenewed.UsedCollateral); err != nil {
		t.Fatal("failed to add renewed contract", err)
	}
	expectedRefreshed.RenewedTo = expectedRenewed.ID
	assertContract(expectedFormed.ID, expectedFormed)
	assertContract(expectedRefreshed.ID, expectedRefreshed)
	assertContract(expectedRenewed.ID, expectedRenewed)

	// assert `contract_sectors_map` entry was updated when renewing the contract
	var currID int64
	if err := store.pool.QueryRow(context.Background(), `SELECT id FROM contract_sectors_map WHERE contract_id = $1`, sqlHash256(expectedRenewed.ID)).Scan(&mapID); err != nil {
		t.Fatal(err)
	} else if currID == mapID {
		t.Fatalf("expected contract_sectors_map entry to be updated, got %d", mapID)
	}
}

func TestRejectContracts(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := store.addTestHost(t)

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
		contractID := store.addTestContract(t, hk, types.FileContractID{byte(i + 1)})
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
	if err := store.RejectPendingContracts(context.Background(), now.Add(-30*time.Minute)); err != nil {
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
	hk := store.addTestHost(t)

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
		if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) (err error) {
			storedKnown, err = tx.IsKnownContract(fce.ID)
			return err
		}); err != nil {
			t.Fatal(err)
		} else if storedKnown != known {
			t.Fatalf("expected known=%v, got %v", known, storedKnown)
		}
	}
	assertKnownContract(false)

	// add a contract
	store.addTestContract(t, hk, fce.ID)
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
	hk := store.addTestHost(t)

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

	// run tests
	store.addTestContract(t, hk, contractID)
	assertState(contracts.ContractStatePending) // fresh contract state
	updateState(contracts.ContractStateActive)  // set to active
	assertState(contracts.ContractStateActive)  // assert active
}

func TestUpdateNextPruned(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host with one contract
	hk := store.addTestHost(t)
	fcid := store.addTestContract(t, hk)

	// assert contract is marked to be pruned within 24h
	tomorrow := time.Now().Add(24 * time.Hour)
	if contract, err := store.Contract(context.Background(), fcid); err != nil {
		t.Fatal(err)
	} else if !(tomorrow.Add(-time.Second).Before(contract.NextPrune) && tomorrow.Add(time.Second).After(contract.NextPrune)) {
		t.Fatal("contract should be scheduled for pruning 24h from now")
	}

	// mark as pruned and assert contract was updated correctly
	oneHourFromNow := time.Now().Add(time.Hour).Round(time.Microsecond)
	if err := store.UpdateNextPrune(context.Background(), fcid, oneHourFromNow); err != nil {
		t.Fatal(err)
	} else if contract, err := store.Contract(context.Background(), fcid); err != nil {
		t.Fatal(err)
	} else if !contract.NextPrune.Equal(oneHourFromNow) {
		t.Fatal("contract next_prune should be updated")
	}

	// assert field is decorated in store.Contracts as well
	if contracts, err := store.Contracts(context.Background(), 0, 10); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contracts))
	} else if contracts[0].NextPrune.IsZero() {
		t.Fatal("contract should be decorated with the next prune time")
	}
}

func TestMarkUnrenewableContractsBad(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	const proofHeight = 100

	// add a hosts
	hk := store.addTestHost(t)

	// prepare 2 contracts, one for testing and another one that remains good
	// for the duration of the test to serve as a canary
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

	revision1 := newTestRevision(hk)
	revision1.ProofHeight = proofHeight
	revision1.ExpirationHeight = 9999

	revision2 := newTestRevision(hk)
	revision2.ProofHeight = 8888
	revision2.ExpirationHeight = 9999

	if err := errors.Join(
		store.AddFormedContract(context.Background(), hk, fcid, revision1, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency),
		store.AddFormedContract(context.Background(), hk, goodFCID, revision2, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency),
	); err != nil {
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

func TestUpdateContractRevision(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	hk := store.addTestHost(t)
	contractID := store.addTestContract(t, hk)

	revision, renewed, err := store.ContractRevision(context.Background(), contractID)
	if err != nil {
		t.Fatal(err)
	} else if renewed {
		t.Fatal("expected contract to not be renewed")
	}
	expectedRevision := newTestRevision(hk)
	if revision != expectedRevision {
		t.Fatalf("expected revision to be %v, got %v", expectedRevision, revision)
	}

	update := revision
	update.Capacity *= 2
	update.Filesize *= 2
	update.RevisionNumber++
	update.RenterOutput.Value = types.NewCurrency64(1000)
	update.MissedHostValue = types.NewCurrency64(100)

	if err := store.UpdateContractRevision(context.Background(), contractID, update); err != nil {
		t.Fatal(err)
	} else if revision, renewed, err := store.ContractRevision(context.Background(), contractID); err != nil {
		t.Fatal(err)
	} else if renewed {
		t.Fatal("expected contract to not be renewed")
	} else if revision != update {
		t.Fatalf("expected revision to be %v, got %v", update, revision)
	}

	if err := store.AddRenewedContract(context.Background(), contractID, types.FileContractID{2}, newTestRevision(hk), types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	} else if revision, renewed, err := store.ContractRevision(context.Background(), contractID); err != nil {
		t.Fatal(err)
	} else if !renewed {
		t.Fatal("expected contract to be renewed")
	} else if revision != update {
		t.Fatalf("expected revision to be %v, got %v", update, revision)
	}

	// assert [contracts.ErrNotFound] is returned for non-existing contract
	if _, _, err := store.ContractRevision(context.Background(), types.FileContractID{}); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("expected ErrContractNotFound, got %v", err)
	} else if err := store.UpdateContractRevision(context.Background(), types.FileContractID{}, newTestRevision(types.PublicKey{})); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("expected ErrContractNotFound, got %v", err)
	}
}

func TestMarkBroadcastAttempt(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := store.addTestHost(t)

	// assert broadcast attempt is defaulted
	fcid := store.addTestContract(t, hk)
	contract, err := store.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	} else if contract.LastBroadcastAttempt.IsZero() {
		t.Fatal("unexpected", contract.LastBroadcastAttempt)
	}

	// mark broadcast attempt
	err = store.MarkBroadcastAttempt(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	}

	// assert timestamp was updated
	updated, err := store.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	} else if updated.LastBroadcastAttempt.IsZero() || !updated.LastBroadcastAttempt.After(contract.LastBroadcastAttempt) {
		t.Fatal("unexpected", contract.LastBroadcastAttempt, updated.LastBroadcastAttempt)
	}
}

// BenchmarkContracts is a benchmark to ensure the performance of
// all methods on the store that return either a contract or a list of
// contract IDs.
func BenchmarkContracts(b *testing.B) {
	const (
		maxContractSize     = 10 * 1 << 40 // 10TB
		numContractsPerHost = 100
		numHosts            = 1000
	)

	randomTime := func() time.Time {
		maxOneHour := time.Duration(frand.Uint64n(60*60)) * time.Second
		return time.Now().Add(-30 * time.Minute).Add(maxOneHour)
	}

	// prepare database
	store := initPostgres(b, zap.NewNop())
	hosts := make([]types.PublicKey, 0, numHosts)
	contractIDs := make([]types.FileContractID, 0, numHosts*numContractsPerHost)
	if err := store.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		for range numHosts {
			var hostID int64
			hk := types.GeneratePrivateKey().PublicKey()
			err := tx.QueryRow(ctx, `INSERT INTO hosts (public_key, last_announcement) VALUES ($1, NOW()) RETURNING id;`, sqlPublicKey(hk)).Scan(&hostID)
			if err != nil {
				return err
			}

			hostContractIDs := make([]types.FileContractID, numContractsPerHost)
			for i := range numContractsPerHost {
				revision := newTestRevision(hk)
				frand.Read(hostContractIDs[i][:])
				size := frand.Uint64n(1e9)
				if _, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, raw_revision, proof_height, expiration_height, contract_price, initial_allowance, miner_fee, total_collateral, remaining_allowance, state, good, size, capacity, last_broadcast_attempt, next_prune) VALUES ($1, $2, $3, 0, 0, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);`,
					hostID,
					sqlHash256(hostContractIDs[i][:]),
					sqlFileContract(revision),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.ZeroCurrency),
					sqlCurrency(types.NewCurrency64(frand.Uint64n(5))), // random remaining allowance
					sqlContractState(uint8(frand.Uint64n(5))),          // random contract state (40% active)
					frand.Uint64n(2) == 0,                              // random good state (50% good)
					size,                                               // random size
					size+frand.Uint64n(1e3),                            // random capacity
					randomTime(),                                       // random last_broadcast_attempt
					randomTime(),                                       // random next_prune
				); err != nil {
					return err
				}
			}
			for i := 1; i < len(hostContractIDs)-1; i++ {
				if _, err := tx.Exec(ctx, `UPDATE contracts SET renewed_from = $1, renewed_to = $2 WHERE contract_id = $3;`, hostContractIDs[i-1], hostContractIDs[i+1], hostContractIDs[i]); err != nil {
					return err
				}
			}
			contractIDs = append(contractIDs, hostContractIDs...)
			hosts = append(hosts, hk)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.Run("contract", func(b *testing.B) {
		if b.N > len(contractIDs) {
			b.Fatalf("too many iterations, %d > %d", b.N, len(contractIDs))
		}
		for b.Loop() {
			rIdx := frand.Intn(len(contractIDs))
			_, err := store.Contract(context.Background(), contractIDs[rIdx])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	for _, limit := range []int{100, 1000} {
		b.Run(fmt.Sprintf("contracts_%d", limit), func(b *testing.B) {
			for b.Loop() {
				_, err := store.Contracts(context.Background(), 0, limit)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("contracts_revisable_%d", limit), func(b *testing.B) {
			for b.Loop() {
				_, err := store.Contracts(context.Background(), 0, limit, contracts.WithRevisable(true))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("contracts_revisable+good_%d", limit), func(b *testing.B) {
			for b.Loop() {
				_, err := store.Contracts(context.Background(), 0, limit, contracts.WithRevisable(true), contracts.WithGood(true))
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("contracts_for_broadcasting_%d", limit), func(b *testing.B) {
			for b.Loop() {
				_, err := store.ContractsForBroadcasting(context.Background(), randomTime(), limit)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("contracts_for_funding_%d", limit), func(b *testing.B) {
			for b.Loop() {
				_, err := store.ContractsForFunding(context.Background(), hosts[frand.Intn(len(hosts))], limit)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("contracts_for_pinning", func(b *testing.B) {
		for b.Loop() {
			_, err := store.ContractsForPinning(context.Background(), hosts[frand.Intn(len(hosts))], maxContractSize)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("contracts_for_pruning", func(b *testing.B) {
		for b.Loop() {
			_, err := store.ContractsForPruning(context.Background(), hosts[frand.Intn(len(hosts))])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("contracts_revisions", func(b *testing.B) {
		for b.Loop() {
			contractID := contractIDs[frand.Intn(len(contractIDs))]
			revision, _, err := store.ContractRevision(context.Background(), contractID)
			if err != nil {
				b.Fatal(err)
			}
			revision.RevisionNumber++
			if err := store.UpdateContractRevision(context.Background(), contractID, revision); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPrunableContractRoots(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// benchmark parameters
	const (
		oneTB    = 1 << 40 // 1TiB of sectors
		nHosts   = 100
		nSectors = oneTB / proto.SectorSize
	)

	// add account
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// add hosts and contracts
	var hks []types.PublicKey
	for range nHosts {
		hk := store.addTestHost(b)
		store.addTestContract(b, hk)
		hks = append(hks, hk)
	}

	// insert sectors in batches
	hostIdx := 0
	rootsByContract := make(map[types.FileContractID][]types.Hash256)
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.SectorPinParams
		for range batchSize {
			hk := hks[hostIdx]
			root := frand.Entropy256()
			sectors = append(sectors, slabs.SectorPinParams{
				Root:    root,
				HostKey: hks[hostIdx],
			})
			rootsByContract[types.FileContractID(hk)] = append(rootsByContract[types.FileContractID(hk)], root)
			hostIdx = (hostIdx + 1) % len(hks)
		}

		// pin slab
		if _, err := store.PinSlab(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	// pin all sectors to contracts
	for contractID, roots := range rootsByContract {
		err := store.PinSectors(context.Background(), contractID, roots)
		if err != nil {
			b.Fatal(err)
		}
	}

	// extend roots to 1TB worth of sectors and random shuffle
	for contractID, roots := range rootsByContract {
		for range nSectors - len(roots) {
			rootsByContract[contractID] = append(rootsByContract[contractID], frand.Entropy256())
		}
		frand.Shuffle(len(rootsByContract[contractID]), func(i, j int) {
			rootsByContract[contractID][i], rootsByContract[contractID][j] = rootsByContract[contractID][j], rootsByContract[contractID][i]
		})
	}

	// run benchmarks with various batch sizes
	for _, batchSize := range []int64{1000, 5000, 10000, nSectors} {
		b.Run(fmt.Sprintf("prunable_roots_batch_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(batchSize * proto.SectorSize)
			for b.Loop() {
				fcid := types.FileContractID(hks[frand.Intn(len(hks))])
				prunable, err := store.PrunableContractRoots(context.Background(), fcid, rootsByContract[fcid][:batchSize])
				if err != nil {
					b.Fatal(err)
				} else if len(prunable) == 0 || len(prunable) == int(batchSize) {
					b.Fatal("unexpected number of prunable roots", len(prunable)) // assert it's not empty or all
				}
			}
		})
	}
}

func (s *Store) addTestContract(t testing.TB, hk types.PublicKey, fcids ...types.FileContractID) types.FileContractID {
	t.Helper()

	var fcid types.FileContractID
	switch len(fcids) {
	case 0:
		fcid = types.FileContractID(hk)
	case 1:
		fcid = fcids[0]
	default:
		panic("developer error")
	}

	err := s.AddFormedContract(context.Background(), hk, fcid, newTestRevision(hk), types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)
	if err != nil {
		t.Fatal(err)
	}
	return fcid
}

func newTestRevision(hk types.PublicKey) types.V2FileContract {
	return types.V2FileContract{
		HostPublicKey:    hk,
		Capacity:         200,
		Filesize:         100,
		FileMerkleRoot:   types.Hash256{1},
		ProofHeight:      600,
		ExpirationHeight: 800,
		RevisionNumber:   1,
		TotalCollateral:  types.Siacoins(100),
	}
}
