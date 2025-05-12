package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/subscriber"
	"go.uber.org/zap"
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

func TestContractElement(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert contract element is not found
	_, err = store.ContractElement(context.Background(), types.FileContractID(hk))
	if !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal(err)
	}

	// add a contract and an element
	if err := store.AddFormedContract(context.Background(), types.FileContractID(hk), hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		t.Fatal(err)
	} else if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.UpdateContractElements(types.V2FileContractElement{
			ID: types.FileContractID(hk),
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
	_, err = store.ContractElement(context.Background(), types.FileContractID(hk))
	if err != nil {
		t.Fatal(err)
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

func TestContractsForBroadcasting(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add two contracts
	fcid1 := types.FileContractID{1}
	if err := store.AddFormedContract(context.Background(), fcid1, hk, 100, 200, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}
	fcid2 := types.FileContractID{2}
	if err := store.AddFormedContract(context.Background(), fcid2, hk, 100, 200, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}

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
	if err := store.AddRenewedContract(context.Background(), contracts.AddRenewedContractParams{
		RenewedFrom: res[0],
		RenewedTo:   types.FileContractID{9, 9, 9},
	}); err != nil {
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

	var fcidCnt uint8
	addContract := func(hk types.PublicKey, remaininAllowance types.Currency) types.FileContractID {
		t.Helper()
		fcidCnt++
		fcid := types.FileContractID{byte(fcidCnt)}
		if err := store.AddFormedContract(context.Background(), fcid, hk, 100, 200, types.ZeroCurrency, remaininAllowance, types.ZeroCurrency, types.ZeroCurrency); err != nil {
			t.Fatal(err)
		}
		return fcid
	}

	// add two hosts
	hk1 := types.PublicKey{1}
	hk2 := types.PublicKey{2}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return errors.Join(
			tx.AddHostAnnouncement(hk1, chain.V2HostAnnouncement{}, time.Now()),
			tx.AddHostAnnouncement(hk2, chain.V2HostAnnouncement{}, time.Now()),
		)
	}); err != nil {
		t.Fatal(err)
	}

	// add four contracts for h1
	fcid1 := addContract(hk1, types.Siacoins(1))
	fcid2 := addContract(hk1, types.Siacoins(3))
	fcid3 := addContract(hk1, types.Siacoins(2))
	_ = addContract(hk1, types.ZeroCurrency)

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

	// add a contract for h2
	fcid5 := addContract(hk2, types.Siacoins(1))

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
		if err := store.AddFormedContract(context.Background(), fcid, hk, 100, 200, types.ZeroCurrency, allowance, types.ZeroCurrency, types.ZeroCurrency); err != nil {
			t.Fatal(err)
		}
		query := `UPDATE contracts SET size = $1, capacity = $2, state = $3, good = $4 WHERE contract_id = $5`
		_, err := store.pool.Exec(context.Background(), query, size, capacity, sqlContractState(state), good, sqlHash256(fcid))
		if err != nil {
			t.Fatal(err)
		}
	}

	// add two hosts
	hk1 := types.PublicKey{1}
	hk2 := types.PublicKey{2}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return errors.Join(
			tx.AddHostAnnouncement(hk1, chain.V2HostAnnouncement{}, time.Now()),
			tx.AddHostAnnouncement(hk2, chain.V2HostAnnouncement{}, time.Now()),
		)
	}); err != nil {
		t.Fatal(err)
	}

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

	addContract := func(hk types.PublicKey, fcid types.FileContractID, allowance types.Currency, size uint64, state contracts.ContractState, good bool, lastPrune time.Time) {
		t.Helper()
		if err := store.AddFormedContract(context.Background(), fcid, hk, 100, 200, types.ZeroCurrency, allowance, types.ZeroCurrency, types.ZeroCurrency); err != nil {
			t.Fatal(err)
		}
		query := `UPDATE contracts SET state = $1, good = $2, size = $3, capacity = $4, last_prune = $5 WHERE contract_id = $6`
		_, err := store.pool.Exec(context.Background(), query, sqlContractState(state), good, size, size, lastPrune, sqlHash256(fcid))
		if err != nil {
			t.Fatal(err)
		}
	}

	// add two hosts
	hk1 := types.PublicKey{1}
	hk2 := types.PublicKey{2}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return errors.Join(
			tx.AddHostAnnouncement(hk1, chain.V2HostAnnouncement{}, time.Now()),
			tx.AddHostAnnouncement(hk2, chain.V2HostAnnouncement{}, time.Now()),
		)
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	oneDayAgo := now.Add(-24 * time.Hour)
	twoDaysAgo := now.Add(-48 * time.Hour)

	// add contracts for h1
	addContract(hk1, types.FileContractID{1}, types.ZeroCurrency, 100, contracts.ContractStateActive, true, twoDaysAgo)       // no allowance
	addContract(hk1, types.FileContractID{2}, types.NewCurrency64(1), 100, contracts.ContractStateResolved, true, twoDaysAgo) // resolved
	addContract(hk1, types.FileContractID{3}, types.NewCurrency64(1), 100, contracts.ContractStateActive, false, twoDaysAgo)  // bad
	addContract(hk1, types.FileContractID{4}, types.NewCurrency64(1), 100, contracts.ContractStateActive, true, now)          // pruned recently
	addContract(hk1, types.FileContractID{5}, types.NewCurrency64(1), 100, contracts.ContractStateActive, true, twoDaysAgo)   // ok - small size
	addContract(hk1, types.FileContractID{6}, types.NewCurrency64(1), 200, contracts.ContractStateActive, true, twoDaysAgo)   // ok - big size

	// add contracts for h2
	addContract(hk2, types.FileContractID{7}, types.NewCurrency64(1), 100, contracts.ContractStateActive, true, twoDaysAgo) // ok

	// assert contracts for pruning for h1
	contractIDs, err := store.ContractsForPruning(context.Background(), hk1, oneDayAgo)
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
	contractIDs, err = store.ContractsForPruning(context.Background(), hk2, oneDayAgo)
	if err != nil {
		t.Fatal(err)
	} else if len(contractIDs) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contractIDs))
	} else if contractIDs[0] != (types.FileContractID{7}) {
		t.Fatalf("expected contract %v, got %v", types.FileContractID{7}, contractIDs[0])
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
		contract.LastBroadcastAttempt = time.Time{}
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

	// assert `contract_sectors_map` entry was created when forming a contract
	var mapID int64
	err = store.pool.QueryRow(context.Background(), `SELECT id FROM contract_sectors_map WHERE contract_id = $1`, sqlHash256(expectedFormed.ID)).Scan(&mapID)
	if err != nil {
		t.Fatal(err)
	}

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

func TestMarkPruned(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add a contract
	fcid := types.FileContractID{1}
	if err := store.AddFormedContract(context.Background(), fcid, hk, 0, 0, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	}

	// assert contract is not marked as pruned
	if contract, err := store.Contract(context.Background(), fcid); err != nil {
		t.Fatal(err)
	} else if !contract.LastPrune.IsZero() {
		t.Fatal("contract should not be pruned")
	}

	// mark as pruned and assert contract was updated correctly
	if err := store.MarkPruned(context.Background(), fcid); err != nil {
		t.Fatal(err)
	} else if contract, err := store.Contract(context.Background(), fcid); err != nil {
		t.Fatal(err)
	} else if contract.LastPrune.IsZero() {
		t.Fatal("contract should be marked as pruned")
	}

	// assert field is decorated in store.Contracts as well
	if contracts, err := store.Contracts(context.Background(), 0, 10); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contracts))
	} else if contracts[0].LastPrune.IsZero() {
		t.Fatal("contract should be marked as pruned")
	}
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

func TestMarkBroadcastAttempt(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add a host
	hk := types.PublicKey{1, 1, 1}
	if err := store.UpdateChainState(context.Background(), func(tx subscriber.UpdateTx) error {
		return tx.AddHostAnnouncement(hk, chain.V2HostAnnouncement{}, time.Now())
	}); err != nil {
		t.Fatal(err)
	}

	// add a contract
	fcid := types.FileContractID{1}
	if err := store.AddFormedContract(context.Background(), fcid, hk, 100, 200, types.Siacoins(1), types.Siacoins(1), types.Siacoins(1), types.Siacoins(1)); err != nil {
		t.Fatal(err)
	}

	// assert broadcast attempt is defaulted
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

// BenchmarkContracts is a benchmark to ensure the performance of
// all methods on the store that return either a contract or a list of
// contract IDs.
//
// M1 Max | Contract                      |                | 1.42 ms/op
// M1 Max | Contracts 100                 | None           | 2.24 ms/op
// M1 Max | Contracts 100                 | Revisable      | 1.85 ms/op
// M1 Max | Contracts 100                 | Revisable+Good | 1.75 ms/op
// M1 Max | Contracts 1000                | None           | 5.37 ms/op
// M1 Max | Contracts 1000                | Revisable      | 5.66 ms/op
// M1 Max | Contracts 1000                | Revisable+Good | 5.47 ms/op
// M1 Max | ContractsForBroadcasting 100  | -              | 1.76 ms/op
// M1 Max | ContractsForBroadcasting 1000 | -              | 2.15 ms/op
// M1 Max | ContractsForFunding 100       | -              | 1.27 ms/op
// M1 Max | ContractsForFunding 1000      | -              | 1.46 ms/op
// M1 Max | ContractsForPinning           | -              | 1.25 ms/op
// M1 Max | ContractsForPruning           | -              | 1.14 ms/op
func BenchmarkContracts(b *testing.B) {
	const (
		maxContractSize     = 10 * 1 << 40 // 10TB
		numContractsPerHost = 100
		numHosts            = 1000
	)

	randomTime := func() time.Time {
		return time.Now().Add(-time.Duration(frand.Uint64n(60*60)) * time.Second)
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
				frand.Read(hostContractIDs[i][:])
				size := frand.Uint64n(1e9)
				if _, err := tx.Exec(ctx, `INSERT INTO contracts (host_id, contract_id, proof_height, expiration_height, contract_price, initial_allowance, miner_fee, total_collateral, remaining_allowance, state, good, size, capacity, last_broadcast_attempt, last_prune) VALUES ($1, $2, 0, 0, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);`,
					hostID,
					sqlHash256(hostContractIDs[i][:]),
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
					randomTime(),                                       // random last_prune
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
			_, err := store.ContractsForPruning(context.Background(), hosts[frand.Intn(len(hosts))], randomTime())
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
