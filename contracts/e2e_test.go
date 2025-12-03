package contracts_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestContractPruning(t *testing.T) {
	const (
		nHosts = 7
		nSlabs = 4
		// the batch size used to fetch sector roots during pruning has to be
		// smaller than the slab we unpin, if it is not, the pruned index is in
		// the first batch and the bug cannot manifest itself
		batchSize = 2
	)

	// create cluster
	logger := zap.NewNop()
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(nHosts), testutils.WithIndexer(testutils.WithContractOptions(contracts.WithSectorRootsBatchSize(batchSize))))
	indexer := cluster.Indexer

	// create an app
	app, sk := cluster.App(t)

	// wait for contracts
	cluster.WaitForContracts(t)

	// assert we have nHosts usable hosts
	available, err := indexer.Hosts().Hosts(t.Context(), 0, nHosts, hosts.WithUsable(true), hosts.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(available) != nHosts {
		t.Fatalf("expected %d usable hosts, got %d", nHosts, len(available))
	}

	// helper to create a new sector root
	expectedRoots := make(map[types.PublicKey][]types.Hash256)
	newRoot := func(hk types.PublicKey) (_ types.Hash256, sector [proto.SectorSize]byte) {
		frand.Read(sector[:])
		root := proto.SectorRoot(&sector)
		expectedRoots[hk] = append(expectedRoots[hk], root)
		return root, sector
	}

	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()

	// prepare slabs
	var pinParams []slabs.SlabPinParams
	for range nSlabs {
		params := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
		}
		for i := range nHosts {
			hk := available[i].PublicKey
			root, sector := newRoot(hk)
			params.Sectors = append(params.Sectors, slabs.PinnedSector{
				Root:    root,
				HostKey: hk,
			})

			if _, err = client.WriteSector(t.Context(), sk, hk, sector[:]); err != nil {
				t.Fatal(err)
			}
		}
		pinParams = append(pinParams, params)
	}

	// pin the slabs
	slabIDs, err := app.PinSlabs(t.Context(), sk, pinParams...)
	if err != nil {
		t.Fatal(err)
	}

	// wait until all sectors are pinned
	for _, id := range slabIDs {
	waiting:
		for range 10 {
			slab, err := indexer.Store().Slab(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, sector := range slab.Sectors {
				if sector.ContractID == nil {
					time.Sleep(100 * time.Millisecond)
					continue waiting
				}
			}
			break
		}
	}

	// helper to fetch contract roots for a given host
	contractRoots := func(hk types.PublicKey) []types.Hash256 {
		t.Helper()

		// fetch active contract
		active, err := indexer.Contracts().Contracts(t.Context(), 0, 2, contracts.WithRevisable(true), contracts.WithGood(true), contracts.WithHostKeys([]types.PublicKey{hk}))
		if err != nil {
			t.Fatal(err)
		} else if len(active) == 0 {
			t.Fatalf("no active contract found for host %s", hk)
		} else if len(active) > 1 {
			// this should not happen unless the contract is full (impossible) or the maintenance
			// is not working as expected.
			t.Fatalf("multiple active contracts found for host %s", hk)
		}
		contractID := active[0].ID

		// fetch contract revision to get filesize
		contract, _, err := indexer.Store().ContractRevision(contractID)
		if err != nil {
			t.Fatal(err)
		}
		nSectors := contract.Revision.Filesize / proto.SectorSize

		// fetch host client
		hc := indexer.HostClient(t, hk)
		hs, err := hc.Settings(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// fetch sector roots (ignore error, so we can retry without failing)
		res, _ := hc.SectorRoots(t.Context(), hs.Prices, contractID, 0, nSectors)
		return res.Roots
	}

	// compare contract roots
	assertRoots := func() {
		t.Helper()

	outer:
		for _, host := range available {
			var got []types.Hash256
			for range 4 {
				if got = contractRoots(host.PublicKey); reflect.DeepEqual(got, expectedRoots[host.PublicKey]) {
					continue outer
				}
				time.Sleep(250 * time.Millisecond)
			}
			t.Log("contract roots:")
			for _, r := range got {
				t.Log(r[:6], "...", r[len(r)-6:])
			}
			t.Log("expected roots:")
			for _, r := range expectedRoots[host.PublicKey] {
				t.Log(r[:6], "...", r[len(r)-6:])
			}
			t.Fatalf("unexpected roots for host %s", host.PublicKey)
		}
	}

	// assert initial state
	time.Sleep(time.Second)
	assertRoots()

	// unpin the 3rd slab and trigger pruning
	if err := app.UnpinSlab(t.Context(), sk, slabIDs[2]); err != nil {
		t.Fatal(err)
	} else if err = indexer.Contracts().TriggerContractPruning(); err != nil {
		t.Fatal(err)
	}

	// remove the 3rd slab's roots from the expected roots (swap and pop)
	for hk := range expectedRoots {
		expectedRoots[hk][2] = expectedRoots[hk][len(expectedRoots[hk])-1]
		expectedRoots[hk] = expectedRoots[hk][:len(expectedRoots[hk])-1]
	}

	// assert the contracts are pruned
	time.Sleep(time.Second)
	assertRoots()
}

func TestSectorPinning(t *testing.T) {
	// create cluster
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(10))
	indexer := cluster.Indexer

	// create an app
	app, sk := cluster.App(t)

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	// assert we have 10 usable hosts
	available, err := indexer.Hosts().Hosts(context.Background(), 0, 10, hosts.WithUsable(true), hosts.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(available) != 10 {
		t.Fatalf("expected 10 usable hosts, got %d", len(available))
	}

	// prepare pin params
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
	}

	client := client.New(client.NewProvider(hosts.NewHostStore(cluster.Indexer.Store())))
	defer client.Close()

	// upload a random sector to each host
	for _, host := range available {
		var sector [proto.SectorSize]byte
		frand.Read(sector[:])
		if _, err = client.WriteSector(context.Background(), sk, host.PublicKey, sector[:]); err != nil {
			t.Fatal(err)
		}
		params.Sectors = append(params.Sectors, slabs.PinnedSector{Root: proto.SectorRoot(&sector), HostKey: host.PublicKey})
	}

	// pin the slab
	slabIDs, err := app.PinSlabs(context.Background(), sk, params)
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	// fetch account
	acc, err := app.Account(t.Context(), sk)
	if err != nil {
		t.Fatal(err)
	}

	// assert the slab is pinned
	time.Sleep(time.Second)
	res, err := indexer.Store().Slabs(acc.AccountKey, slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(res))
	}

	// assert the sectors were pinned
	for _, sector := range res[0].Sectors {
		if sector.HostKey == nil || sector.ContractID == nil {
			t.Fatal("sector is not pinned")
		}
	}

	// unpin the slab
	if err := app.UnpinSlab(context.Background(), sk, slabID); err != nil {
		t.Fatal(err)
	}

	// assert the slab is unpinned
	_, err = indexer.Store().Slabs(acc.AccountKey, slabIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal("unexpected error", err)
	}
}
