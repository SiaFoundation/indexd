package contracts_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
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
	app := cluster.App(t)

	// wait for contracts
	cluster.WaitForContracts(t)

	// assert we have nHosts usable hosts
	hosts, err := indexer.Hosts().Hosts(t.Context(), 0, nHosts, hosts.WithUsable(true), hosts.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != nHosts {
		t.Fatalf("expected %d usable hosts, got %d", nHosts, len(hosts))
	}

	// helper to create a new sector root
	newRoot := func() (_ types.Hash256, sector [proto.SectorSize]byte) {
		frand.Read(sector[:])
		root := proto.SectorRoot(&sector)
		return root, sector
	}

	// prepare slabs
	var pinParams []slabs.SlabPinParams
	for range nSlabs {
		params := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
		}
		for i := range nHosts {
			hk := hosts[i].PublicKey
			root, sector := newRoot()
			params.Sectors = append(params.Sectors, slabs.PinnedSector{
				Root:    root,
				HostKey: hk,
			})

			client := indexer.HostClient(t, hk)
			_, err = client.WriteSector(t.Context(), hosts[i].Settings.Prices, app.AccountToken(hk), bytes.NewReader(sector[:]), proto.SectorSize)
			if err != nil {
				t.Fatal(err)
			}
		}
		pinParams = append(pinParams, params)
	}

	// pin the slabs
	slabIDs, err := app.PinSlabs(t.Context(), pinParams...)
	if err != nil {
		t.Fatal(err)
	}

	// fetch account
	acc, err := app.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// assert all slabs were pinned
	time.Sleep(time.Second)
	res, err := indexer.Store().Slabs(t.Context(), acc.AccountKey, slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != nSlabs {
		t.Fatalf("expected %d slabs, got %d", nSlabs, len(res))
	}

	// assert all sectors were pinned
	contractRoots := make(map[types.FileContractID][]types.Hash256)
	for _, slab := range res {
		if len(slab.Sectors) != nHosts {
			t.Fatalf("expected %d sectors, got %d", nHosts, len(slab.Sectors))
		}
		for _, sector := range slab.Sectors {
			if sector.HostKey == nil || sector.ContractID == nil {
				t.Fatal("sector is not pinned")
			}
			contractRoots[*sector.ContractID] = append(contractRoots[*sector.ContractID], sector.Root)
		}
	}

	// helper to fetch host's active contract ID
	activeContractID := func(hk types.PublicKey) types.FileContractID {
		t.Helper()

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
		return active[0].ID
	}

	// compare contract roots
	assertRoots := func(expectedSize uint64) {
		t.Helper()

		for _, host := range hosts {
			contractID := activeContractID(host.PublicKey)

			contract, _, err := indexer.Store().ContractRevision(t.Context(), contractID)
			if err != nil {
				t.Fatal(err)
			} else if contract.Revision.Filesize != expectedSize {
				t.Fatal("unexpected filesize, expected", expectedSize, "got", contract.Revision.Filesize)
			}

			client := indexer.HostClient(t, host.PublicKey)
			hs, err := client.Settings(t.Context())
			if err != nil {
				t.Fatal(err)
			}

			res, err := client.SectorRoots(t.Context(), hs.Prices, contractID, 0, contract.Revision.Filesize/proto.SectorSize)
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(res.Roots, contractRoots[contractID]) {
				t.Fatalf("unexpected roots for host %s", host.PublicKey)
			}
		}
	}

	time.Sleep(time.Second)
	assertRoots(proto.SectorSize * nSlabs)

	// unpin the 3rd slab and trigger pruning
	if err := app.UnpinSlab(t.Context(), slabIDs[2]); err != nil {
		t.Fatal(err)
	} else if err = indexer.Contracts().TriggerContractPruning(); err != nil {
		t.Fatal(err)
	}

	// remove the 3rd slab's roots from the expected roots (swap and pop)
	for fcid := range contractRoots {
		contractRoots[fcid][2] = contractRoots[fcid][len(contractRoots[fcid])-1]
		contractRoots[fcid] = contractRoots[fcid][:len(contractRoots[fcid])-1]
	}

	// assert the contracts are pruned
	time.Sleep(time.Second)
	assertRoots(proto.SectorSize * (nSlabs - 1))
}

func TestSectorPinning(t *testing.T) {
	// create cluster
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(10))
	indexer := cluster.Indexer

	// create an app
	app := cluster.App(t)

	// wait for contracts to be formed
	cluster.WaitForContracts(t)

	// assert we have 10 usable hosts
	hosts, err := indexer.Hosts().Hosts(context.Background(), 0, 10, hosts.WithUsable(true), hosts.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 10 {
		t.Fatalf("expected 10 usable hosts, got %d", len(hosts))
	}

	// prepare pin params
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
	}

	// upload a random sector to each host
	for _, host := range hosts {
		var sector [proto.SectorSize]byte
		frand.Read(sector[:])
		_, err = indexer.HostClient(t, host.PublicKey).WriteSector(context.Background(), host.Settings.Prices, app.AccountToken(host.PublicKey), bytes.NewReader(sector[:]), proto.SectorSize)
		if err != nil {
			t.Fatal(err)
		}
		params.Sectors = append(params.Sectors, slabs.PinnedSector{Root: proto.SectorRoot(&sector), HostKey: host.PublicKey})
	}

	// pin the slab
	slabIDs, err := app.PinSlabs(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	// fetch account
	acc, err := app.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// assert the slab is pinned
	time.Sleep(time.Second)
	res, err := indexer.Store().Slabs(context.Background(), acc.AccountKey, slabIDs)
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
	if err := app.UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal(err)
	}

	// assert the slab is unpinned
	_, err = indexer.Store().Slabs(context.Background(), acc.AccountKey, slabIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal("unexpected error", err)
	}
}
