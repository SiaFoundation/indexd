package contracts_test

import (
	"bytes"
	"context"
	"errors"
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
	// create cluster
	logger := zaptest.NewLogger(t)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(10))

	// convenience variables
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

	getActiveContract := func(hostKey types.PublicKey) types.FileContractID {
		t.Helper()

		active, err := indexer.Contracts().Contracts(context.Background(), 0, 100, contracts.WithRevisable(true), contracts.WithGood(true))
		if err != nil {
			t.Fatal(err)
		}
		var contractID types.FileContractID
		for _, c := range active {
			if c.HostKey == hostKey {
				if contractID != (types.FileContractID{}) {
					// this should not happen unless the contract is full (impossible) or the maintenance
					// is not working as expected.
					t.Fatalf("found multiple usable contracts for host %s", hostKey)
				}
				contractID = c.ID
			}
		}
		if contractID == (types.FileContractID{}) {
			t.Fatalf("no usable contract found for host %s", hostKey)
		}
		return contractID
	}

	for _, host := range hosts {
		res, err := indexer.HostClient(t, host.PublicKey).SectorRoots(context.Background(), host.Settings.Prices, getActiveContract(host.PublicKey), 0, 1)
		if err != nil {
			t.Fatal(err)
		} else if len(res.Roots) != 1 {
			t.Fatalf("expected one sector roots for host %s, got %d", host.PublicKey, len(res.Roots))
		}
	}

	// unpin the slab
	if err := app.UnpinSlab(context.Background(), slabIDs[0]); err != nil {
		t.Fatal(err)
	}

	// trigger contract pruning
	if err = indexer.Contracts().TriggerContractPruning(); err != nil {
		t.Fatal(err)
	}

	// assert the contracts are pruned
	time.Sleep(time.Second)
	for _, host := range hosts {
		contractID := getActiveContract(host.PublicKey)
		contract, _, err := indexer.Store().ContractRevision(context.Background(), contractID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Revision.Filesize != 0 {
			t.Fatalf("expected contract %s to be pruned, got filesize %d", contractID, contract.Revision.Filesize)
		} else if contract.Revision.Capacity != proto.SectorSize {
			t.Fatalf("expected contract %s to be pruned, got capacity %d", contractID, contract.Revision.Capacity)
		}
	}
}

func TestSectorPinning(t *testing.T) {
	// create cluster
	logger := zap.NewNop()
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
