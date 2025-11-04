package contracts_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestContractPruning(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithApps(1), testutils.WithHosts(10))

	// convenience variables
	indexer := cluster.Indexer
	store := indexer.Store()

	// fetch account
	app := cluster.Apps[0]
	acc, err := app.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}

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

	// assert the slab is pinned
	time.Sleep(time.Second)
	res, err := store.Slabs(context.Background(), acc.AccountKey, slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(res))
	}

	// assert the sectors were pinned
	contracts := make(map[types.PublicKey]types.FileContractID)
	for _, sector := range res[0].Sectors {
		if sector.HostKey == nil || sector.ContractID == nil {
			t.Fatal("sector is not pinned")
		}
		contracts[*sector.HostKey] = *sector.ContractID
	}

	for _, host := range hosts {
		res, err := indexer.HostClient(t, host.PublicKey).SectorRoots(context.Background(), host.Settings.Prices, contracts[host.PublicKey], 0, 1)
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
		contract, _, err := store.ContractRevision(context.Background(), contracts[host.PublicKey])
		if err != nil {
			t.Fatal(err)
		} else if contract.Revision.Filesize != 0 {
			t.Fatalf("expected contract %s to be pruned, got filesize %d", contracts[host.PublicKey], contract.Revision.Filesize)
		} else if contract.Revision.Capacity != proto.SectorSize {
			t.Fatalf("expected contract %s to be pruned, got capacity %d", contracts[host.PublicKey], contract.Revision.Capacity)
		}
	}
}

func TestSectorPinning(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(10), testutils.WithApps(1))
	indexer := cluster.Indexer
	store := indexer.Store()
	time.Sleep(time.Second)

	// fetch account
	app := cluster.Apps[0]
	acc, err := app.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
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

	// assert the slab is pinned
	time.Sleep(time.Second)
	res, err := store.Slabs(context.Background(), acc.AccountKey, slabIDs)
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
	_, err = store.Slabs(context.Background(), acc.AccountKey, slabIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal("unexpected error", err)
	}
}
