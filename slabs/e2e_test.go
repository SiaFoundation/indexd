package slabs_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestDataIntegrityE2E(t *testing.T) {
	// create cluster
	logger := testutils.NewLogger(false)
	cluster := testutils.NewCluster(t, testutils.WithLogger(logger), testutils.WithHosts(3), testutils.WithIndexer(testutils.WithSlabOptions(slabs.WithHealthCheckInterval(100*time.Millisecond), slabs.WithIntegrityCheckIntervals(50*time.Millisecond, 50*time.Millisecond))))
	indexer := cluster.Indexer
	time.Sleep(time.Second)

	// add an account
	a1 := types.GeneratePrivateKey()
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert we have 3 usable hosts
	time.Sleep(time.Second)
	hosts, err := indexer.Hosts(context.Background(), admin.WithUsable(true), admin.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 3 {
		t.Fatalf("expected 3 usable hosts, got %d", len(hosts))
	}

	// convenience variables
	app := indexer.App(a1)

	// prepare pin params
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
	}

	// upload a random sector to each host
	for _, host := range hosts {
		var sector [proto.SectorSize]byte
		frand.Read(sector[:])
		_, err = indexer.HostClient(t, host.PublicKey).WriteSector(context.Background(), host.Settings.Prices, proto.NewAccountToken(a1, host.PublicKey), bytes.NewReader(sector[:]), proto.SectorSize)
		if err != nil {
			t.Fatal(err)
		}
		params.Sectors = append(params.Sectors, slabs.SectorPinParams{Root: proto.SectorRoot(&sector), HostKey: host.PublicKey})
	}

	// pin the slab
	slabID, err := app.PinSlab(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}

	// fetch pinned slab metadata
	time.Sleep(time.Second)
	pinnedSlab, err := app.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	} else if len(pinnedSlab.Sectors) != len(params.Sectors) {
		t.Fatalf("expected %d sectors, got %d", len(params.Sectors), len(pinnedSlab.Sectors))
	}

	// drop a sector from the first host
	for _, sector := range pinnedSlab.Sectors {
		if sector.HostKey == cluster.Hosts[0].PublicKey() {
			cluster.Hosts[0].DropSector(sector.Root)
			break
		}
	}

	// assert the integrity check detects the lost sector
	time.Sleep(time.Second)
	h, err := cluster.Indexer.Host(context.Background(), cluster.Hosts[0].PublicKey())
	if err != nil {
		t.Fatal(err)
	} else if h.LostSectors != 1 {
		t.Fatalf("expected 1 lost sector, got %d", h.LostSectors)
	}
}
