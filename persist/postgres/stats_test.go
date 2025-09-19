package postgres

import (
	"context"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSectorStatsNumSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account and host
	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account), accounts.AccountMeta{}); err != nil {
		t.Fatal("failed to add account:", err)
	}
	hk := store.addTestHost(t)

	// helper to create slabs
	newSlab := func(i byte) slabs.SlabPinParams {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     10,
			Sectors: []slabs.PinnedSector{
				{
					Root:    frand.Entropy256(),
					HostKey: hk,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: hk,
				},
			},
		}
		return slab
	}

	assertStats := func(numSlabs int64) {
		t.Helper()
		stats, err := store.SectorStats(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if stats.NumSlabs != numSlabs {
			t.Fatalf("expected %d slabs, got %d", numSlabs, stats.NumSlabs)
		}
	}

	// we start with 0 slabs
	assertStats(0)

	// pin some slabs
	var slabIDs []slabs.SlabID
	for i := range byte(10) {
		slabID, err := store.PinSlab(context.Background(), account, time.Now(), newSlab(i))
		if err != nil {
			t.Fatal(err)
		}
		slabIDs = append(slabIDs, slabID)
		assertStats(int64(len(slabIDs)))
	}

	// unpin them again
	for len(slabIDs) > 0 {
		slabID := slabIDs[0]
		if err := store.UnpinSlab(context.Background(), account, slabID); err != nil {
			t.Fatal(err)
		}
		slabIDs = slabIDs[1:]
		assertStats(int64(len(slabIDs)))
	}
}
